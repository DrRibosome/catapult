package k8sutil

import (
	"catapultd/pkg/catapult"
	"catapultd/pkg/gen/api"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	JobIDLabel       = "catapult/job-id"
	SchedulerIDLabel = "catapult/scheduler-id"
)

// DeleteClientPods efficiently deletes all pods for client
func DeleteClientPods(client catapult.ClientID, namespace string, clientset *kubernetes.Clientset) error {
	pods := clientset.CoreV1().Pods(namespace)
	opts := metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%v=%v", JobIDLabel, client.String()),
	}
	return pods.DeleteCollection(nil, opts)
}

func ParseResources(r *api.Resources) (resources corev1.ResourceList, err error) {
	var cpus, mem resource.Quantity

	cpus, err = resource.ParseQuantity(r.Cpus)
	if err != nil {
		return
	}

	mem, err = resource.ParseQuantity(r.Mem)
	if err != nil {
		return
	}

	resources = corev1.ResourceList{
		corev1.ResourceCPU:    cpus,
		corev1.ResourceMemory: mem,
	}
	return
}

func LaunchExecutor(
	clientID string,
	schedulerID string,
	jobSpec *api.JobSpec,
	namespace string,
	clientset *kubernetes.Clientset) (*corev1.Pod, error) {
	pods := clientset.CoreV1().Pods(namespace)

	var (
		requests corev1.ResourceList
		err      error
	)

	if jobSpec.Resources != nil {
		requests, err = ParseResources(jobSpec.Resources)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse resources: %+v", jobSpec.Resources)
		}
	} else {
		// client resources not specified
		requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("900m"),
			corev1.ResourceMemory: resource.MustParse("700Mi"),
		}
	}

	// limit based off request, but with min 1 cpu
	limits := requests.DeepCopy()
	minLimitCPU := resource.MustParse("2000m")
	if limits.Cpu().Cmp(minLimitCPU) < 0 {
		limits[corev1.ResourceCPU] = minLimitCPU
	}

	c := corev1.Container{
		Name:            "main",
		Image:           jobSpec.Image,
		ImagePullPolicy: corev1.PullAlways,
		Command:         []string{"catapult-executor"},
		Args: []string{
			"-server", "catapult:12100",
			"-work", "/data/work",
			"-job", clientID,
		},
		Resources: corev1.ResourceRequirements{
			Requests: requests,
			Limits:   limits,
		},

		VolumeMounts: []corev1.VolumeMount{
			// mount secrets
			// https://cloud.google.com/kubernetes-engine/docs/tutorials/authenticating-to-cloud-platform
			{
				Name:      "catapult-key",
				ReadOnly:  true,
				MountPath: "/var/secrets/google",
			},
			{
				Name:      "ssd0",
				MountPath: "/data",
			},
		},
		Env: []corev1.EnvVar{
			// NOTE: the key json here depends on the file name used to upload
			// the secret initially. For instance, if you used:
			// `--from-file=catapult-worker-key.json` then you'd neet to point
			// the google cloud credentials to `$mount/catapult-worker-key.json`
			{Name: "GOOGLE_APPLICATION_CREDENTIALS", Value: "/var/secrets/google/catapult-worker-key.json"},

			{Name: "CATAPULT_JOB_ID", Value: clientID},

			// Extract node name from spec and add as env variable. This allows
			// the running container to discover the fetcherd endpoint it should
			// be using.
			{
				Name: "K8S_NODE_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "spec.nodeName",
					},
				},
			},
			{
				Name: "K8S_POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				},
			},
		},
		SecurityContext: &corev1.SecurityContext{
			Capabilities: &corev1.Capabilities{
				Add: []corev1.Capability{
					"SYS_PTRACE",
				},
			},
		},
	}

	secret := corev1.SecretVolumeSource{
		SecretName: "catapult-key",
	}

	return pods.Create(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "catapult-runner-",
			Namespace:    namespace,
			Labels: map[string]string{
				"app":            "catapult-runner",
				JobIDLabel:       clientID,
				SchedulerIDLabel: schedulerID,
			},
		},
		Spec: corev1.PodSpec{
			Tolerations: []corev1.Toleration{
				{
					Key:      "dedicated",
					Value:    "catapult",
					Effect:   corev1.TaintEffectNoSchedule,
					Operator: corev1.TolerationOpEqual,
				},
			},
			NodeSelector: map[string]string{
				"dedicated": "catapult",
			},
			Containers: []corev1.Container{c},

			RestartPolicy: corev1.RestartPolicyAlways,

			Volumes: []corev1.Volume{
				// secrets, as created by:
				// kubectl create secret generic catapult-key --from-file=catapult-worker-key.json
				{Name: "catapult-key", VolumeSource: corev1.VolumeSource{Secret: &secret}},

				{
					Name: "ssd0",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{Path: "/mnt/disks/ssd0"},
					},
				},
			},
		},
	})
}

// BatchLaunchExecutors sequentially luanches specified number of executors
//
// Returns first error, if any
func BatchLaunchExecutors(
	num int,
	clientID string,
	schedulerID string,
	jobSpec *api.JobSpec,
	namespace string,
	clientset *kubernetes.Clientset) (numLaunched int, err error) {

	for numLaunched = 0; numLaunched < num; numLaunched++ {
		_, err = LaunchExecutor(clientID, schedulerID, jobSpec, namespace, clientset)
		if err != nil {
			return
		}
	}

	return
}

// extractClientID extracts extractClientID from pod
func ExtractClientID(pod *corev1.Pod) (catapult.ClientID, error) {
	if pod.Labels == nil {
		return catapult.ClientID{}, errors.New("no labels")
	}

	id := pod.Labels[JobIDLabel]
	return uuid.Parse(id)
}

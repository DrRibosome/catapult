package podwatcher

import corev1 "k8s.io/api/core/v1"

type Event interface {
	isPodEvent()
}

type PodAdded struct {
	Pod *corev1.Pod
}

func (*PodAdded) isPodEvent() {}

type PodDeleted struct {
	Pod *corev1.Pod
}

func (*PodDeleted) isPodEvent() {}

type ShouldDelete struct {
	Pod    *corev1.Pod
	Result chan bool
}

func (*ShouldDelete) isPodEvent() {}

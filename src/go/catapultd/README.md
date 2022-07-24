# catapultd

**contents**
- [pushing images](#pushing images)

## Kubernetes

### 1. kubectl setup
See [GKE quickstart](https://cloud.google.com/kubernetes-engine/docs/quickstart)

```sh
# project=myproject
# zone=us-east4-b
# cluster=cluster-1

# gcloud config
gcloud config set project $project
gcloud config set compute/zone $zone

# kube config
gcloud container clusters get-credentials $cluster
```

### 2. grant admin permissions
Grant admin permissions to default service account to allow launching jobs.
```sh
kubectl create rolebinding default-admin --clusterrole=admin --serviceaccount=default:default
```

## building

### 1. generate protobufs
```sh
./gen-protos.sh
```

### 2. build
```sh
go build ./cmd/catapult-executor
```

## Required GKE Node Configuration

### Labels
Tasks scheduled with node selector `dedicated=catapult`

### Taints
Workers tolerate node taint `dedicated=catapult`

## Pushing Images
Instructions for pushing _just_ the `catapultd` image, which contains only the
scheduler.

```sh
# optional: ./push-image.sh $tag
./push-image.sh
```

## Get static _internal_ ip
```sh
gcloud compute addresses create catapult --region us-east1 --subnet default
```

And you can check the result via:

```sh
gcloud compute addresses list
```

Then, use that ip for the internal service load balancer.

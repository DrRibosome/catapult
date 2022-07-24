# catapult
Python frontend for interacting with `catapultd` task launcher.

## Running the Example
The example creates a few jobs, runs them until completion, then exits
```sh
python -m catapult.example
```

## Port Forwarding

### SSH Tunnel
Port forward to `research`, then to static internal ip for load balancer
for `catapult` server.
```sh
gcloud compute ssh research --project myproject --zone us-east1-d -- -NL 127.0.0.1:8081:10.142.0.51:9998
```

### K8s Port Forwarding
Port forward directly to scheduler running on the cluster, websocket connection
```sh
gcloud container clusters get-credentials cluster-2 --zone us-east4-b --project myproject  && kubectl port-forward $(kubectl get pod --selector="app=catapultd" --output jsonpath='{.items[0].metadata.name}') 8080:9999
```

## Asyncio Debug
Enter debug mode by setting
```sh
PYTHONASYNCIODEBUG=1
```

See also: https://docs.python.org/3/library/asyncio-dev.html

## Running Tests
```sh
pytest -p no:warnings --pyargs catapult
```

### Testing Notes
- Image and scheduler are hardcoded into the test file
- Add `-s` to show output

## Generating Protos
Run

```sh
./gen-protos.sh
```

As a note, this requires a special structure to the `catapultd` proto directory.
See this github issue [comment](#https://github.com/grpc/grpc/issues/9575#issuecomment-293934506)
for how this works.

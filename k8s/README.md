# NO!
Experimental, for testing, don't use in production or if you do fix all the things.

Based on https://kubernetes.io/docs/tutorials/stateful-application/cassandra/

this are purely notes

```
minikube start --cpus 16 --memory 8192 --disk-size 50000mb
eval $(minikube -p minikube docker-env)
docker build . -t tremor-cluster -f Dockerfile.cluster

kubectl apply -f  k8s/service.yml

kubectl apply -f  k8s/statefulset.yml

kubectl get statefulset tremor
kubectl get pods -l="app=tremor"

kubectl logs pod/tremor-0
kubectl describe pods -l="app=tremor"

kubectl exec -it tremor-0 -- /tremor cluster status --api tremor-0:8000

kubectl apply -f  k8s/deployment.yml
kubectl get deployment tremor
kubectl autoscale deployment tremor --cpu-percent=50 --min=2 --max=10

```


# UCSC CSE138 Winter 2023 Assignment Grader ([Course Page](https://github.com/palvaro/CSE138-Winter23))

I ran this against a [MicroK8s](https://microk8s.io) cluster, with the
[local registry enabled](https://microk8s.io/docs/registry-built-in). Roughly, the steps to do it are:

1. Install MicroK8s and launch a cluster [[link](https://microk8s.io/#install-microk8s)]
   - Then wait for the cluster to run
```bash
microk8s status --wait-ready
```

2. Enable the registry addon [[link](https://microk8s.io/docs/registry-built-in)]
```bash
microk8s enable registry
```

3. Find the IP address of the registry
   - If you're running in Linux, it'll be `localhost` or `127.0.0.1`
   - If you're running in Windows/Mac, the K8s cluster and the registry are going to be running inside a VM. In Mac
     (and probably windows too) this is done using [Multipass](https://multipass.run), and you can do the following
     to find the IP:
```bash
multipass list
```
     In my case there were two IP addresses listed here, and one of them was the one I needed. You can find which one
     by `ping`ing them both; whichever answers is what you're looking for

4. (Only for Mac/Windows) Find the IP range of the pods in the Kubernetes cluster
   - [By default](https://microk8s.io/docs/change-cidr) this will be `10.1.0.0/16`, and you really shouldn't have to
     dig deeper; but, if you do, at least in the version I used (1.26.1) this could be done like so:
```bash
microk8s kubectl cluster-info dump | grep -A1 CALICO_IPV4POOL_CIDR
```

5. (Only for Mac/Windows) Add a route to the VM so that the tester process can connect to the pods directly
   - On Mac: (Here `VM-IP` is the same as the `registry-ip` found above)
```bash
sudo route -n add -net 10.1.0.0/16 VM-IP
```

6. (Only for Mac/Windows) Add the CIDR IP block of the host in the [./pkg/k8s/netpolicy.go](./pkg/k8s/netpolicy.go)
   file (currently line 27). It is set to `192.168.0.0/16`, and that should *probably* work for you too.

7. Put the IP of the registry in your Docker configurations so that you can push your image to it. (also described in
   the registry setup link above)
   - On Linux it should be at `/etc/docker/daemon.json`, and on Mac it should be at `~/.docker/daemon.json`
   - This file may already contain a bunch of configurations, you want to add the following key-value pair (replace
     `registry-ip` from what you found in the last step):
```json
  "insecure-registries" : ["registry-ip:32000"]
```

8. Build all Docker images (from students) and push to the local registry
   - The Docker image names are to be the students' group names, and the tag should be the same (`cse138-hw3-v1.0`) for
     everyone. You can rebuild a team's image and push it again, if the code changes.
   - You can see this in the [./cmd/hw3-grader/main.go](cmd/hw3-grader/main.go) file
   - Again, you should replace `registry-ip` with what you found above
```bash
cd team-name_project-dir
docker build -t registry-ip/team-name:cse138-hw3-v1.0
docker push registry-ip/team-name:cse138-hw3-v1.0
```

9. Run the tests for each student group with the env variable `GROUP` containing the group (Docker image) name
```bash
GROUP=team-name go run ./cmd/hw3-grader
```


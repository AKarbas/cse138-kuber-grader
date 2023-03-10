# UCSC CSE138 Winter 2023 Assignment Grader ([Course Page](https://github.com/palvaro/CSE138-Winter23))

I ran this against a MicroK8s cluster, with the local registry enabled. Roughly:
1. Install MicroK8s and launch a cluster
2. Enable the registry addon
3. Build all Docker images (from students) and push to the local registry
   - May need to configure the Docker client to connect to the registry only over HTTP (defaults to HTTPS and fails to be more secure)
   - The Docker image names were the students groups, the tag was static
     - Can see this in the [./cmd/hw3-grader/main.go](cmd/hw3-grader/main.go) file
4. Run the tests for each student group, with the env variable `GROUP` containing the group (Docker image) name
   - Can use GNU Parallel to launch multiple at the same time
   - This doesn't work well if not on Linux if you don't increase the resources allocated to the MicroK8s node VM
   - May need to add a route to ensure requests reach the pods inside the VM

Extra:
- When running the grader from the VMM host,
  1. A route may be needed to ensure requests reach the pods
  2. Must also change the hardcoded host CIDR block in [./pkg/k8s/netpolicy.go](./pkg/k8s/netpolicy.go)

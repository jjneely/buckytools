# Quick graphite cluster setup on local machine 

The main use case is testing of distributed operations on graphite files.

## How to use
1. (optional) Replace the `bucky` and `buckyd` execs in the root directory with the desired version.
2. Run `docker-compose build`. This will include the executables into the Docker containers, and will build the containers themselves.
3. Run `docker-compose up -d`. This will start the cluster

Examine cluster with `docker ps`
Connect to separate instances with `docker exec -it graphite-cluster-docker_host{1,2,3}_1 /bin/bash`

## Cluster structure
The cluster runs three instances each running *go-carbon*, *Grafana*, *carbon-api*, and *buckyd*. They constitute a cluster managed by *buckyd*. Instances also have *bucky* installed.

Mapping of the services to `localhost` ports:

| service/node | host1 | host2 | host3 |
|---|---|---|---|
|Grafana | localhost:81 | localhost:82 | localhost:83 | 
|Graphite line interface | localhost:2103 | localhost:2203 | localhost:2303 |

The folders `storage{1,2,3}` map to the graphite storage in `host{1,2,3}`. One can examine the contest of the graphite storages there.

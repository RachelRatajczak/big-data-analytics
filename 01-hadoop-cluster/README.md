# 01 — Hadoop cluster setup
Standalone to fully distributed mode on a 3-node Ubuntu VM cluster

> Graduate coursework — University of Illinois Springfield, Data Analytics (CSC 534)

![Hadoop](https://img.shields.io/badge/Hadoop-3.x-blue) ![HDFS](https://img.shields.io/badge/HDFS-YARN-blue) ![Ubuntu](https://img.shields.io/badge/OS-Ubuntu-orange)

## Cluster architecture

| Node | Hostname | IP | Daemons |
|------|----------|----|---------|
| Master | rrata-hm | 192.168.1.101 | NameNode, 2° NameNode, ResourceManager, JobHistory Server, NodeManager, DataNode |
| Worker 1 | rrata-hw1 | 192.168.1.102 | DataNode, NodeManager |
| Worker 2 | rrata-hw2 | 192.168.1.103 | DataNode, NodeManager |

The master node also acts as a worker given the small cluster size.

---

## Phase 1 — standalone mode

1. Install Java JDK: `sudo apt update && sudo apt install -y default-jdk`
2. Download Hadoop tarball via `wget`, extract to `/usr/local/hadoop`, set ownership to `csc:csc`
3. Edit `~/.bashrc` — set `JAVA_HOME`, `HADOOP_HOME`, update `PATH` with `bin/` and `sbin/`
4. Verify: `hadoop version`

---

## Phase 2 — fully distributed mode

### Hostname and hosts mapping
All three nodes share the same `/etc/hosts`:
```
192.168.1.101   rrata-hm
192.168.1.102   rrata-hw1
192.168.1.103   rrata-hw2
```

### Passwordless SSH
```bash
ssh-keygen -t rsa -P ""
ssh-copy-id csc@rrata-hm
ssh-copy-id csc@rrata-hw1
ssh-copy-id csc@rrata-hw2
chmod 600 ~/.ssh/authorized_keys
```
Required so the master can start/stop daemons on all nodes without password prompts.

### Hadoop config files

| File | Purpose |
|------|---------|
| `core-site.xml` | NameNode URI — `hdfs://rrata-hm:9000` |
| `hdfs-site.xml` | Replication factor, NameNode + DataNode storage directories |
| `yarn-site.xml` | ResourceManager hostname, NodeManager shuffle service |
| `mapred-site.xml` | Execution framework = yarn, JobHistory Server ports (10020 / 19888) |
| `workers` | Lists all three nodes as DataNodes/NodeManagers |
| `hadoop-env.sh` | Explicit JAVA_HOME for Hadoop daemon processes |

### Push config to workers
```bash
ssh csc@rrata-hw1 "sudo chown -R csc:csc /usr/local"
ssh csc@rrata-hw2 "sudo chown -R csc:csc /usr/local"
scp -r /usr/local/hadoop csc@rrata-hw1:/usr/local/hadoop
scp -r /usr/local/hadoop csc@rrata-hw2:/usr/local/hadoop
```

### Format and start
```bash
hadoop namenode -format       # run once — creates NameNode directory structure
start-dfs.sh                  # NameNode on master, DataNodes on all nodes
start-yarn.sh                 # ResourceManager on master, NodeManagers on all nodes
mapred --daemon start historyserver
```

---

## HDFS verification
```bash
hdfs dfs -mkdir -p /user/csc
echo "hello hadoop" > testfile.txt
hdfs dfs -put testfile.txt /user/csc/
hdfs fsck /user/csc/testfile.txt -files -blocks -locations
```
fsck result: status HEALTHY · 1 block (34 bytes) · replication = 1 · block confirmed on DataNode port 50010.

---

## Web UIs

| UI | URL | What it shows |
|----|-----|---------------|
| NameNode | 192.168.1.101:9870 | Cluster health, HDFS storage, journal status |
| DataNode (worker 1) | 192.168.1.102:9864 | Block pool, NameNode connection, volume capacity |
| DataNode (worker 2) | 192.168.1.103:9864 | Same as worker 1 |
| YARN ResourceManager | 192.168.1.101:8088 | 3 active nodes, cluster metrics, scheduler, app queue |
| JobHistory Server | 192.168.1.101:19888 | Completed/failed MapReduce jobs, task stats |

---

## Key concepts demonstrated
- Standalone vs. fully distributed Hadoop modes
- NameNode (metadata management) vs. DataNode (block storage)
- YARN: ResourceManager (scheduling) vs. NodeManager (execution)
- Why passwordless SSH is required for multi-node operation
- HDFS block replication across DataNodes

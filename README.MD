### rckit 
a redis cluster manager tool implement by rust.

#### create cluster
```bash
./rckit create -n 127.0.0.1:{7000,7001,7002,7003,7004,7005} -s 1
```

#### add node
```bash 
# add master and slave
./rckit add -c 127.0.0.1:7000 -n 127.0.0.1:7007,127.0.0.1:7006
# add master
./rckit add -c 127.0.0.1:7000 -n 127.0.0.1:7007
```

#### delete node
```bash
# delete node 7003  from cluster.
./rckit delete -n 127.0.0.1:7003
```

#### fix cluster
```bash
# fix cluster.
./rckit delete -n 127.0.0.1:7003
```
#### migrate slots
```bash
# migrate 10 slots from other slots to 7003
./rckit migrate  -c 10 -d 127.0.0.1:7003
# migrate 10 slots from 7001 to 7003
./rckit migrate -s 127.0.0.1:7001 -c 10  -d 127.0.0.1:7003
# migrate all slots from 7001 to 7003
./rckit migrate -s 127.0.0.1:7001  -d 127.0.0.1:7003
# migrate all 10 slots from 7001 to other node
./rckit migrate -s 127.0.0.1:7001  -c 10
```

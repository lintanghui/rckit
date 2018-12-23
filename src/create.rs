use cluster::{Cluster, Error, Node, Role};
use std::collections::HashMap;
use util;
#[test]
fn test_cluster() {
    let mut addrs: Vec<&str> = Vec::new();
    addrs.push("127.0.0.1:9999");
    addrs.push("127.0.0.2:8888");
    addrs.push("127.0.0.3:8888");
    addrs.push("127.0.0.4:8888");
    addrs.push("127.0.0.1:8889");
    addrs.push("127.0.0.2:8889");
    addrs.push("127.0.0.3:8889");
    addrs.push("127.0.0.4:8889");

    let mut cluster = Create::new(addrs, 4, 4).unwrap();
    cluster.init_slots();
    assert_eq!(cluster.master.len(), 4);
    assert_eq!(cluster.slots.len(), 4);
    assert_eq!(cluster.slave.len(), 4);
    println!("{:?}", cluster.slave.pop());
    println!("{:?}", cluster.slave.pop());
    println!("{:?}", cluster.slave.pop());
    println!("{:?}", cluster.slave.pop());
}
#[test]
fn test_split_slots() {
    let mut res = slpit_slots(100, 3).unwrap();
    println!("{:?}", res);
    assert_eq!(res.len(), 3);
    assert_eq!(res.pop(), Some(Chunk(67, 100)));
    assert_eq!(res.pop(), Some(Chunk(34, 67)));
    assert_eq!(res.pop(), Some(Chunk(0, 34)));
}

#[test]
fn test_spread() {
    let mut map = HashMap::new();
    let node1 = Node::new(b"aa::bb").unwrap();
    let node2 = Node::new(b"bb::bb").unwrap();
    let node3 = Node::new(b"cc::bb").unwrap();
    let node4 = Node::new(b"dd:bb").unwrap();
    map.insert("11", vec![node1, node2]);
    map.insert("13", vec![node3, node4]);
    let mut target = spread(&mut map, 3).unwrap();
    assert_eq!(target.len(), 3);
    println!("{:?}", target.pop());
    println!("{:?}", target.pop());
    println!("{:?}", target.pop());
}

pub struct Create {
    pub cluster: Cluster,
    master_count: usize,
    slave_count: usize,
    slots: Vec<Chunk>,
    master: Vec<Node>,
    slave: Vec<Node>,
}

const CLUSTER_SLOTS: usize = 16384;

impl Create {
    pub fn new(
        addrs: Vec<&str>,
        mut master_count: usize,
        slave_count: usize,
    ) -> Result<Create, Error> {
        let mut nodes = Vec::new();
        for n in addrs.into_iter() {
            let mut node = Node::new(n.as_bytes()).unwrap();
            node.connect();
            nodes.push(node);
        }
        let mut create = Create {
            cluster: Cluster::new(nodes),
            master_count,
            slave_count,
            slots: vec![],
            master: vec![],
            slave: vec![],
        };
        if master_count == 0 {
            master_count = create.cluster.len() / (slave_count + 1);
        }
        if master_count < 3 {
            Err(Error::BadCluster)
        } else {
            create.master_count = master_count;
            create.slave_count = slave_count;
            Ok(create)
        }
    }

    pub fn init_slots(&mut self) {
        let slaves = {
            let mut ips = HashMap::new();
            for n in &self.cluster.nodes {
                let key = &*n.ip;
                ips.entry(key).or_insert(vec![]).push(n.clone());
            }
            self.master = spread(&mut ips, self.master_count).expect("spread master err");
            println!("create redis cluster");
            println!("distribute master");
            for node in &self.master {
                println!("master: {:?}", node);
            }
            self.slots = slpit_slots(CLUSTER_SLOTS, self.master_count).unwrap();
            spread(&mut ips, self.cluster.len() - self.master_count).unwrap()
        };
        self.distribute_slave(slaves);
        println!("distributie slave");
        for node in &self.slave {
            println!("slave: {:?}", node);
        }
    }
    pub fn add_slots(&mut self) {
        for node in &self.master {
            let chunk = &self.slots.pop().unwrap();
            node.add_slots(&(chunk.0..chunk.1).into_iter().collect::<Vec<usize>>());
        }
    }

    pub fn set_config_epoch(&self) {
        let epoch = 1;
        for node in &self.master {
            node.set_config_epoch(epoch);
        }
    }
    pub fn join_cluster(&mut self) {
        if self.cluster.len() == 0 {
            return;
        }
        let first_node = self.cluster.nodes.pop().unwrap();
        for node in &self.cluster.nodes {
            first_node.meet(&*node.ip, &*node.port);
        }
    }
    pub fn set_slave(&self) -> Result<(), Error> {
        for node in &self.slave {
            let _: () = node.set_slave();
        }
        Ok(())
    }
    fn distribute_slave(&mut self, slaves: Vec<Node>) {
        let mut inuse = HashMap::new();
        loop {
            for master in &self.master {
                for slave in &slaves {
                    // if master.ip == slave.ip {
                    //     continue;
                    // }
                    let mut key = String::from(slave.ip.clone() + ":" + &slave.port);
                    if inuse.contains_key(&key) {
                        continue;
                    }
                    inuse.insert(key, slave);
                    let mut s =
                        Node::new((slave.ip.clone() + ":" + &*slave.port).as_bytes()).unwrap();
                    s.slaveof = Some(master.name.clone());
                    self.slave.push(s);
                    break;
                }
            }
            if self.slave.len() >= slaves.len() {
                break;
            }
        }
    }
    pub fn consistent(&self) -> bool {
        self.cluster.consistency()
    }
}

pub fn slpit_slots(n: usize, m: usize) -> Option<Vec<Chunk>> {
    let chunks = util::divide(n, m);
    let mut res = Vec::new();
    let mut total: usize = 0;
    for num in chunks.into_iter() {
        res.push(Chunk(total, total + num));
        total = total + num;
    }
    Some(res)
}

#[derive(Debug)]
pub struct Chunk(usize, usize);
impl PartialEq for Chunk {
    fn eq(&self, other: &Chunk) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

fn spread(nodes: &mut HashMap<&str, Vec<Node>>, n: usize) -> Option<Vec<Node>> {
    let mut target: Vec<Node> = Vec::new();
    let len = {
        let mut len: usize = 0;
        for (_, v) in nodes.into_iter() {
            len = len + v.len()
        }
        len
    };
    if len < n {
        return None;
    }
    loop {
        for (_, v) in nodes.into_iter() {
            if target.len() >= n {
                return Some(target);
            }
            let node = v.pop().unwrap();
            // println!("{:?} {} {}", &node, target.len(), n);
            target.push(node);
        }
    }
    // let target = nodes
    //     .into_iter()
    //     .map(|(_, v)| v.clone())
    //     .flatten()
    //     .take(n)
    //     .collect();
    // Some(target)
}

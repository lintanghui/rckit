use cluster::{Error, Node};
use conn::Conn;
use std::collections::HashMap;

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

    let mut cluster = Cluster::new(addrs, 4, 4).unwrap();
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
    let node1 = Node {
        name: Some(String::from("aa")),
        ip: String::from("aa"),
        port: String::from("bb"),
        slaveof: None,
    };
    let node2 = Node {
        name: Some(String::from("aa")),
        ip: String::from("bb"),
        port: String::from("bb"),
        slaveof: None,
    };
    let node3 = Node {
        name: Some(String::from("aa")),
        ip: String::from("cc"),
        port: String::from("bb"),
        slaveof: None,
    };
    let node4 = Node {
        name: Some(String::from("aa")),
        ip: String::from("dd"),
        port: String::from("bb"),
        slaveof: None,
    };
    map.insert("11", vec![node1, node2]);
    map.insert("13", vec![node3, node4]);
    let mut target = spread(&mut map, 3).unwrap();
    assert_eq!(target.len(), 3);
    println!("{:?}", target.pop());
    println!("{:?}", target.pop());
    println!("{:?}", target.pop());
}

pub struct Cluster {
    nodes: Vec<Node>,
    master_count: usize,
    slave_count: usize,
    slots: Vec<Chunk>,
    master: Vec<Node>,
    slave: Vec<Node>,
}

const CLUSTER_SLOTS: usize = 16384;

impl Cluster {
    pub fn new(
        addrs: Vec<&str>,
        mut master_count: usize,
        slave_count: usize,
    ) -> Result<Cluster, Error> {
        let mut nodes = Vec::new();
        for n in addrs.into_iter() {
            let node = Node::new(n.as_bytes()).unwrap();
            nodes.push(node);
        }
        let mut cluster = Cluster {
            nodes,
            master_count,
            slave_count,
            slots: vec![],
            master: vec![],
            slave: vec![],
        };
        if master_count == 0 {
            master_count = cluster.nodes.len() / (slave_count + 1);
        }
        if master_count < 3 {
            Err(Error::BadCluster)
        } else {
            cluster.master_count = master_count;
            cluster.slave_count = slave_count;
            Ok(cluster)
        }
    }
    pub fn check(&mut self) -> Result<(), Error> {
        for mut node in self.nodes.iter_mut() {
            let conn = Conn::new(node.ip.clone(), node.port.clone());
            let nodes_info = conn.node_info();
            println!("{:?}", nodes_info);
            assert_eq!(
                nodes_info.get("cluster_known_nodes").cloned(),
                Some("1".to_string())
            );
            let mut nodes = try!(conn.nodes());
            let n = nodes.pop().unwrap();
            println!("get node {:?}", n);
            node.name = n.name;
        }
        Ok(())
    }
    pub fn init_slots(&mut self) {
        let slaves = {
            let mut ips = HashMap::new();
            for n in &self.nodes {
                let key = &*n.ip;
                ips.entry(key).or_insert(vec![]).push(n.clone());
            }
            self.master = spread(&mut ips, self.master_count).expect("spread master err");
            self.slots = slpit_slots(CLUSTER_SLOTS, self.master_count).unwrap();
            spread(&mut ips, self.nodes.len() - self.master_count).unwrap()
        };
        println!("master {:?} slots{:?}", self.master, self.slots);
        self.distribute_slave(slaves);
    }
    pub fn add_slots(&mut self) {
        for node in &self.master {
            let conn = Conn::new(node.ip.clone(), node.port.clone());
            let chunk = &self.slots.pop().unwrap();
            conn.add_slots(&(chunk.0..chunk.1).into_iter().collect::<Vec<usize>>());
        }
    }

    pub fn set_config_epoch(&self) {
        let epoch = 1;
        for node in &self.master {
            let conn = Conn::new(node.ip.clone(), node.port.clone());
            conn.set_config_epoch(epoch);
        }
    }
    pub fn join_cluster(&mut self) {
        if self.nodes.len() == 0 {
            return;
        }
        let first_node = self.nodes.pop().unwrap();
        for node in &self.nodes {
            let conn = Conn::new(node.ip.clone(), node.port.clone());
            conn.meet(&*first_node.ip, &*first_node.port);
        }
    }
    pub fn set_slave(&self) -> Result<(), Error> {
        for node in &self.slave {
            println!("set {} replicate of {:?}", node.port, node.slaveof);
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
                    let mut s = Node {
                        name: None,
                        ip: slave.ip.clone(),
                        port: slave.port.clone(),
                        slaveof: master.name.clone(),
                    };
                    self.slave.push(s);
                    break;
                }
            }
            println!("{:?} {}", self.slave, self.slave.len());
            if self.slave.len() >= slaves.len() {
                break;
            }
        }
    }
}

pub fn slpit_slots(n: usize, m: usize) -> Option<Vec<Chunk>> {
    let chunks = divide(n, m);
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

fn divide(n: usize, m: usize) -> Vec<usize> {
    let avg = n / m;
    let remain = n % m;
    let mut c = Vec::new();
    let mut i = 0;
    while i < m {
        if i < remain {
            c.push(avg + 1);
        } else {
            c.push(avg);
        }
        i = i + 1;
    }
    c
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
            println!("{:?} {} {}", &node, target.len(), n);
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

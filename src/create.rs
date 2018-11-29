#![feature(vec_remove_item)]
use std::collections::HashMap;
use std::io;
use std::result;

#[test]
fn test_cluster() {
    let mut addrs: Vec<&[u8]> = Vec::new();
    addrs.push(b"127.0.0.1:9999");
    addrs.push(b"127.0.0.2:8888");
    addrs.push(b"127.0.0.3:8888");
    addrs.push(b"127.0.0.4:8888");
    addrs.push(b"127.0.0.5:8888");
    addrs.push(b"127.0.0.6:8888");
    addrs.push(b"127.0.0.7:8888");
    addrs.push(b"127.0.0.8:8888");

    let mut cluster = Cluster::new(addrs, 4, 4).unwrap();
    cluster.init_slots();
    assert_eq!(cluster.master.len(), 4);
    assert_eq!(cluster.slots.len(), 4);
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
fn test_node_init() {
    let node = Node::new(b"127.0.0.1:8888").unwrap();
    assert_eq!(node.ip, "127.0.0.1");
    assert_eq!(node.port, "8888");
}
pub static COLON_STR: &'static str = ":";
#[derive(Debug)]
pub enum Error {
    None,
    BadAddr,
    BadCluster,
    IoError(io::Error),
}
#[test]
fn test_spread() {
    let mut map = HashMap::new();
    let node1 = Node {
        ip: String::from("aa"),
        port: String::from("bb"),
        slave: None,
    };
    let node2 = Node {
        ip: String::from("bb"),
        port: String::from("bb"),
        slave: None,
    };
    let node3 = Node {
        ip: String::from("cc"),
        port: String::from("bb"),
        slave: None,
    };
    let node4 = Node {
        ip: String::from("dd"),
        port: String::from("bb"),
        slave: None,
    };
    map.insert("11", vec![node1, node2]);
    map.insert("13", vec![node3, node4]);
    let mut target = spread(&map, 3).unwrap();
    assert_eq!(target.len(), 3);
    println!("{:?}", target.pop());
    println!("{:?}", target.pop());
    println!("{:?}", target.pop());
}

pub type AsResult<T> = result::Result<T, Error>;

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
        addrs: Vec<&[u8]>,
        mut master_count: usize,
        slave_count: usize,
    ) -> Result<Cluster, Error> {
        let mut nodes = Vec::new();
        for n in addrs.into_iter() {
            let node = Node::new(n).unwrap();
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
        if master_count <= 3 {
            Err(Error::BadCluster)
        } else {
            cluster.master_count = master_count;
            cluster.slave_count = slave_count;
            Ok(cluster)
        }
    }
    pub fn init_slots(&mut self) {
        let mut ips = HashMap::new();
        for n in &self.nodes {
            ips.entry(key.clone()).or_insert(vec![]).push(n.clone());
        }
        self.master = spread(&ips, self.master_count).unwrap();
        self.slots = slpit_slots(CLUSTER_SLOTS, self.master_count).unwrap();
        let slaves = spread(&ips, self.nodes.len() - self.master_count).unwrap();
        self.distribute_slave(slaves);
    }

    fn distribute_slave(&mut self, slaves: Vec<Node>) {
        let mut inuse = HashMap::new();
        loop {
            for master in &self.master {
                for slave in &slaves {
                    if master.ip == slave.ip {
                        continue;
                    }
                    let mut key = String::from(slave.ip.clone());
                    if inuse.contains_key(&key) {
                        continue;
                    }
                    inuse.insert(key, slave);
                    let mut slaveof = String::from(master.ip.clone() + &master.port);
                    let mut s = Node {
                        ip: slave.ip.clone(),
                        port: slave.port.clone(),
                        slave: Some(slaveof),
                    };
                    self.slave.push(s);
                }
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
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    ip: String,
    port: String,
    slave: Option<String>,
}

impl Node {
    fn new(addr: &[u8]) -> AsResult<Node> {
        let content = String::from_utf8_lossy(addr);
        let mut items: Vec<&str> = content.split(COLON_STR).collect();
        if items.len() != 2 {
            Err(Error::BadAddr)
        } else {
            Ok(Node {
                port: items.pop().unwrap().to_string(),
                ip: items.pop().unwrap().to_string(),
                slave: None,
            })
        }
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

fn spread(nodes: &HashMap<&str, Vec<Node>>, n: usize) -> Option<Vec<Node>> {
    let target = nodes
        .into_iter()
        .map(|(_, v)| v.clone())
        .flatten()
        .take(n)
        .collect();
    Some(target)
}

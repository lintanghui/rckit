use conn::Conn;
use std::collections::HashMap;
use std::result;
use std::str;
pub static COLON_STR: &'static str = ":";
#[test]
fn test_node_init() {
    let node = Node::new(b"127.0.0.1:8888").unwrap();
    assert_eq!(node.ip, "127.0.0.1");
    assert_eq!(node.port, "8888");
}
#[test]
fn test_consistency() {
    let mut nodes = Vec::new();
    nodes.push(Node::new(b"127.0.0.1:7000").unwrap());
    nodes.push(Node::new(b"127.0.0.1:7001").unwrap());
    nodes.push(Node::new(b"127.0.0.1:7002").unwrap());
    nodes.push(Node::new(b"127.0.0.1:7003").unwrap());
    nodes.push(Node::new(b"127.0.0.1:7004").unwrap());
    let cluster = Cluster::new(nodes);
    assert_eq!(cluster.consistency(), true);
}
#[derive(Debug, PartialEq, Clone)]
pub enum Role {
    master,
    slave,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub name: String,
    pub ip: String,
    pub port: String,
    role: Option<Role>,
    pub slaveof: Option<String>,
    pub slots: Option<Vec<usize>>,
}
#[derive(Debug)]
pub struct Cluster {
    pub nodes: Vec<Node>,
}

impl Cluster {
    pub fn new(nodes: Vec<Node>) -> Cluster {
        Cluster { nodes }
    }
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
    pub fn consistency(&self) -> bool {
        let mut node_slot: HashMap<usize, Node> = HashMap::new();
        for node in &self.nodes {
            let mut slot_num = 0;
            let conn = Conn::new(node.ip.clone(), node.port.clone());
            let nodes = conn.nodes().expect("get cluster nodes err");
            println!("node{:?}  {:?}", node.port, nodes.len());
            for node in nodes.into_iter() {
                match node.slots.as_ref() {
                    Some(slots) => {
                        for slot in slots {
                            if node_slot.contains_key(&slot) {
                                if node_slot.get(&slot).expect("get slots err").clone()
                                    != node.clone()
                                {
                                    println!(
                                        "slot {} want {:?} get {:?}",
                                        slot,
                                        node_slot.get(&slot).unwrap().name,
                                        node.name
                                    );
                                    return false;
                                }
                            } else {
                                slot_num = slot_num + 1;
                                node_slot.insert(slot.clone(), node.clone());
                            }
                        }
                    }
                    None => continue,
                }
            }
            if slot_num != 16384 {
                println!("all slots not covered");
                return false;
            }
        }
        true
    }
    pub fn check(&mut self) -> Result<(), Error> {
        for mut node in self.nodes.iter_mut() {
            let conn = Conn::new(node.ip.clone(), node.port.clone());
            let nodes_info = conn.node_info();
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
}

impl Node {
    pub fn new(addr: &[u8]) -> AsResult<Node> {
        let content = String::from_utf8_lossy(addr);
        let mut items: Vec<&str> = content.split(COLON_STR).collect();
        if items.len() != 2 {
            Err(Error::BadAddr)
        } else {
            Ok(Node {
                name: str::from_utf8(addr).unwrap().to_string(),
                role: None,
                port: items.pop().unwrap().to_string(),
                ip: items.pop().unwrap().to_string(),
                slaveof: None,
                slots: None,
            })
        }
    }
    pub fn set_role(&mut self, role: Role) {
        self.role = Some(role);
    }
    pub fn set_slave(&self) {
        let conn = Conn::new(self.ip.clone(), self.port.clone());
        conn.set_slave(self.slaveof.clone().unwrap());
    }
    pub fn addr(&self) -> String {
        self.ip.clone() + ":" + &*self.port
    }
}
#[derive(Debug)]
pub enum Error {
    None,
    BadAddr,
    BadCluster,
}
pub type AsResult<T> = result::Result<T, Error>;

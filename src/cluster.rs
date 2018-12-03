use conn::Conn;
use std::collections::HashMap;
use std::result;
pub static COLON_STR: &'static str = ":";
#[test]
fn test_node_init() {
    let node = Node::new(b"127.0.0.1:8888").unwrap();
    assert_eq!(node.ip, "127.0.0.1");
    assert_eq!(node.port, "8888");
}

#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub name: Option<String>,
    pub ip: String,
    pub port: String,
    pub slaveof: Option<String>,
    pub slots: Option<Vec<usize>>,
}
pub struct Cluster {
    nodes: Vec<Node>,
}
impl Cluster {
    fn new(nodes: Vec<Node>) -> Cluster {
        Cluster { nodes }
    }
    fn consistency(&self) -> bool {
        let mut node_slot: HashMap<usize, Node> = HashMap::new();
        for node in &self.nodes {
            let conn = Conn::new(node.ip.clone(), node.port.clone());
            let nodes = conn.nodes().expect("get cluster nodes err");
            for node in nodes.into_iter() {
                let slots = node.slots.clone();
                for slot in slots.unwrap() {
                    if node_slot.contains_key(&slot) {
                        if node_slot.get(&slot).unwrap().clone() != node.clone() {
                            return false;
                        }
                    } else {
                        node_slot.insert(slot.clone(), node.clone());
                    }
                }
            }
        }
        true
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
                name: None,
                port: items.pop().unwrap().to_string(),
                ip: items.pop().unwrap().to_string(),
                slaveof: None,
                slots: None,
            })
        }
    }
    pub fn set_slave(&self) {
        let conn = Conn::new(self.ip.clone(), self.port.clone());
        conn.set_slave(self.slaveof.clone().unwrap());
    }
}
#[derive(Debug)]
pub enum Error {
    None,
    BadAddr,
    BadCluster,
}
pub type AsResult<T> = result::Result<T, Error>;

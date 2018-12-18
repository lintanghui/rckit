use conn::Conn;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::result;
use std::str;
use util;
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

impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.ip == other.ip && self.port == other.port
    }
}
impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let slot_num;
        match &self.slots {
            Some(slots) => slot_num = slots.len(),
            None => slot_num = 0,
        }
        write!(
            f,
            "Node{{name: {:?} ,ip: {},port: {},slots: {},role:{:?},slaveof:{:?} }}",
            self.name, self.ip, self.port, slot_num, self.role, self.slaveof
        )
    }
}
#[derive(Clone)]
pub struct Node {
    pub name: String,
    pub ip: String,
    pub port: String,
    role: Option<Role>,
    pub slaveof: Option<String>,
    pub slots: Option<Vec<usize>>,
    conn: Rc<Option<Conn>>,
}
// impl Clone for Node {
//     fn clone(&self) -> Node {
//         Node {
//             conn: None,
//             name: self.name.clone(),
//             ip: self.ip.clone(),
//             port: self.port.clone(),
//             role: self.role.clone(),
//             slaveof: self.slaveof.clone(),
//             slots: self.slots.clone(),
//         }
//     }
// }
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
                                slot_num = slot_num + 1;
                            } else {
                                slot_num = slot_num + 1;
                                node_slot.insert(slot.clone(), node.clone());
                            }
                        }
                    }
                    None => println!("not slot in this node {:?}", node),
                }
            }
            println!("slot num {}", slot_num);
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

    pub fn delete_node(&self, del_node: &Node) {
        if del_node.is_master() {
            let nodes: Vec<&Node> = self
                .nodes
                .iter()
                .filter(|&x| x.role == Some(Role::master) && x.name != del_node.name)
                .collect();
            let slots = del_node.slots.as_ref().unwrap();
            let slot_count = slots.len();
            let mut dispatch = util::divide(slot_count, nodes.len());
            let mut start = 0;
            for node in nodes {
                println!("start migrate from {:?} to {:?}", del_node, node);
                let count = dispatch.pop().unwrap();
                let migrate = &slots[start..start + count];
                // todo:MIGRATE DATA
                for slot in migrate.iter() {
                    migrate_slot(del_node, node, *slot);
                }
                start = start + count;
                println!("stop migrate from {:?} to {:?}", del_node, node);
            }
        }

        let forget = |node: &Node| {
            for n in &self.nodes {
                if n.name == node.name {
                    continue;
                }
                if Some(node.name.to_string()) == n.slaveof {
                    // todo forget slav
                    continue;
                }
                println!("node {:?} forget {:?}", n, node);
                n.forget(&node);
            }
        };
        forget(del_node);
    }

    pub fn node(&self, node: &str) -> Option<&Node> {
        for n in &self.nodes {
            if &n.addr() == node {
                return Some(n);
            }
        }
        None
    }
}

pub fn migrate_slot(src: &Node, dst: &Node, slot: usize) {
    dst.setslot("IMPORTING", src.name.clone(), slot);
    src.setslot("MIGRATING", dst.name.clone(), slot);
    loop {
        match src.keysinslot(slot) {
            Some(key) => src.migrate(&*dst.ip, &*dst.port, key),
            None => break,
        }
    }
    src.setslot("NODE", dst.name.clone(), slot);
    dst.setslot("NODE", dst.name.clone(), slot);
}
impl Node {
    pub fn new(addr: &[u8]) -> AsResult<Node> {
        let content = String::from_utf8_lossy(addr);
        let items: Vec<&str> = content.split(COLON_STR).collect();
        if items.len() != 2 {
            Err(Error::BadAddr)
        } else {
            let ip = items[0];
            let port = items[1];

            let con = if ip != "" {
                Some(Conn::new(ip.to_string(), port.to_string()))
            } else {
                None
            };
            Ok(Node {
                name: str::from_utf8(addr).unwrap().to_string(),
                role: None,
                port: port.to_string(),
                ip: ip.to_string(),
                slaveof: None,
                slots: None,
                conn: Rc::new(con),
            })
        }
    }
    pub fn set_role(&mut self, role: Role) {
        self.role = Some(role);
    }
    pub fn set_slave(&self) {
        if let Some(conn) = self.conn.as_ref() {
            conn.set_slave(self.slaveof.clone().unwrap());
        } else {
            let conn = Conn::new(self.ip.clone(), self.port.clone());
            conn.set_slave(self.slaveof.clone().unwrap());
        }
    }
    pub fn addr(&self) -> String {
        self.ip.clone() + ":" + &*self.port
    }
    pub fn add_slots(&self, slots: &[usize]) {
        if let Some(conn) = self.conn.as_ref() {
            conn.add_slots(slots);
        } else {
            let conn = Conn::new(self.ip.clone(), self.port.clone());
            conn.add_slots(slots);
        }
    }
    pub fn nodes(&self) -> Vec<Node> {
        if let Some(conn) = self.conn.as_ref() {
            conn.nodes().expect("get nodes from node fail")
        } else {
            let conn = Conn::new(self.ip.clone(), self.port.clone());
            conn.nodes().expect("get nodes from node fail")
        }
    }
    pub fn getslots(&self) -> Vec<usize> {
        vec![]
    }
    fn is_master(&self) -> bool {
        self.role == Some(Role::master)
    }
    pub fn forget(&self, node: &Node) {
        if let Some(conn) = self.conn.as_ref() {
            conn.forget(&*node.name);
        } else {
            let conn = Conn::new(self.ip.clone(), self.port.clone());
            conn.forget(&*node.name);
        }
    }
    pub fn setslot(&self, state: &str, nodeid: String, slot: usize) {
        if let Some(conn) = self.conn.as_ref() {
            conn.setslot(state, slot, &*nodeid);
        } else {
            let conn = Conn::new(self.ip.clone(), self.port.clone());
            conn.setslot(state, slot, &*nodeid);
        }
    }
    fn keysinslot(&self, slot: usize) -> Option<Vec<String>> {
        if let Some(conn) = self.conn.as_ref() {
            conn.keyinslots(slot, 100)
        } else {
            let conn = Conn::new(self.ip.clone(), self.port.clone());
            conn.keyinslots(slot, 100)
        }
    }
    fn migrate(&self, dstip: &str, dstport: &str, key: Vec<String>) {
        if let Some(conn) = self.conn.as_ref() {
            conn.migrate(dstip, dstport, key);
        } else {
            let conn = Conn::new(self.ip.clone(), self.port.clone());
            conn.migrate(dstip, dstport, key);
        }
    }
}
#[derive(Debug)]
pub enum Error {
    None,
    BadAddr,
    BadCluster,
}
pub type AsResult<T> = result::Result<T, Error>;

use redis::Connection;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
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
        let slot_num = self.slots.borrow().len();
        write!(
            f,
            "Node{{name: {:?} ,ip: {},port: {},slots: {},role:{:?},slaveof:{:?} }}",
            self.name, self.ip, self.port, slot_num, self.role, self.slaveof
        )
    }
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
            let nodes = node.nodes();
            println!("node{:?}  {:?}", node.port, nodes.len());
            for node in nodes.into_iter() {
                for slot in node.slots.clone().into_inner() {
                    if node_slot.contains_key(&slot) {
                        if node_slot.get(&slot).expect("get slots err").clone() != node.clone() {
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
            println!("slot num {}", slot_num);
            if slot_num != 16384 {
                println!("all slots not covered");
                return false;
            }
        }
        true
    }
    pub fn check(&self) -> Result<(), Error> {
        for node in &self.nodes {
            let nodes_info = node.info();
            assert_eq!(
                nodes_info.get("cluster_known_nodes").cloned(),
                Some("1".to_string())
            );
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
            let slots = del_node.slots.borrow();
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
    pub fn fill_slots(&self) {
        let slots: HashSet<usize> = self
            .nodes
            .iter()
            .filter(|x| x.is_master())
            .map(|x| x.slots.clone().into_inner())
            .flatten()
            .collect();
        let all_slots: HashSet<usize> = (1..16384).into_iter().collect();
        let miss = all_slots
            .difference(&slots)
            .cloned()
            .collect::<Vec<usize>>();
        let mut dist = util::divide(miss.len(), self.nodes.len());
        let mut idx = 0;
        for node in self.nodes.iter().filter(|x| x.is_master()) {
            // let slot = miss
            //     .iter()
            //     .map(|x| x.clone())
            //     .take(dist.pop().unwrap())
            //     .collect::<Vec<usize>>();
            let num = dist.pop().unwrap();
            let slots = &miss[idx..idx + num];
            node.add_slots(slots);
            idx += num;
        }
    }
    pub fn fix_slots(&self) {
        for master in self.nodes.iter().filter(|x| x.is_master()) {
            master.fix_node();
        }
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

#[derive(Clone)]
pub struct Node {
    pub name: String,
    pub ip: String,
    pub port: String,
    role: Option<Role>,
    pub slaveof: Option<String>,
    nodes: RefCell<HashMap<String, Node>>,
    slots: RefCell<Vec<usize>>,
    migrating: HashMap<usize, String>,
    importing: HashMap<usize, String>,
    conn: Rc<Option<Connection>>,
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
                let addr = "redis://".to_string() + ip + ":" + port;
                Some(
                    redis::Client::open(&*addr)
                        .unwrap()
                        .get_connection()
                        .unwrap(),
                )
            } else {
                None
            };
            Ok(Node {
                name: str::from_utf8(addr).unwrap().to_string(),
                role: None,
                port: port.to_string(),
                ip: ip.to_string(),
                slaveof: None,
                nodes: RefCell::new(HashMap::new()),
                slots: RefCell::new(vec![]),
                migrating: HashMap::new(),
                importing: HashMap::new(),
                conn: Rc::new(con),
            })
        }
    }
    pub fn connect(&mut self) {
        for node in &self.nodes() {
            if node.ip == self.ip && node.port == self.port {
                self.name = node.name.clone();
                self.slaveof = node.slaveof.clone();
                self.slots = node.slots.clone();
            }
        }
    }

    pub fn fix_node(&self) {
        for (slot, nodeid) in &self.migrating {
            let target = self.nodes.borrow().get(nodeid).cloned().unwrap();
            if target.importing.contains_key(slot) {
                migrate_slot(self, &target, *slot);
                continue;
            }
            self.setslot("STABLE", nodeid.to_string(), *slot);
        }
        for (slot, nodeid) in &self.importing {
            let target = self.nodes.borrow().get(nodeid).cloned().unwrap();
            if target.importing.contains_key(slot) {
                migrate_slot(self, &target, *slot);
                continue;
            }
            self.setslot("STABLE", nodeid.to_string(), *slot);
        }
    }
    pub fn info(&self) -> HashMap<String, String> {
        let mut node_infos = HashMap::new();
        let a = self.conn.as_ref().as_ref().unwrap();
        let info: String = redis::cmd("CLUSTER").arg("INFO").query(a).unwrap();
        let infos: Vec<String> = info.split("\r\n").map(|x| x.to_string()).collect();

        for mut info in infos.into_iter() {
            let kv: Vec<String> = info.split(":").map(|x| x.to_string()).collect();
            if kv.len() == 2 {
                node_infos.insert(kv[0].clone(), kv[1].clone());
            }
        }
        node_infos
    }
    pub fn set_role(&mut self, role: Role) {
        self.role = Some(role);
    }
    pub fn set_slave(&self) {
        let node_id = self.slaveof.clone().unwrap();
        println!("set {}  replicate to {}", self.ip, node_id);
        if let Some(conn) = self.conn.as_ref() {
            let _: () = redis::cmd("CLUSTER")
                .arg("REPLICATE")
                .arg(&*node_id)
                .query(conn)
                .expect("cluster replicate err");
        }
    }
    pub fn addr(&self) -> String {
        self.ip.clone() + ":" + &*self.port
    }
    pub fn add_slots(&self, slots: &[usize]) {
        if let Some(conn) = self.conn.as_ref() {
            let _: () = redis::cmd("cluster")
                .arg("addslots")
                .arg(slots)
                .query(conn)
                .expect("add slots err");
        }
    }
    pub fn set_config_epoch(&self, epoch: usize) {
        if let Some(conn) = self.conn.as_ref() {
            let _: () = redis::cmd("CLUSTER")
                .arg("SET-CONFIG-EPOCH")
                .arg(epoch)
                .query(conn)
                .expect("set config epoch err");
        }
    }
    pub fn nodes(&self) -> Vec<Node> {
        if let Some(conn) = self.conn.as_ref() {
            let info: String = redis::cmd("CLUSTER").arg("NODES").query(conn).unwrap();
            // let infos: Vec<String> = info.split("\n").map(|x| x.to_string()).collect();
            let mut nodes: Vec<Node> = Vec::new();
            for mut info in info.lines() {
                let kv: Vec<String> = info.split(" ").map(|x| x.to_string()).collect();
                if kv.len() < 8 {
                    return vec![];
                }
                let mut slots = vec![];
                let mut migrating = HashMap::new();
                let mut importing = HashMap::new();
                let addr = kv[1].split('@').next().expect("must contain addr");
                let mut node = Node::new(addr.as_bytes()).unwrap();
                if kv[2].contains("master") {
                    node.set_role(Role::master);
                } else {
                    node.set_role(Role::slave);
                }
                if kv[3] != "-" {
                    node.slaveof = Some(kv[3].clone());
                }
                for content in &kv[8..] {
                    if content.contains("->-") {
                        let mut scope: Vec<&str> = content.split("->-").collect();
                        let slot = scope[0].to_string().parse::<usize>().unwrap();
                        let nodeid = scope[1];
                        migrating.insert(slot, nodeid.to_string());
                    }
                    if content.contains("-<-") {
                        let mut scope: Vec<&str> = content.split("-<-").collect();
                        let slot = scope[0].to_string().parse::<usize>().unwrap();
                        let nodeid = scope[1];
                        importing.insert(slot, nodeid.to_string());
                    }
                    let mut scope: Vec<&str> = content.split("-").collect();
                    let start = scope[0].to_string().parse::<usize>().unwrap();
                    slots.push(start);
                    if scope.len() == 2 {
                        let end = scope[1].to_string().parse::<usize>().unwrap();
                        for i in (start + 1..end + 1).into_iter() {
                            slots.push(i);
                        }
                    }
                }
                node.migrating = migrating;
                node.importing = importing;
                node.slots = RefCell::new(slots);
                node.name = kv[0].clone();
                self.nodes
                    .borrow_mut()
                    .insert(node.name.clone(), node.clone());
                nodes.push(node);
            }
            return nodes;
        }
        vec![]
    }

    pub fn meet(&self, ip: &str, port: &str) {
        if let Some(conn) = self.conn.as_ref() {
            let _: () = redis::cmd("CLUSTER")
                .arg("MEET")
                .arg(ip)
                .arg(port)
                .query(conn)
                .unwrap();
        }
    }
    pub fn slots(&self) -> Vec<usize> {
        self.slots.clone().into_inner()
    }
    pub fn is_master(&self) -> bool {
        self.role == Some(Role::master)
    }
    pub fn forget(&self, node: &Node) {
        if let Some(conn) = self.conn.as_ref() {
            let _: () = redis::cmd("CLUSTER")
                .arg("FORGET")
                .arg(&node.name)
                .query(conn)
                .unwrap();
        }
    }
    pub fn setslot(&self, state: &str, nodeid: String, slot: usize) {
        if let Some(conn) = self.conn.as_ref() {
            let _: () = redis::cmd("CLUSTER")
                .arg("SETSLOT")
                .arg(slot)
                .arg(state)
                .arg(&*nodeid)
                .query(conn)
                .unwrap();
        }
    }
    fn keysinslot(&self, slot: usize) -> Option<Vec<String>> {
        if let Some(conn) = self.conn.as_ref() {
            let result: Vec<String> = redis::cmd("CLUSTER")
                .arg("GETKEYSINSLOT")
                .arg(slot)
                .arg(100)
                .query(conn)
                .unwrap();
            if result.len() > 0 {
                return Some(result);
            }
        }
        return None;
    }
    fn migrate(&self, dstip: &str, dstport: &str, key: Vec<String>) {
        println!("migrate keys {:?}", key);
        if let Some(conn) = self.conn.as_ref() {
            let _: () = redis::cmd("MIGRATE")
                .arg(dstip)
                .arg(dstport)
                .arg("")
                .arg("0")
                .arg(5000)
                .arg("KEYS")
                .arg(key)
                .query(conn)
                .unwrap();
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

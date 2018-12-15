use cluster::{Error, Node, Role};
use redis::Commands;
use std::collections::HashMap;
#[derive(Debug)]
pub struct Conn {
    ip: String,
    port: String,
    client: redis::Client,
}
#[test]
fn test_conn() {
    let conn = Conn::new("127.0.0.1".to_string(), "7001".to_string());
    //  conn.add_slots(&[1, 2, 3]);
    let info = conn.node_info();
    println!("info {:?}", info);
    let key = conn.keyinslots(7761, 3);
    println!("{:?}", key);
    let key = conn.keyinslots(7762, 3);
    println!("{:?}", key);
}

impl Conn {
    pub fn new(ip: String, port: String) -> Conn {
        let addr = "redis://".to_string() + &ip + ":" + &port;

        let client = redis::Client::open(&*addr).expect("open redis err");
        Conn {
            ip,
            port,
            client: client,
        }
    }
    pub fn add_slots(&self, slots: &[usize]) {
        let con = self.client.get_connection().unwrap();
        let _: () = redis::cmd("cluster")
            .arg("addslots")
            .arg(slots)
            .query(&con)
            .expect("add slots err");
    }
    pub fn set_config_epoch(&self, epoch: usize) {
        let con = self.client.get_connection().unwrap();
        let _: () = redis::cmd("CLUSTER")
            .arg("SET-CONFIG-EPOCH")
            .arg(epoch)
            .query(&con)
            .expect("set config epoch err");
    }
    pub fn meet(&self, ip: &str, port: &str) {
        let con = self.client.get_connection().unwrap();
        let _: () = redis::cmd("CLUSTER")
            .arg("MEET")
            .arg(ip)
            .arg(port)
            .query(&con)
            .unwrap();
    }
    pub fn set_slave(&self, node_id: String) {
        println!("set {} to replicate {}", self.ip, node_id);
        let con = self.client.get_connection().unwrap();
        let _: () = redis::cmd("CLUSTER")
            .arg("REPLICATE")
            .arg(&*node_id)
            .query(&con)
            .expect("cluster replicate err");
    }
    pub fn node_info(&self) -> HashMap<String, String> {
        let con = self.client.get_connection().unwrap();
        let info: String = redis::cmd("CLUSTER").arg("INFO").query(&con).unwrap();
        let infos: Vec<String> = info.split("\r\n").map(|x| x.to_string()).collect();
        let mut node_infos = HashMap::new();
        for mut info in infos.into_iter() {
            let kv: Vec<String> = info.split(":").map(|x| x.to_string()).collect();
            if kv.len() == 2 {
                node_infos.insert(kv[0].clone(), kv[1].clone());
            }
        }
        node_infos
    }
    pub fn nodes(&self) -> Result<Vec<Node>, Error> {
        let con = self.client.get_connection().unwrap();
        let info: String = redis::cmd("CLUSTER").arg("NODES").query(&con).unwrap();
        // let infos: Vec<String> = info.split("\n").map(|x| x.to_string()).collect();
        let mut nodes: Vec<Node> = Vec::new();
        for mut info in info.lines() {
            let kv: Vec<String> = info.split(" ").map(|x| x.to_string()).collect();
            if kv.len() < 8 {
                return Err(Error::BadCluster);
            }
            let mut slots = vec![];
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
                if content.contains("->") || content.contains("->") {
                    continue;
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
            node.slots = Some(slots);
            node.name = kv[0].clone();
            nodes.push(node);
        }
        Ok(nodes)
    }
    pub fn health(&self) -> Result<(), Error> {
        Ok(())
    }
    pub fn forget(&self, node: &str) {
        let con = self.client.get_connection().unwrap();
        let _: () = redis::cmd("CLUSTER")
            .arg("FORGET")
            .arg(node)
            .query(&con)
            .unwrap();
    }
    pub fn setslot(&self, state: &str, slot: usize, nodeid: &str) {
        let con = self.client.get_connection().unwrap();
        if let Ok(()) = redis::cmd("CLUSTER")
            .arg("SETSLOT")
            .arg(slot)
            .arg(state)
            .arg(nodeid)
            .query(&con)
        {
            ();
        } else {
            println!("set slot{} to node{} err", slot, nodeid);
        };
    }
    pub fn migrate(&self, ip: &str, port: &str, key: Vec<String>) {
        let con = self.client.get_connection().unwrap();
        println!("migrate keys {:?}", key);
        let _: () = redis::cmd("MIGRATE")
            .arg(ip)
            .arg(port)
            .arg("")
            .arg("0")
            .arg(5000)
            .arg("KEYS")
            .arg(key)
            .query(&con)
            .unwrap();
    }
    pub fn keyinslots(&self, slot: usize, count: usize) -> Option<Vec<String>> {
        if let Ok(con) = self.client.get_connection() {
            let result: Vec<String> = redis::cmd("CLUSTER")
                .arg("GETKEYSINSLOT")
                .arg(slot)
                .arg(count)
                .query(&con)
                .unwrap();
            if result.len() > 0 {
                return Some(result);
            }
            return None;
        } else {
            println!("get connection err {:?}", self.client);
        }
        None
    }
}

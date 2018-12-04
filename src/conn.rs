use cluster::{Error, Node};
use redis::Commands;
use std::collections::HashMap;
#[derive(Debug)]
pub struct Conn {
    ip: String,
    port: String,
    client: redis::Client,
}
#[test]
fn test_conn_add_slots() {
    let conn = Conn::new("127.0.0.1".to_string(), "7008".to_string());
    //  conn.add_slots(&[1, 2, 3]);
    let info = conn.node_info();
    println!("info {:?}", info);
}

impl Conn {
    pub fn new(ip: String, port: String) -> Conn {
        let addr = "redis://".to_string() + &ip + ":" + &port;
        let client = redis::Client::open(&*addr).unwrap();
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
            let mut node = Node::new(kv[1].as_bytes()).unwrap();
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
            node.name = Some(kv[0].clone());
            nodes.push(node);
        }
        Ok(nodes)
    }
    pub fn health(&self) -> Result<(), Error> {
        Ok(())
    }
    pub fn add_node(&self, node: Node) -> Result<(), Error> {
        Ok(())
    }
}

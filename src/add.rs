use cluster::{Cluster, Error, Node, Role};
use conn::Conn;
use std::collections::HashMap;
#[derive(Debug)]
pub struct Add {
    origin: String,
    pub cluster: Cluster,
    conn: Conn,
    slave_master: HashMap<String, String>,
}

impl Add {
    pub fn new(origin: String, addrs: Vec<String>) -> Result<Add, Error> {
        let addr = origin.clone();
        let items: Vec<&str> = addr.split(":").collect();
        let conn = Conn::new(items[0].to_string(), items[1].to_string());
        conn.health().unwrap();
        let mut nodes = Vec::new();
        let mut sm = HashMap::new();
        for n in addrs.into_iter() {
            let mut ms: Vec<&str> = n.split(",").collect();
            let master_host = ms.pop().unwrap();
            let master = Node::new(master_host.as_bytes()).expect("create new node fail");
            nodes.push(master);
            if ms.len() == 1 {
                let slave_host = ms.pop().unwrap();
                let mut node = Node::new(slave_host.as_bytes()).expect("create new node fail");
                node.set_role(Role::slave);
                nodes.push(node);
                sm.insert(slave_host.to_string(), master_host.to_string());
            }
        }
        Ok(Add {
            origin,
            slave_master: sm,
            cluster: Cluster::new(nodes),
            conn,
        })
    }
    pub fn add_node(&self) -> Result<(), Error> {
        for node in &self.cluster.nodes {
            self.conn.meet(&*node.ip, &*node.port);
        }
        Ok(())
    }
    pub fn set_slave(&mut self) {
        let mut nodes_info = HashMap::new();
        for node in &self.cluster.nodes {
            nodes_info.insert(node.ip.clone() + ":" + &*node.port, node.clone());
        }
        println!("nodes_info {:?}", nodes_info);
        println!("s_m info {:?}", self.slave_master);
        for node in self.cluster.nodes.iter_mut() {
            if self.slave_master.contains_key(&*node.addr()) {
                let master = self.slave_master.get(&node.addr()).unwrap();
                let master_node = nodes_info.get(master);
                node.slaveof = Some(master_node.unwrap().clone().name);
                let _: () = node.set_slave();
            }
        }
    }
}

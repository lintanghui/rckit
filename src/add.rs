use cluster::{Error, Node};
use conn::Conn;
#[derive(Debug)]
pub struct Add {
    cluster: String,
    nodes: Vec<String>,
    conn: Conn,
}

impl Add {
    pub fn new(&self, cluster: String, nodes: Vec<String>) -> Result<Add, Error> {
        let addr = cluster.clone();
        let mut items: Vec<&str> = addr.split(":").collect();
        let conn = Conn::new(
            items.pop().unwrap().to_string(),
            items.pop().unwrap().to_string(),
        );
        conn.health().unwrap();
        Ok(Add {
            cluster,
            nodes,
            conn,
        })
    }
    pub fn add_node(&self) -> Result<(), Error> {
        let nodes: Vec<(Node, Node)> = self
            .nodes
            .iter()
            .map(|x| {
                let mut couple: Vec<_> = x.split(",").collect();
                let master = couple.pop().unwrap();
                let slave = couple.pop().unwrap();
                (Node::new(master.as_bytes()).unwrap(), {
                    let mut node = Node::new(slave.as_bytes()).unwrap();
                    node.slaveof = Some(master.to_string());
                    node
                })
            })
            .collect();
        for (master, slave) in nodes {
            self.conn.meet(&*master.ip, &*master.port);
            self.conn.meet(&*slave.ip, &*slave.port);
            // TODO:wait consistency and set replicate
        }
        Ok(())
    }
}

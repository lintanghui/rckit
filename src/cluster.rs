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
            })
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

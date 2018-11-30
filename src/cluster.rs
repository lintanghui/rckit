use redis::Commands;
pub struct Conn {
    ip: String,
    port: String,
    client: redis::Client,
}
#[test]
fn test_conn_add_slots() {
    let conn = Conn::new("127.0.0.1:".to_string(), "6379".to_string());
    conn.add_slots(&[1, 2, 3]);
}
impl Conn {
    pub fn new(ip: String, port: String) -> Conn {
        let addr = "redis://".to_string() + &ip + &port;
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
            .unwrap();
    }
    pub fn set_config_epoch(&self, epoch: usize) {
        let con = self.client.get_connection().unwrap();
        let _: () = redis::cmd("CLUSTER")
            .arg("SET-CONFIG-EPOCH")
            .arg(epoch)
            .query(&con)
            .unwrap();
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
}

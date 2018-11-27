pub struct node {
    ip: String,
    port: u8,
    slave: String,
}

pub struct cluster {
    nodes: Vec<node>,
    slots: Vec<Vec<u32>>,
    master: Vec<node>,
    slaveof: Option<String>,
}

const cluster_slots: u32 = 16384;
#[test]
fn test_split_slots() {
    let res = cluster::slpit_slots(100, 3);
    println!("{:?}", res);
    assert_eq!(res.len(), 3);
}
impl cluster {
    pub fn slpit_slots(n: u32, m: u32) -> Vec<chunk> {
        let chunks = divide(n, m);
        println!("{:?}", chunks);
        let mut res = Vec::new();
        let mut total: u32 = 0;
        for num in chunks.into_iter() {
            res.push(chunk(total, total + num));
        }
        res
    }
}
#[derive(Debug)]
pub struct chunk(u32, u32);
fn divide(n: u32, m: u32) -> Vec<u32> {
    let avg = n / m;
    let remain = n % m;
    let mut c = Vec::new();
    let mut i = 0;
    while i < m {
        if i < remain {
            c.push(avg + 1);
        } else {
            c.push(avg);
        }
        i = i + 1;
    }
    c
}

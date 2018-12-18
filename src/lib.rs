extern crate clap;
extern crate redis;

mod add;
mod cluster;
mod conn;
mod create;
mod util;

use add::Add;
use clap::{App, Arg, SubCommand};
use cluster::{Cluster, Node};
use create::Create;
use std::{thread, time};
pub fn run() {
    let matches = App::new("rckit")
        .about("redis cluster tool")
        .subcommand(
            SubCommand::with_name("create")
                .about("create rredis cluster")
                .arg(
                    Arg::with_name("node")
                        .help("cluster nodes")
                        .short("n")
                        .required(true)
                        .multiple(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("replicate")
                        .short("s")
                        .default_value("1")
                        .help("slave replicate number "),
                )
                .arg(
                    Arg::with_name("master")
                        .short("m")
                        .default_value("0")
                        .takes_value(true)
                        .help("mster number"),
                ),
        )
        .subcommand(
            SubCommand::with_name("add")
                .about("Add  node to existing cluster")
                .arg(
                    Arg::with_name("cluster")
                        .required(true)
                        .short("c")
                        .help("-c clusterip:port, spec cluster ip and  port")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("node")
                        .required(true)
                        .short("n")
                        .help(
                            "add new node  to cluster,
                        -n <master,slave> <master>",
                        )
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("delete")
                .about(
                    "delete node from cluster.\r\nif node is a master,it will migrate slots to other node and delete is's slave too",
                )
                .arg(
                    Arg::with_name("node")
                        .help("-n <node>")
                        .short("n")
                        .required(true)
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("migrate")
            .about("migrate slots from src to dst")
            .arg(
                Arg::with_name("dst")
                    .short("d")
                    .help("-d <node>")
                    .required(true)
                    .takes_value(true),
                )
            .arg(
                Arg::with_name("src")
                .short("s")
                .help("-s <node>")
                .required(true)
                .takes_value(true),
                )
            .arg(
                Arg::with_name("count")
                .help("-c count")
                .short("c")
                .required(true)
                .takes_value(true),
            ),
        ).get_matches();
    if let Some(sub_m) = matches.subcommand_matches("create") {
        let slave_count = clap::value_t!(sub_m.value_of("replicate"), usize).unwrap();
        let mut master_count = clap::value_t!(sub_m.value_of("master"), usize).unwrap();
        let node: Vec<&str> = sub_m.values_of("node").unwrap().collect();
        println!(
            "create cluster with replicate {} node{:?}",
            slave_count, &node
        );
        let mut create = Create::new(node, master_count, slave_count).unwrap();
        create.cluster.check().expect("check node err");
        create.init_slots();
        create.add_slots();
        create.set_config_epoch();
        create.join_cluster();
        while !create.consistent() {
            eprintln!("wait consistent fail");
            thread::sleep(time::Duration::from_secs(1));
        }
        create.set_slave().expect("set slave err");
    }
    if let Some(sub_m) = matches.subcommand_matches("add") {
        let cluster = sub_m
            .value_of("cluster")
            .expect("must spec existing cluster node");
        let nodes: Vec<&str> = sub_m
            .values_of("node")
            .expect("must spec at least one node be add to cluster")
            .collect();
        println!("add node {:?} to cluster {}", nodes, cluster);
        let mut add = Add::new(
            cluster.to_string(),
            nodes.iter().map(|x| x.to_string()).collect(),
        )
        .unwrap();
        add.cluster.check().expect("check cluste nodes fail");
        let _: () = add.add_node().expect("add node fail");
        while !add.cluster.consistency() {
            eprintln!("wait consistent fail");
            thread::sleep(time::Duration::from_secs(1));
        }
        add.set_slave();
    }
    if let Some(sub_m) = matches.subcommand_matches("delete") {
        let newnodes: Vec<&str> = sub_m
            .values_of("node")
            .expect("get node to deleted fail")
            .collect();
        let new_node = Node::new(newnodes[0].as_bytes()).expect("new node fail");
        let nodes = new_node.nodes();
        for n in &nodes {
            println!("nodes {:?} ", n);
        }
        let cluster = Cluster::new(nodes);

        for node in newnodes {
            let del_node = cluster.node(node).expect("get node from cluster fail");

            println!("delete node {:?}", del_node);
            cluster.delete_node(del_node);
        }
    }
    if let Some(sub_m) = matches.subcommand_matches("migrate") {
        let src = sub_m.value_of("src").expect("src node  nil");
        let dst = sub_m.value_of("dst").expect("dst node nil");
        let count = clap::value_t!(sub_m.value_of("count"), usize).expect("slot count nil");
        let mut src_node = Node::new(src.as_bytes()).unwrap();
        let mut dst_node = Node::new(dst.as_bytes()).unwrap();
        src_node.connect();
        dst_node.connect();
        let mut slots = src_node.slots();
        let mut i = 0;
        println!("slots to migrate {}", slots.len());
        loop {
            if let Some(slot) = slots.pop() {
                i += 1;
                cluster::migrate_slot(&src_node, &dst_node, slot);
            } else {
                return;
            }
            if i >= count {
                return;
            }
        }
    }
}

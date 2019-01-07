#![deny(warnings)]
#![allow(clippy::let_unit_value)]
#[macro_use]
extern crate clap;
extern crate redis;

mod add;
mod cluster;
mod create;
mod util;

use add::Add;
use clap::App;
use cluster::{Cluster, Node};
use create::Create;
use std::{thread, time};

pub fn run() {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();

    if let Some(sub_m) = matches.subcommand_matches("create") {
        let slave_count = clap::value_t!(sub_m.value_of("replicate"), usize).unwrap();
        let mut master_count = clap::value_t!(sub_m.value_of("master"), usize).unwrap();
        let node: Vec<&str> = sub_m.values_of("node").unwrap().collect();
        let mut create = Create::new(node, master_count, slave_count).unwrap();
        create.cluster.check().expect("check node err");
        create.init_slots();
        create.add_slots();
        create.set_config_epoch();
        create.join_cluster();
        println!("wait consistent...");
        while !create.consistent() {
            thread::sleep(time::Duration::from_secs(1));
        }
        create.set_slave().expect("set slave err");
        return;
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
        return;
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
        return;
    }

    if let Some(sub_m) = matches.subcommand_matches("migrate") {
        let arg = (
            sub_m.value_of("node"),
            sub_m.value_of("dst"),
            clap::value_t!(sub_m.value_of("count"), usize),
        );
        let slot = clap::value_t!(sub_m.value_of("slot"), usize);
        let migrate = |src: &Node, dst: &Node, count: &[usize]| {
            for slot in count {
                cluster::migrate_slot(&src, dst, *slot)
            }
        };
        if let Ok(slot) = slot {
            if let (Some(src), Some(dst), _) = arg {
                let mut src_node = Node::new(src.as_bytes()).unwrap();
                let mut dst_node = Node::new(dst.as_bytes()).unwrap();
                src_node.connect();
                dst_node.connect();
                return migrate(&mut src_node, &dst_node, &[slot]);
            } else {
                println!("{}", matches.usage());
                return;
            }
        }
        match arg {
            (Some(src), Some(dst), Ok(count)) => {
                let mut src_node = Node::new(src.as_bytes()).unwrap();
                let mut dst_node = Node::new(dst.as_bytes()).unwrap();
                src_node.connect();
                dst_node.connect();
                let slots = src_node.slots();
                migrate(&mut src_node, &dst_node, &slots[..count])
            }
            (Some(src), _, Ok(count)) => {
                let mut src_node = Node::new(src.as_bytes()).unwrap();
                src_node.connect();
                let src_name = src_node.name.clone();
                let masters: Vec<Node> = src_node
                    .nodes()
                    .into_iter()
                    .filter(|x| x.is_master() && x.name != src_name)
                    .collect();
                let mut dist = util::divide(count, masters.len());
                let mut idx = 0;
                let mut slots = src_node.slots();

                for master in masters.into_iter() {
                    let num = dist.pop().unwrap();
                    let migra = &slots[idx..idx + num];
                    migrate(&mut src_node, &master, migra);
                    idx += num;
                }
            }
            (Some(src), Some(dst), Err(_)) => {
                let mut src_node = Node::new(src.as_bytes()).unwrap();
                let mut dst_node = Node::new(dst.as_bytes()).unwrap();
                src_node.connect();
                dst_node.connect();
                let slots = src_node.slots();
                migrate(&mut src_node, &dst_node, &slots[..])
            }
            (None, Some(dst), Ok(count)) => {
                let mut dst_node = Node::new(dst.as_bytes()).unwrap();
                let dst_name = dst_node.name.clone();
                let mut masters: Vec<Node> = dst_node
                    .nodes()
                    .clone()
                    .into_iter()
                    .filter(|x| x.is_master() && x.name != dst_name)
                    .collect();
                let mut slots = util::divide(count, masters.len());
                for master in masters {
                    let num = slots.pop().unwrap();
                    let slot = master.slots();
                    migrate(&master, &dst_node, &slot[..num])
                }
            }
            _ => println!("err"),
        }
        return;
    }

    if let Some(sub_m) = matches.subcommand_matches("fix") {
        let addr = sub_m.value_of("node").expect("get node err");
        let mut node = Node::new(addr.as_bytes()).unwrap();
        node.connect();
        let nodes = node.nodes();
        let cluster = Cluster::new(nodes);
        cluster.fix_slots();
        cluster.fill_slots();
        return;
    }

    if let Some(sub_m) = matches.subcommand_matches("reshard") {
        let addr = sub_m.value_of("node").expect("get node err");
        let mut node = Node::new(addr.as_bytes()).unwrap();
        node.connect();
        let nodes = node.nodes();
        let cluster = Cluster::new(nodes);
        cluster.reshard();
        return;
    }

    println!("{}", matches.usage())
}

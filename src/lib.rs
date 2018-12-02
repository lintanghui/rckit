extern crate clap;
extern crate redis;

mod cluster;
mod add;
mod conn;
mod create;
use clap::{App, Arg, SubCommand};
use create::Cluster;
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
                        .multiple(true)
                        .takes_value(true)
                        .help("mster number"),
                ),
        )
        .subcommand(
            SubCommand::with_name("add")
                .about("Add  node to cluster")
                .arg(
                    Arg::with_name("cluter")
                        .required(true)
                        .short("c")
                        .help("existing node of cluster"),
                )
                .arg(
                    Arg::with_name("node")
                        .required(true)
                        .short("n")
                        .help("new node<slave,master> <master> add edto cluster")
                        .multiple(true)
                        .takes_value(true),
                ),
        )
        .get_matches();
    if let Some(sub_m) = matches.subcommand_matches("create") {
        let slave_count = clap::value_t!(sub_m.value_of("replicate"), usize).unwrap();
        let mut master_count = clap::value_t!(sub_m.value_of("master"), usize).unwrap();
        let node: Vec<&str> = sub_m.values_of("node").unwrap().collect();
        println!(
            "create cluster with replicate {} node{:?}",
            slave_count, &node
        );
        let mut cluster = Cluster::new(node, master_count, slave_count).unwrap();
        cluster.init_slots();
        cluster.add_slots();
        cluster.set_config_epoch();
        cluster.join_cluster();
    }
    if let Some(sub_m) = matches.subcommand_matches("add") {
        let cluster = sub_m
            .value_of("cluster")
            .expect("must spec existing cluster node");
        let nodes: Vec<&str> = sub_m
            .values_of("node")
            .expect("must spec at least one node be add to cluster")
            .collect();
    }
}

extern crate clap;
extern crate redis;

pub mod cluster;
mod create;
use clap::{App, Arg, SubCommand};
use create::Cluster;
pub fn run() {
    let matches = App::new("rckit")
        .subcommand(
            SubCommand::with_name("create")
                .arg(
                    Arg::with_name("node")
                        .short("n")
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
}

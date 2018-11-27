extern crate clap;

use clap::{App, SubCommand};
fn main() {
    let matches = App::new("rckie")
        .subcommand(SubCommand::with_name("create"))
        .get_matches();
    match matches.subcommand_name() {
        Some("create") => println!("create cluster"),
        _ => println!("other command"),
    }
}

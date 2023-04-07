use kd_tree::layout;
use kd_tree::node::{CompoundIdentifier, Descriptor, CompoundRecord};
use rand::prelude::*;
use rand::distributions::Alphanumeric;
use kdam::tqdm;
use kd_tree::tree;
use std::fs::File;
use std::io::{self, BufReader, BufRead};
use std::path::Path;

fn main() {

    let n = 8;
    let db_filename = "/home/josh/db/param_test/".to_string();
    dbg!(layout::NODE_PAGE_SIZE);
    dbg!(layout::RECORD_PAGE_SIZE);

    let mut tree = tree::Tree::new(db_filename.clone(), n, true);

    for _ in tqdm!(0..1e7 as usize) {
        let rec = CompoundRecord::random(n);
        tree.add_record(&rec).unwrap();
    }
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}


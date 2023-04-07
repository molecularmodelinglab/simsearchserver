use kd_tree::{tree, node, layout};
use rand::prelude::*;

fn main() {

    let n = 8;
    let node_filename = "/home/josh/db/1_bil_test/node".to_string();
    let record_filename = "/home/josh/db/1_bil_test/record".to_string();
    let mut tree = tree::Tree::from_filenames(node_filename.clone(), record_filename.clone(), n, true);

    let descriptor = node::Descriptor::random(n);
    dbg!(&descriptor);


    //let descriptor = Descriptor::from_vec(vec![0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]);
    //let nn = tree.get_nearest_neighbors(&descriptor, 10);
    tree.output_depths();

    //dbg!(&nn);

}

use kd_tree::{tree, node, layout};
use rand::prelude::*;

fn main() {

    //let db_filename = "/home/josh/db/1_bil_test/".to_string();
    //let node_filename = "/home/josh/db/100mil_layout::DESCRIPTOR_LENGTHk_page/node".to_string();
    //let node_filename = "/home/josh/db/100mil_test_fixed_strings/node".to_string();
    let node_filename = "/home/josh/db/1_bil_test/node".to_string();
    //let record_filename = "/home/josh/db/100mil_layout::DESCRIPTOR_LENGTHk_page/record".to_string();
    //let record_filename = "/home/josh/db/100mil_test_fixed_strings/record".to_string();
    let record_filename = "/home/josh/db/1_bil_test/record".to_string();
    //let mut tree = Arc::new(Mutex::new(tree::Tree::from_filenames(node_filename.clone(), record_filename.clone())));
    let mut tree = tree::Tree::from_filenames(node_filename.clone(), record_filename.clone(), true);
    //pretty_env_logger::init();


    let random_arr: [f32; layout::DESCRIPTOR_LENGTH] = rand::random();
    let descriptor = node::Descriptor { data: random_arr };
    dbg!(&descriptor);


    //let descriptor = Descriptor::from_vec(vec![0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]);
    //let nn = tree.get_nearest_neighbors(&descriptor, 10);
    tree.output_depths();

    //dbg!(&nn);

}

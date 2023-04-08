use kd_tree::{tree, node, layout};
use rand::prelude::*;

fn main() {
    param_sweep();

}

fn single_query() {
    let n = 8;
    let directory = "/home/josh/db/1_bil_test/".to_string();

    let mut tree = tree::Tree::read_from_directory(directory.clone());

    let descriptor = node::Descriptor::random(n);
    dbg!(&descriptor);


    //let descriptor = Descriptor::from_vec(vec![0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]);
    //let nn = tree.get_nearest_neighbors(&descriptor, 10);
    tree.output_depths();

    //dbg!(&nn);

}

fn param_sweep() {


    use glob::glob;
    use std::time::{Duration, Instant};
    use log::info;
    env_logger::init();

    for dirname in glob("/home/josh/db/benchmark*").unwrap() {

        let directory = dirname.unwrap().into_os_string().into_string().unwrap();
        dbg!(&directory);

        let mut tree = tree::Tree::read_from_directory(directory.clone());

        for _ in 0..10 {
            for nn in [1,10,100,1000].into_iter() {

                let descriptor = node::Descriptor::random(tree.config.desc_length);

                let start = Instant::now();
 
                tree.get_nearest_neighbors(&descriptor, nn);

                let duration = start.elapsed();

                info!("{} {}: {}", &directory, &nn, &duration.as_secs_f64());
            }
        }


    }
}

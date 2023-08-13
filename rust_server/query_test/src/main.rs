use kd_tree::{tree, node, data};
//use rand::prelude::*;

fn main() {
    //param_sweep();
    single_query();

}

fn single_query() {
    let directory = "/pool/smallsa/trees/8dim".to_string();

    let mut tree = tree::Tree::read_from_directory(directory.clone());


    let mut descriptor = data::Descriptor::random(tree.config.desc_length);
  
    dbg!(&descriptor);


    descriptor.data = vec![-0.06, -0.03, 0.358, -0.03, 0.381, 0.049, 0.797, 0.298];


    let nn = tree.get_nearest_neighbors(&descriptor, 10);
    //tree.output_depths();

    dbg!(&nn);

}

fn param_sweep() {


    use glob::glob;
    use std::time::Instant;
    use log::info;
    env_logger::init();

    for dirname in glob("/home/josh/db/benchmark*").unwrap() {

        let directory = dirname.unwrap().into_os_string().into_string().unwrap();
        dbg!(&directory);

        let mut tree = tree::Tree::read_from_directory(directory.clone());

        for _ in 0..10 {
            for nn in [1,10,100,1000].into_iter() {

                let descriptor = data::Descriptor::random(tree.config.desc_length);

                let start = Instant::now();
 
                tree.get_nearest_neighbors(&descriptor, nn);

                let duration = start.elapsed();

                info!("{} {}: {}", &directory, &nn, &duration.as_secs_f64());
            }
        }


    }
}

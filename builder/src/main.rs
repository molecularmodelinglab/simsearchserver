use kd_tree::node::CompoundRecord;
use kdam::tqdm;
use kd_tree::tree;
use std::fs::File;
use std::io::{self,BufRead};
use std::path::Path;

fn main() {

    build_single();
    //param_sweep();

}

fn build_single() {

    let config_filename = "/home/josh/git/simsearchserver/build_config.yaml".to_string();

    let config = tree::TreeConfig::from_file(config_filename);

    /*
    let mut config = tree::TreeConfig::default(); 
    config.desc_length = 8;
    config.node_page_length = 4096;
    config.record_page_length = 65536;
    */

    dbg!(&config);

    let mut tree = tree::Tree::create_with_config(config.clone());

    for _ in tqdm!(0..1e4 as usize) {
        let rec = CompoundRecord::random(config.desc_length);
        tree.add_record(&rec).unwrap();
    }

    tree.flush();
}


fn param_sweep() {

    use std::time::Instant;

    use log::info;
    env_logger::init();


    let mut config = tree::TreeConfig::default();
    for desc_length in [8,10,12,16].into_iter() {
        for node_page_size in [2048,4096,8192].into_iter() {
            for record_page_size in [2048, 4096, 8192, 16384].into_iter() {
                for db_size in [1e5, 1e6, 1e7, 1e8].into_iter() {

                    config.desc_length = desc_length;
                    config.node_page_length = node_page_size;
                    config.record_page_length = record_page_size;

                    let directory_name = format!("/home/josh/db/benchmark_{}_{}_{}_{}", db_size, desc_length, node_page_size, record_page_size);
                    config.directory = directory_name.clone();
                    
                    dbg!(&config);

                    let mut tree = tree::Tree::force_create_with_config(config.clone());

                    let start = Instant::now();
                    for _ in tqdm!(0..db_size as usize) {
                        let rec = CompoundRecord::random(config.desc_length);
                        tree.add_record(&rec).unwrap();
                    }
                    let duration = start.elapsed();
                    info!("{}: {}", &directory_name, &duration.as_secs_f64());
                }
            }
        }
    }
}


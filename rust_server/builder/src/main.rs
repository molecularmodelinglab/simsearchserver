use kd_tree::node::{CompoundRecord,CompoundIdentifier, Descriptor};
use kdam::tqdm;
use kd_tree::tree;

fn main() {

    build_from_file();
    //build_single();
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

    //tree.uniform_layout(20, 0.0, 1.0);

    for _ in tqdm!(0..config.num_records.unwrap() as usize) {
        let rec = CompoundRecord::random(config.desc_length);
        tree.add_record(&rec).unwrap();
    }

    tree.flush();
}

fn build_from_file() {

    let config_filename = "/home/josh/git/simsearchserver/rust_server/build_config.yaml".to_string();

    let config = tree::TreeConfig::from_file(config_filename);

    let n = 16;
    use::std::fs::File;
    use std::io::prelude::*;

    let mut file = File::open("/home/josh/git/simsearchserver/embeddings/chembl_2mil_morgan_pca_16.csv").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();

    let mut records: Vec<CompoundRecord> = Vec::new();

    for (i, line) in contents.split("\n").enumerate() {
        if i == 0 {
            continue;
        }

        if line == "" {
            break;
        }

        let mut s = line.split(",");
        let identifier = CompoundIdentifier::from_string(s.next().unwrap().to_string());
        let s: Vec<_> = s.collect();

        let s: Vec<f32> = s.into_iter().map(|x| x.parse::<f32>().unwrap()).collect();
        let descriptor = Descriptor{ data: s.clone(), length: n};

        assert_eq!(s.len(), n);

        let cr = CompoundRecord {
            compound_identifier: identifier,
            descriptor,
            dataset_identifier: 1,
            length: n,
        };

        records.push(cr);
    }

    dbg!(&records.len());
    dbg!(&records[0]);

    let mut tree = tree::Tree::force_create_with_config(config.clone());

    for record in tqdm!(records.iter()) {
        //tree.add_record(&record).unwrap();
        tree.add_record(&record);
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


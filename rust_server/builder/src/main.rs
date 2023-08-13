use kd_tree::data::{CompoundIdentifier, Descriptor, CompoundRecord};
use kd_tree::database::{Database, DatabaseRecord};

use kdam::tqdm;
use kd_tree::tree;
use glob::glob;
use::std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufRead};
use std::path::Path;


// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}


use clap::Parser;
#[derive(Parser, Debug)] #[command(author, version, about, long_about = None)]
struct Args {

    //Which task to carry out
    #[arg(short, long)]
    task: String,

    //Input filename if task is build_from_file
    #[arg(short, long)]
    input_filename: Option<String>,

    //Output dirname if task is build_from_file
    #[arg(short, long)]
    output_dirname: Option<String>,

    //Dim if task is build_from_file
    #[arg(short, long)]
    dim: Option<usize>,
}

fn main() {

    build_from_smallsa_files();


    /*
    let args = Args::parse();
    dbg!(&args);

    match args.task.as_str() {
        "build_from_file" => build_from_file(args),
        "build_single" => build_single(),
        "param_sweep" => param_sweep(),
        _ => panic!("Unknown task: {}", args.task),
    }
    */


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

fn build_from_smallsa_files() {

    let mut config = tree::TreeConfig::default();
    config.desc_length = 16;
    config.record_page_length = 16384;
    config.directory = "/pool/smallsa/trees/16dim_16384/".to_string();
    let mut tree = tree::Tree::force_create_with_config(config.clone());

    let mut filenames: Vec<String> = glob("/pool/smallsa/16dim/*clean").expect("Glob failed").map(|x| x.unwrap().into_os_string().into_string().unwrap()).collect();

    filenames.reverse();

    for filename in filenames.iter(){

        let clean_filename = filename.clone();
        dbg!(&clean_filename);

        let stem = clean_filename.split("/").last().unwrap().split("_").next().unwrap().clone();
        dbg!(stem);
        let lines = read_lines(&clean_filename).unwrap();

        let mut counter = -1;
        for line in lines {
            if let Ok(good_line) = line {
                if counter == -1 {
                    counter += 1;
                    continue;
                }

                //println!("{}", good_line);
                let mut s = good_line.split(",");

                let fields: Vec<&str> = s.collect();

                match fields.len() {
                    7 => {},
                    _ => {
                        println!("Skipping line due to bad fields: {}", good_line);
                        continue;
                    }
                }
                let id_val = fields[0];
                let latent_string = fields[6];
                let latent_string = latent_string.replace('"', "");
                let latent_string = latent_string.replace("\\", "");
                let latent_string = latent_string.replace("[", "");
                let latent_string = latent_string.replace("]", "");

                let split: Vec<&str> = latent_string.split_whitespace().collect();
                let desc = split.iter().filter_map(|x| x.parse::<f32>().ok()).collect::<Vec<f32>>();
                match desc.len() == config.desc_length {
                    true => {},
                    false => {
                        dbg!(desc);
                        println!("Skipping line due to bad descriptor: {}", good_line);
                        continue;
                    }
                }
                let descriptor = Descriptor{ data: desc.clone(), length: config.desc_length};
                let id_string = format!("{}{}", stem, id_val);
                //dbg!(&id_string);
                let identifier = CompoundIdentifier::from_string(id_string);
                //dbg!(desc);
                //dbg!(&identifier);

                let record = CompoundRecord{ 
                    dataset_identifier: '0' as u8,
                    compound_identifier: identifier, 
                    descriptor,
                    length: config.desc_length,};

                tree.add_record(&record).unwrap();

                counter += 1;
            }
        }

    }
    tree.flush();




}

fn build_from_file(args: Args) {

    let mut config = tree::TreeConfig::from_file(config_filename);

    config.desc_length = 8;
    config.directory = "/pool/merge_test".to_string();

    let database_filename = config.directory.clone() + "/db.db";

    let mut database = Database::new(&database_filename);



    let n = 8;
    use::std::fs::File;
    use std::io::prelude::*;

    let mut file = File::open("/home/josh/git/simsearchserver/rust_server/builder/test_full_data.csv").unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();

    //let mut records: Vec<CompoundRecord> = Vec::new();

    for (i, line) in tqdm!(contents.split("\n").enumerate()) {
        if i == 0 {
            continue;
        }

        if line == "" {
            break;
        }

        let mut s = line.split(",");
        
        let smiles = s.next().unwrap().to_string();

        let identifier_string = s.next().unwrap().to_string();
        let identifier = CompoundIdentifier::from_string(identifier_string);
        let s: Vec<_> = s.collect();

        let s: Vec<f32> = s.into_iter().map(|x| x.parse::<f32>().unwrap()).collect();
        let descriptor = Descriptor{ data: s.clone(), length: n};

        let dim_matches = s.len() == n;
        match dim_matches {
            true => {},
            false => {
                panic!("Provided dimension ({}) does not match descriptor length from input file ({})", &n, &s.len());
            }
        }

        let cr = CompoundRecord::new(0, smiles, identifier, descriptor, n);

        let idx = database.add_compound_record(&cr);

        let idx = match idx {
            Ok(idx) => idx,
            Err(e) => {
                dbg!(e);
                continue;
            }
        };

        tree.add_record(&cr);
    }
    tree.flush();

    //dbg!(&records.len());
    //dbg!(&records[0]);


    //for record in tqdm!(records.iter()) {
        //tree.add_record(&record).unwrap();
    //}


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


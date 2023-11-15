use kd_tree::data::{CompoundIdentifier, Descriptor, CompoundRecord};
use kd_tree::database::{Database, DatabaseRecord};

use kdam::{tqdm, BarExt};
use kd_tree::tree;
use glob::glob;
use std::io::prelude::*;
use std::io::{self, BufRead};
use std::path::Path;
use std::fs::{File, OpenOptions};
use std::time::Instant;

use rand::thread_rng;
use rand::seq::SliceRandom;



// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

use clap::{Args, Parser, Subcommand};

/// Here's my app!
#[derive(Debug, Parser, Clone)]
#[clap(name = "my-app", version)]
pub struct App {
    #[clap(flatten)]
    global_opts: GlobalOpts,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand, Clone)]
enum Command {

    /// Help message for read.
    TestRandom(TestRandomArgs),    /// Help message for write.
    BuildFromFiles(BuildFromFileArgs),
    // ...other commands (can #[clap(flatten)] other enum variants here)
}

#[derive(Debug, Args, Clone)]
struct TestRandomArgs {

    ///Filenames of source data
    #[arg(long, num_args(1..))]
    filenames: Vec<String>,

    ///Config filename
    #[clap(long)]
    config_filename: String,

    ///Cache size for build, in GB
    #[clap(long, default_value_t = 1.0)]
    cache_size: f32,

}

#[derive(Debug, Args, Clone)]
struct BuildFromFileArgs {

    ///Filenames of source data
    #[arg(long, num_args(1..))]
    filenames: Vec<String>,

    ///Config filename
    #[clap(long)]
    config_filename: String,

    ///Cache size for build, in GB
    #[clap(long, default_value_t = 1.0)]
    cache_size: f32,

}

#[derive(Debug, Args, Clone)]
struct GlobalOpts {
    /// Color
    #[clap(long, global = true)]
    color: bool,

    //... other global options
}

/*
use clap::Parser;
#[derive(Parser, Debug)] 
#[command(author, version, about, long_about = None)]
struct Args {

    ///Which task to carry out
    #[arg(short, long)]
    task: String,

    ///Input filename if task is build_from_file
    #[arg(short, long)]
    input_filename: Option<String>,

    ///Output dirname if task is build_from_file
    #[arg(short, long)]
    output_dirname: Option<String>,

    ///Dim if task is build_from_file
    #[arg(short, long)]
    dim: Option<usize>,

    ///Config if task is build_from_file
    #[arg(short, long)]
    config_filename: Option<String>,

    ///Memory limit for cache, in GB (default 50GB)
    #[arg(long)]
    cache_limit: Option<f32>,

    ///Filenames for task build_from_files
    #[arg(long, num_args(0..))]
    build_filenames: Vec<String>,


}
*/

fn main() {

    let args = App::parse();
    dbg!(&args);

    dbg!(&args.command);
    match &args.command {
        Command::TestRandom(_) => {dbg!("TEST RANDOM");},
        Command::BuildFromFiles(bargs) => {build_from_files(&bargs);},
    }
}

/*
fn build_random(args: GlobalOpts) {

    let config_filename = args.config_filename.unwrap();

    let cache_limit = args.cache_limit.unwrap_or(50.0);

    dbg!(&config_filename);

    let mut config = tree::TreeConfig::from_file(config_filename);

    dbg!(&config);

    let mut tree = tree::Tree::create_with_config(config.clone());

    let mut start = Instant::now();
    for i in tqdm!(0..config.num_records.unwrap() as usize) {
        let rec = CompoundRecord::random(config.desc_length);
        let add_result = tree.add_record(&rec);
        match add_result {
            Ok(_) => {},
            Err(e) => {
                dbg!(e);
                panic!("Error adding record");
            },
        }
    


        if i % 100000 == 0 {
            let elapsed = start.elapsed();
            println!("\nTIME REPORT: {} {}\n", i, elapsed.as_secs_f64());
            dbg!(tree.record_handler.get_cache_size_gb());
        }
    }

    tree.flush();

    println!("Tree construction complete");
}
*/

fn build_from_files(args: &BuildFromFileArgs) {


    let config = tree::TreeConfig::from_file(args.config_filename.clone());

    let mut tree = tree::Tree::create_with_config(config.clone());

    match args.filenames.len() {
        0 => panic!("No filenames supplied"),
        _ => {},
    }

    let log_file_path = config.directory + "/build_log.txt";

    let mut log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(log_file_path.clone()).unwrap();

    let mut success_counter: usize = 0;
    let mut error_counter: usize = 0;

    let start = Instant::now();
    for filename in tqdm!(args.filenames.iter()) {

        let clean_filename = filename.clone();
        println!("{:?}", clean_filename);

        let stem = clean_filename.split("/").last().unwrap().split("_").next().unwrap();

        let lines = read_lines(&clean_filename).expect(&format!("Could not read file: {:?}", &clean_filename));

        let mut counter = -1;
        for line in lines {

            if let Ok(mut good_line) = line {

                //ignore header
                if counter == -1 {
                    counter += 1;
                    continue;
                }

                //strip out smiles extension stuff
                if good_line.contains("|") {

                    let mut keep_string: Vec<char> = Vec::new();
                    let mut keep = true;
                    for char in good_line.chars() {
                        if char == '|' { keep = !keep; continue }
                        if keep {
                            
                            keep_string.push(char.clone());
                        }

                    }

                    good_line = keep_string.into_iter().collect();
                }


                if (success_counter % 1000000 == 0) & (success_counter != 0) {
                    let elapsed = start.elapsed().as_secs_f64();
                    let log_string = format!("Total records added: {:?} in {:?} seconds\n", success_counter, elapsed);
                    log_file.write(log_string.as_bytes());
                }

                let s = good_line.split(",");

                let fields: Vec<&str> = s.collect();

                let mut field_iter = fields.into_iter();

                let smiles = field_iter.next().unwrap();
                let id_val = field_iter.next().unwrap();

                let descriptor_fields: Vec<&str> = field_iter.collect();

                let descriptor_values = match parse_descriptor_vec(descriptor_fields) {
                    Ok(a) => a,
                    Err(s) => {
                        let error_line = format!("Error parsing descriptor vec:\n\t{}\n\t{}\n", &good_line, &s);
                        log_file.write(error_line.as_bytes());
                        error_counter += 1;
                        continue},
                };
                
                let mut descriptor = Descriptor{ data: descriptor_values.clone(), length: config.desc_length};

                descriptor.add_small_noise();

                let id_string = format!("{}{}", stem, id_val);

                let identifier = CompoundIdentifier::from_string(id_string);

                let record = CompoundRecord{ 
                    compound_identifier: identifier, 
                    smiles: smiles.to_string(),
                    descriptor,
                    length: config.desc_length,};

                match tree.add_record(&record) {
                    Ok(_) => {},
                    Err(e) => {
                        let error_line = format!("Error adding record to tree:\n\t{}\n\t{}\n", &good_line, &e);
                        log_file.write(error_line.as_bytes());
                        error_counter += 1;
                        continue
                    },
                }

                success_counter += 1;
                counter += 1;
            }
        }
    }

    tree.flush();

    println!("{} records failed to be added to tree, logged in {}", error_counter, log_file_path);
}

fn parse_descriptor_vec(v: Vec<&str>) -> Result<Vec<f32>, std::num::ParseFloatError> {

    let descriptor_values: Result<Vec<f32>,_> = v.iter()
        .map(|x| x
                  .replace("[","")
                  .replace("]","")
                  .replace(" ","")
                  .parse::<f32>()
            )
        .collect();
    return descriptor_values;


}

/*
fn build_from_file(args: GlobalOpts) {


    let mut config = tree::TreeConfig::default();

    config.desc_length = 8;
    config.directory = args.output_dirname.unwrap();

    let mut tree = tree::Tree::create_with_config(config.clone());

    let n = 8;
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

        tree.add_record(&cr);
    }
    tree.flush();

}
*/

/*
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
*/

/*
mod tests {

    use super::*;

    #[test]
    fn actual_smiles_tree() {

        let mut config = tree::TreeConfig::default();

        config.desc_length = 8;
        config.directory = "/tmp/actual_smiles_tree".to_string();

        let mut tree = tree::Tree::force_create_with_config(config.clone());

        let n = 8;
        use std::io::prelude::*;

        let mut file = File::open("/home/josh/git/simsearchserver/rust_server/builder/test_full_data.csv").unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        let mut records: Vec<CompoundRecord> = Vec::new();

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

            tree.add_record(&cr);
            records.push(cr);
        }
        tree.flush();

        let nn = tree.get_nearest_neighbors(&records[0].descriptor, 10).to_yaml();

        dbg!(&records[0]);
        println!("{}", nn);


    }





}
*/


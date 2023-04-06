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

    fn get_random_record() -> CompoundRecord {

        let random_arr: [f32; layout::DESCRIPTOR_LENGTH] = rand::random();
        //let mut rng = thread_rng();
        //let chars: String = (0..16).map(|_| rng.sample(Alphanumeric) as char).collect();

        let descriptor = Descriptor { data: random_arr };

        let identifier = CompoundIdentifier::random();

        let cr = CompoundRecord {
            dataset_identifier: 0,
            compound_identifier: identifier,
            descriptor,
        };

        return cr;
    }


    let db_filename = "/home/josh/db/1_bil_8k_node_64k_record/".to_string();
    dbg!(layout::NODE_PAGE_SIZE);
    dbg!(layout::RECORD_PAGE_SIZE);
    let mut tree = tree::Tree::new(db_filename.clone(), true);
    for _ in tqdm!(0..1e9 as usize) {
        let rec = get_random_record();
        tree.add_record(&rec).unwrap();
    }

    /*
    let db_filename = "/home/josh/db/chembl_8/".to_string();
    let input_filename = "/home/josh/db/chembl_8.csv".to_string();
    let mut tree = tree::Tree::new(db_filename.clone(), true);


    println!("Building tree at {} from input file {} ...", &db_filename, &input_filename);

    // File hosts must exist in current path before this produces output
    if let Ok(lines) = read_lines(input_filename) {
        // Consumes the iterator, returns an (Optional) String
        for (i, line_res) in tqdm!(lines.enumerate()) {
            if let Ok(line) = line_res {

                if i == 0 {
                    continue;
                }

                if line == "" {
                    break;
                }

                let mut s = line.split(",");
                let identifier = CompoundIdentifier(s.next().unwrap().to_string());
                s.next();
                let s: Vec<_> = s.collect();

                let s: Vec<f32> = s.into_iter().map(|x| x.parse::<f32>().unwrap()).collect();
                let descriptor = Descriptor::from_vec(s.clone());

                assert_eq!(s.len(), 8);

                let cr = CompoundRecord {
                    compound_identifier: identifier,
                    descriptor,
                    dataset_identifier: 1,
                };

                tree.add_record(&cr).unwrap();
            }
        }
    }
    */
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}



struct RecordDataset {

    filename: String,
    fd: File,
}



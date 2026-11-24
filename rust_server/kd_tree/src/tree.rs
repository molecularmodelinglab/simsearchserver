//! Implementation of kd-tree creation and querying
use crate::data::{Descriptor, CompoundRecord, CompoundIndex};
use crate::database::{Database, ImmutDatabase};
use crate::node::{InternalNode, PagePointer};
use crate::page::RecordPage;
use crate::layout;
use crate::io::{ImmutNodePager, FastNodePager, RecordPager, GetNode};
use crate::data::{Parser};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use std::time::Instant;
use rand::Rng;
use byteorder::{ByteOrder, LittleEndian};


use std::fs::File;
use std::fs;
use std::io::prelude::*;

use std::path::Path;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::VecDeque;

const KIND_NODE: u8 = 0;
const KIND_LEAF: u8 = 1;

#[inline]
fn encode_pointer(pointer: &PagePointer) -> (u8, usize) {
    return match pointer {
        PagePointer::Node(i) => (KIND_NODE, *i),
        PagePointer::Leaf(i) => (KIND_LEAF, *i),
    };
}

#[inline]
fn decode_pointer(kind: u8, index: usize) -> PagePointer {
    return match kind {
        KIND_LEAF => PagePointer::Leaf(index),
        _ => PagePointer::Node(index),
    };
}


pub struct ImmutTree {
    pub node_handler: ImmutNodePager,
    pub record_handler: RecordPager,
    pub database: ImmutDatabase,
    pub root: PagePointer,
    pub config: TreeConfig,
}

impl ImmutTree {

    pub fn read_from_directory(directory_name: String) -> Self {

        let config_filename = directory_name.clone() + "/config.yaml";
        let config = TreeConfig::from_file(config_filename);

        config.check_format(&directory_name);

        let node_filename = config.get_node_filename();
        let record_filename = config.get_record_filename();


        let node_handler = ImmutNodePager::from_file(&node_filename).unwrap();
        let record_handler = RecordPager::new(record_filename, config.record_page_length, config.desc_length, false, config.cache_limit).unwrap();

        let database = ImmutDatabase::open(&config.get_database_filename());

        // Internal nodes only appear once a page splits
        let root = match node_handler.len() {
            0 => PagePointer::Leaf(0),
            _ => PagePointer::Node(0),
        };

        return Self {
            node_handler,
            record_handler, 
            database,
            root,
            config,
            };
    }

    pub fn get_record_page(&mut self, index: &usize) -> RecordPage {

        return self.record_handler.get_record_page(index).unwrap();
    }

    pub fn output_depths(&mut self) {

        let mut nodes_to_check: VecDeque<(PagePointer, usize)> = VecDeque::new();

        let root_pointer = self.root.clone();

        nodes_to_check.push_back((root_pointer, 0));

        loop {

            let popped_val = nodes_to_check.pop_back();

            let curr_tup = match popped_val {
                None => {break;},
                Some(x) => {x},
            };

            let (curr_pointer, count_so_far) = curr_tup;

            match curr_pointer {

                PagePointer::Leaf(index) => {
                    println!("{}", count_so_far + 1);

                    let page = self.record_handler.get_record_page(&index).unwrap();
                    let records = page.get_records();
                    println!("RECORD PAGE {}", index);
                    for record in records {
                        println!("\tCOMPOUND: {}", record.index.to_string());
                    }
                },
                PagePointer::Node(index) => {


                    let node = self.node_handler.get_node(&index).unwrap().clone();

                    println!("{}", curr_pointer);
                        println!("\tLEFT:  {}", node.left_child_pointer);
                        println!("\tRIGHT: {}", node.right_child_pointer);

                    nodes_to_check.push_back((node.left_child_pointer, count_so_far + 1));
                    nodes_to_check.push_back((node.right_child_pointer, count_so_far + 1));
                },
            }
        }
    }

    ///Returns whether or not the exact provided descriptor is in the tree
    pub fn record_in_tree(&mut self, record: &CompoundRecord) -> Result<bool, String> {

        let mut curr_pointer: PagePointer = self.root.clone();

        loop {
            match curr_pointer {
                PagePointer::Leaf(index) => {

                    let page: RecordPage = self.record_handler.get_record_page(&index).unwrap();
                    return Ok(page.descriptor_in_page(&record.descriptor));

                },
                PagePointer::Node(index) => {

                    let node = self.node_handler.get_node(&index).unwrap().clone();

                    let axis = node.split_axis;
                    let this_value = record.descriptor.data[axis];
                    let split_value = node.split_value;

                    match this_value <= split_value {
                        true => curr_pointer = node.left_child_pointer,
                        false => curr_pointer = node.right_child_pointer,
                    }
                }
            }
        }
    }

    pub fn print_record_lengths(&self) {

        for i in 0..self.record_handler.len() {

            let page = self.record_handler.get_record_page(&i).unwrap();
            println!("{:?}", page.len());

        }
    }

    /*
    pub fn num_nodes(&mut self) -> usize {

        return self.node_handler.num_nodes();

    }
    */

    pub fn get_nearest_neighbors(&self, query_descriptor: &Descriptor, n: usize) -> NearestNeighbors {

        let top_hits = self.get_top_hits(query_descriptor, n);

        let nearest_neighbors = NearestNeighbors::from_top_hits(top_hits, &self.database);

        return nearest_neighbors;
    }

    ///Returns the `n` nearest neighbors of the provided `query_descriptor`
    ///
    ///Performance should worsen as `n` grows larger, as fewer branches of the tree can be pruned
    ///with more distant already-found points
    fn get_top_hits(&self, query_descriptor: &Descriptor, n: usize) -> TopHits {

        let mut hits = TopHits::new(n);

        // Bit patterns of non-negative floats order like the floats, so raw
        // bits serve as a heap key without a total order on f32.
        let mut frontier: BinaryHeap<Reverse<(u32, u8, usize)>> = BinaryHeap::new();

        let (kind, index) = encode_pointer(&self.root);
        frontier.push(Reverse((0u32, kind, index)));

        while let Some(Reverse((bound_bits, kind, index))) = frontier.pop() {

            if f32::from_bits(bound_bits) >= hits.get_highest_squared_dist() {
                continue;
            }

            let mut current = decode_pointer(kind, index);

            loop {

                match current {

                    PagePointer::Leaf(leaf_index) => {

                        hits.record_pages_visited += 1;

                        let page = self.record_handler.page_ref(&leaf_index).unwrap();

                        // The page's own box bounds every axis at once.
                        if page.squared_distance_to_bounds(query_descriptor)
                            >= hits.get_highest_squared_dist() {
                            hits.record_pages_pruned += 1;
                            break;
                        }

                        for offset in 0..page.len() {

                            let squared = page.squared_distance_at(offset, query_descriptor);

                            if squared >= hits.get_highest_squared_dist() {
                                continue;
                            }

                            let record = page.get_record_at(offset).unwrap();

                            hits.try_add(squared, &record, &current).unwrap();
                        }

                        break;
                    },

                    PagePointer::Node(node_index) => {

                        hits.nodes_visited += 1;

                        let node = self.node_handler.get_node(&node_index).unwrap().clone();

                        let delta = query_descriptor.data[node.split_axis] - node.split_value;

                        let (near, far) = match delta <= 0.0 {
                            true => (node.left_child_pointer, node.right_child_pointer),
                            false => (node.right_child_pointer, node.left_child_pointer),
                        };

                        let far_bound = delta * delta;

                        if far_bound < hits.get_highest_squared_dist() {
                            let (far_kind, far_index) = encode_pointer(&far);
                            frontier.push(Reverse((far_bound.to_bits(), far_kind, far_index)));
                        }

                        current = near;
                    },
                }
            }
        }

        return hits;

    }

}


#[derive(Debug, PartialEq, Clone)]
pub struct TreeRecord {
    pub index: CompoundIndex,
    pub descriptor: Descriptor,
    pub length: usize,
}

impl TreeRecord {


    pub fn default(length: usize) -> Self {

        return Self {
            index: 0,
            descriptor: Descriptor {data: vec![0.0; length], length},
            length,
        };
    }
    
    pub fn random(length: usize) -> TreeRecord {

        let descriptor = Descriptor::random(length);
        let index = rand::thread_rng().gen_range(0..10000);

        let cr = TreeRecord {
            index,
            descriptor,
            length,
        };

        return cr;
    }

    pub fn get_descriptor_size(&self) -> usize {
        let descriptor_size: usize = self.length * 4;
        return descriptor_size;
    }

    pub fn get_record_size(&self) -> usize {
        let record_size = layout::INDEX_SIZE + self.get_descriptor_size();
        return record_size;
    }

    pub fn compute_record_size(length: usize) -> usize {
        let record_size = layout::INDEX_SIZE + (length * 4);
        return record_size;
    }

    //TODO: handle trailing whitespace
    pub fn from_slice(record_slice: &[u8], length: usize) -> Result<Self, String> {


        let index = Parser::get_usize_from_array(record_slice, layout::INDEX_START, layout::INDEX_SIZE).unwrap() as u64;
        let descriptor = Parser::get_descriptor_from_array(record_slice, layout::DESCRIPTOR_START, length).unwrap();

        return Ok (Self {
            index,
            descriptor,
            length,
        })
    }


    /// Serializes the record into `buf`, which must be exactly record-sized.
    pub fn write_to(&self, buf: &mut [u8]) {

        assert!(buf.len() == self.get_record_size());

        buf[layout::INDEX_START..layout::INDEX_START + layout::INDEX_SIZE]
            .copy_from_slice(&self.index.to_be_bytes());

        for i in 0..self.length {
            let at = layout::DESCRIPTOR_START + i * 4;
            LittleEndian::write_f32(&mut buf[at..at + 4], self.descriptor.data[i]);
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {

        let mut vec: Vec<u8> = vec![0u8; self.get_record_size()];

        self.write_to(&mut vec);

        return vec;
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BuildConfig {
}



#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TreeConfig {
    pub directory: String,
    pub desc_length: usize,
    pub record_page_length: usize,
    pub num_records: Option<usize>,
    pub cache_limit: Option<f32>,
    pub format_version: Option<u32>,
}



impl TreeConfig {

    pub fn default() -> Self {
        return Self {
            directory: "/tmp/kd_tree".to_string(),
            desc_length: 8,
            record_page_length: 4096,
            num_records: None,
            cache_limit: None,
            format_version: Some(layout::FORMAT_VERSION),
        }
    }

    /// Refuses an index written in a different on-disk format.
    pub fn check_format(&self, directory: &str) {

        let found = self.format_version.unwrap_or(1);

        if found != layout::FORMAT_VERSION {
            panic!(
                "index at {} is on-disk format version {}, but this build reads version {}. \n\
                 Version 1 stored descriptors big-endian behind a 6-byte page header; version 2 \n\
                 stores them little-endian behind an 8-byte header. Rebuild the index.",
                directory, found, layout::FORMAT_VERSION,
            );
        }
    }

    pub fn from_file(filename: String) -> Self {

        let serialized = std::fs::read_to_string(filename).expect("TreeConfig file can't be found or read");

        let deserialized: Self = serde_yaml::from_str(&serialized).unwrap();        

        return deserialized;
    }

    pub fn to_file(&self, filename: String) {
        
        let serialized = serde_yaml::to_string(&self).unwrap();
        let mut file = File::create(filename).unwrap();

        file.write(serialized.as_bytes()).unwrap();
    }

    pub fn get_node_filename(&self) -> String {

        return self.directory.clone() + "/node";
    }

    pub fn get_record_filename(&self) -> String {

        return self.directory.clone() + "/record";
    }

    pub fn get_config_filename(&self) -> String {

        return self.directory.clone() + "/config.yaml";
    }

    pub fn get_database_filename(&self) -> String {

        return self.directory.clone() + "/db.db";
    }



}

/// Struct to represent the kd-tree
///
/// Can be either for reading or just querying given a directory on disk. Internal nodes and leaf
/// nodes are stored in separate files and paged separately.
///
#[derive(Debug)]
pub struct Tree {
    pub node_handler: FastNodePager,
    pub record_handler: RecordPager,
    pub database: Database,
    pub root: PagePointer,
    pub config: TreeConfig,
}

#[derive(Debug, Serialize)]
pub struct NearestNeighbors {
    pub distances: Vec<f32>,
    pub records: Vec<Option<CompoundRecord>>,
    pub record_pages_visited: usize,
    pub record_pages_pruned: usize,
    pub nodes_visited: usize,
}

impl NearestNeighbors {

    fn from_top_hits(top_hits: TopHits, database: &ImmutDatabase) -> Self {

        let mut distances: Vec<f32> = Vec::new();
        let mut records: Vec<Option<CompoundRecord>> = Vec::new();

        for i in 0..top_hits.records.len() {

                let record = top_hits.records[i].as_ref().unwrap();
                
                let database_record = database.query(&top_hits.records[i].as_ref().unwrap().index).unwrap();

                let compound_record = CompoundRecord {
                    smiles: database_record.smiles.clone(),
                    compound_identifier: database_record.identifier.clone(),
                    descriptor: record.descriptor.clone(),
                    length: record.length,
                };

                distances.push(top_hits.squared_distances[i].sqrt());
                records.push(Some(compound_record));

        }

        return Self {
            distances,
            records,
            record_pages_visited: top_hits.record_pages_visited,
            record_pages_pruned: top_hits.record_pages_pruned,
            nodes_visited: top_hits.nodes_visited,
        }
    }

    pub fn to_yaml(&self) -> String {

        let mut s = String::new();
        for i in 0..self.records.len() {

            let record = match &self.records[i] {
                None => {panic!()},
                Some(x) => x.clone(),
            };
            let identifier_string = record.compound_identifier.to_string();
            let smiles = record.smiles.to_string();
            s = s + &format!("  {:?}: \n", identifier_string);
            s = s + &format!("    smiles: {}\n", &smiles);
            s = s + &format!("    embedding: {}\n", &record.descriptor.yaml());
            s = s + &format!("    distance: {}\n", &self.distances[i]).to_string();
        }

        return s;

    }


    pub fn to_json(&self) -> String {

        let mut s = String::new();
        s += "{";
        for i in 0..self.records.len() {

            let record = match &self.records[i] {
                None => {panic!()},
                Some(x) => x.clone(),
            };

            let identifier = record.compound_identifier.to_string();
            let smiles = record.smiles.to_string();
            s = s + &format!("  \"{:?}\": {{\n", identifier);
            s = s + &format!("  \"distance\": \"{}\"", &self.distances[i]).to_string();
            s = s + &format!("  \"SMILES\": \"{}\"", smiles).to_string();
            s = s + "," + "\n";
        s += "},\n";
        }
        s += "}";

        return s;

    }


}


///struct for keeping N- top closest points
///
///handles distance sorting and truncating to N items
#[derive(Debug)]
pub struct TopHits {
    pub max_length: usize,
    pub nodes_visited: usize,
    pub record_pages_visited: usize,
    pub record_pages_pruned: usize,
    pub squared_distances: Vec<f32>,
    pub records: Vec<Option<TreeRecord>>,
    pub pointers: Vec<Option<PagePointer>>,
}

impl TopHits {

    /// Behavior is undefined if we don't visit at least `max_lengths` records
    pub fn new(max_length: usize) -> Self {

        let mut squared_distances: Vec<f32> = Vec::new();
        let mut records: Vec<Option<TreeRecord>> = Vec::new();
        let mut pointers: Vec<Option<PagePointer>> = Vec::new();
        for _ in 0..max_length {
            squared_distances.push(f32::MAX);
            records.push(None);
            pointers.push(None);
        }
        return Self {
            max_length,
            nodes_visited: 0,
            record_pages_visited: 0,
            record_pages_pruned: 0,
            squared_distances,
            records,
            pointers,
        }
    }

    ///Internal method for adding a record to the list
    ///
    ///Undefined if called without checking if we can via `try_add`
    fn _add(&mut self, distance: f32, record: &TreeRecord, page_pointer: &PagePointer) -> Result<(), String> {
        let mut insert_index: Option<usize> = None;
        for (i, item) in self.squared_distances.iter().enumerate() {
            if item > &distance {
                insert_index = Some(i);
                break;
            }
        }

        let insert_index = match insert_index {
            None => {panic!()},
            Some(x) => x,
        };

        self.squared_distances.insert(insert_index, distance);
        self.records.insert(insert_index, Some(record.clone()));
        self.pointers.insert(insert_index, Some(page_pointer.clone()));

        self.squared_distances.truncate(self.max_length);
        self.records.truncate(self.max_length);
        self.pointers.truncate(self.max_length);

        Ok(())
    }

    ///Public method to be called on every record for consideration as a neighbor
    pub fn try_add(&mut self, distance: f32, record: &TreeRecord, page_pointer: &PagePointer) -> Result<(), String> {

        let worst_best_distance = self.get_highest_squared_dist();
        if distance < worst_best_distance {
            self._add(distance, record, page_pointer)?;
        }

        Ok(())
    }


    ///# Returns
    ///
    ///the highest squared distance of the list (the Nth, where N=`max_length`)
    ///
    ///Should be constant time access, it's just looking at the back of the vector?
    pub fn get_highest_squared_dist(&self) -> f32 {

        return self.squared_distances[self.max_length - 1];
    }

    pub fn to_json(&self) -> String {

        let mut s = String::new();
        s += "{";
        for i in 0..self.records.len() {

            let record = match &self.records[i] {
                None => {panic!()},
                Some(x) => x.clone(),
            };

            let index = record.index.to_string();
            s = s + &format!("  \"{:?}\": {{\n", index);
            s = s + &format!("  \"distance\": \"{}\"", &self.squared_distances[i].sqrt()).to_string();
            s = s + "," + "\n";
        s += "},\n";
        }
        s += "}";

        return s;

    }

}

#[derive(Debug)]
pub enum Direction {
    Left,
    Right,
}

#[derive(Debug)]
pub enum NodeAction {
    CheckIgnoredBranch,
    Descend,

}

impl Tree {

    pub fn read_from_directory(directory_name: String) -> Self {

        let config_filename = directory_name.clone() + "/config.yaml";
        let config = TreeConfig::from_file(config_filename);

        config.check_format(&directory_name);
        dbg!(&config);

        let node_filename = config.get_node_filename();
        let record_filename = config.get_record_filename();

        let node_handler = FastNodePager::from_file(&node_filename).unwrap();
        let record_handler = RecordPager::new(record_filename, config.record_page_length, config.desc_length, false, config.cache_limit).unwrap();

        let database = Database::open(&config.get_database_filename());

        let root = match node_handler.len() {
            0 => PagePointer::Leaf(0),
            _ => PagePointer::Node(0),
        };

        return Self {
            node_handler,
            record_handler, 
            database,
            root,
            config,
            };
    }

    pub fn force_create_with_config(config: TreeConfig) -> Self {

        let dir_path = Path::new(&config.directory);

        let mut last_error: Option<std::io::Error> = None;

        for attempt in 0..5 {

            if !dir_path.exists() {
                last_error = None;
                break;
            }

            match fs::remove_dir_all(dir_path) {
                Ok(_) => {
                    last_error = None;
                },
                Err(e) => {
                    last_error = Some(e);
                    std::thread::sleep(std::time::Duration::from_millis(50 * (attempt + 1)));
                },
            }
        }

        match last_error {
            Some(e) => panic!("could not remove existing tree directory {}: {}", config.directory, e),
            None => {},
        }

        return Self::create_with_config(config);
    }


    pub fn create_with_config(config: TreeConfig) -> Self {

        let dir_path = Path::new(&config.directory);

        match dir_path.is_dir() {
            true => {panic!("Directory already exists: {}", config.directory)},
            false => {},
        }

        fs::create_dir_all(dir_path)
            .unwrap_or_else(|e| panic!("could not create directory for tree {}: {}", config.directory, e));

        return Self::new(config);
    }

    pub fn flush(&mut self) {

        let node_filename = self.config.get_node_filename();
        self.node_handler.to_file(&node_filename).unwrap();
        self.record_handler.flush();

        // The database buffers writes and stamps its entry count here
        self.database.flush();

    }

    fn new(config: TreeConfig) -> Self {
        let mut config = config;
        config.format_version = Some(layout::FORMAT_VERSION);

        let record_filename = config.get_record_filename();
        let config_filename = config.get_config_filename();

        let node_handler = FastNodePager::new();
        let mut record_handler = RecordPager::new(record_filename, config.record_page_length, config.desc_length, true, config.cache_limit).unwrap();

        let first_record_page = RecordPage::new(config.record_page_length, config.desc_length);
        //record_handler.write_page(&first_record_page).unwrap();
        record_handler.add_page(&first_record_page).unwrap();

        config.to_file(config_filename);

        let database_filename = config.directory.clone() + "/db.db";

        let database = Database::new(&database_filename);

        return Self {
            node_handler,
            record_handler, 
            database,
            root: PagePointer::Leaf(0),
            config,
        };
    }
  
    pub fn get_record_page(&mut self, index: &usize) -> RecordPage {

        return self.record_handler.get_record_page(index).unwrap();
    }

    pub fn output_depths(&mut self) {

        let mut nodes_to_check: VecDeque<(PagePointer, usize)> = VecDeque::new();

        let root_pointer = self.root.clone();

        nodes_to_check.push_back((root_pointer, 0));

        loop {

            let popped_val = nodes_to_check.pop_back();

            let curr_tup = match popped_val {
                None => {break;},
                Some(x) => {x},
            };

            let (curr_pointer, count_so_far) = curr_tup;

            match curr_pointer {

                PagePointer::Leaf(index) => {
                    println!("{}", count_so_far + 1);

                    let page = self.record_handler.get_record_page(&index).unwrap();
                    let records = page.get_records();
                    println!("RECORD PAGE {}", index);
                    for record in records {
                        println!("\tCOMPOUND: {}", record.index.to_string());
                    }
                },
                PagePointer::Node(index) => {


                    let node = self.node_handler.get_node(&index).unwrap().clone();

                    println!("{}", curr_pointer);
                        println!("\tLEFT:  {}", node.left_child_pointer);
                        println!("\tRIGHT: {}", node.right_child_pointer);

                    nodes_to_check.push_back((node.left_child_pointer, count_so_far + 1));
                    nodes_to_check.push_back((node.right_child_pointer, count_so_far + 1));
                },
            }
        }
    }

    ///Returns whether or not the exact provided descriptor is in the tree
    pub fn record_in_tree(&mut self, record: &CompoundRecord) -> Result<bool, String> {

        let mut curr_pointer: PagePointer = self.root.clone();

        loop {
            match curr_pointer {
                PagePointer::Leaf(index) => {

                    let page: RecordPage = self.record_handler.get_record_page(&index).unwrap();
                    return Ok(page.descriptor_in_page(&record.descriptor));

                },
                PagePointer::Node(index) => {

                    let node = self.node_handler.get_node(&index).unwrap().clone();

                    let axis = node.split_axis;
                    let this_value = record.descriptor.data[axis];
                    let split_value = node.split_value;

                    match this_value <= split_value {
                        true => curr_pointer = node.left_child_pointer,
                        false => curr_pointer = node.right_child_pointer,
                    }
                }
            }
        }
    }

    pub fn print_record_lengths(&self) {

        for i in 0..self.record_handler.len() {

            let page = self.record_handler.get_record_page(&i).unwrap();
            println!("{:?}", page.len());

        }
    }

    pub fn num_nodes(&mut self) -> usize {

        return self.node_handler.num_nodes();

    }

    pub fn print_record_page(&mut self, page: RecordPage) {

        let records = page.get_records();

        for record in records.iter() {

            dbg!(record.index);
            dbg!(self.database.query(&record.index));
        }


    }

    pub fn get_records_per_page(&mut self) -> usize {

        return (self.config.record_page_length - layout::page_data_start(self.config.desc_length)) / TreeRecord::compute_record_size(self.config.desc_length);

    }


    ///Adds the records to the tree. Descends down the tree until a leaf node is found, and appends
    ///the records to that node. If this fills the node, the node is split at its median and two
    ///half-filled leaf nodes are created. A new internal node is created to point to these two
    ///children.
    pub fn add_record(&mut self, record: &CompoundRecord) -> Result<(), String> {

        let index = self.database.add_compound_record(record)?;

        let tree_record = record.get_tree_record(&index);

        let mut curr_pointer = self.root.clone();

        let mut last_pointer = self.root.clone();

        let mut last_was_left = true;


        loop {

            match curr_pointer {
                PagePointer::Leaf(index) => {

                    let mut page: RecordPage = self.record_handler.get_record_page(&index).unwrap();

                    let res = page.add_record(&tree_record);
                    match res {
                        Ok(_) => {},
                        Err(_e) => {self.print_record_page(page); panic!();},
                    }

                    match page.is_full() {
                        true => { //println!("NEED TO SPLIT");
                            let _ = &self.split(page, &curr_pointer, &last_pointer, last_was_left);},
                        //false => { self.record_handler.write_page_at_offset(&page, &index).unwrap(); },
                        false => { self.record_handler.update_page(&page, &index).unwrap(); },
                    }

                    break;
                },
                PagePointer::Node(index) => {

                    let node = self.node_handler.get_node(&index).unwrap().clone();

                    let axis = node.split_axis;
                    let this_value = record.descriptor.data[axis];
                    let split_value = node.split_value;

                    match this_value <= split_value {

                        true => {
                            last_pointer = curr_pointer.clone();
                            last_was_left = true;

                            curr_pointer = node.left_child_pointer;
                        },
                        false => {
                            last_pointer = curr_pointer.clone();
                            last_was_left = false;

                            curr_pointer = node.right_child_pointer;

                        },
                    }
                }


            }
        }

        Ok(())
    }

    pub fn uniform_layout(&mut self, n_levels: usize, lower_bound: f32, upper_bound: f32) {
        
        let max_depth = n_levels;

        struct NodeTuple {
            pointer: PagePointer,
            bounds: HashMap<usize, (f32, f32)>,
            level: usize,
        }

        let mut to_visit: VecDeque<NodeTuple> = VecDeque::new();

        self.root = PagePointer::Node(0);

        let mut curr_depth = 0;
        let curr_pointer = self.root.clone();

        let mut bounds: HashMap<usize, (f32, f32)> = HashMap::new();

        for i in 0..self.config.desc_length {
            bounds.insert(i, (lower_bound, upper_bound));
        }

        let root_tup = NodeTuple {
            pointer: curr_pointer.clone(),
            bounds: bounds.clone(),
            level: 0,
        };

        to_visit.push_front(root_tup);

        let mut time = Instant::now();
        loop {

            let popped_val = to_visit.pop_front();

            let curr_tup = match popped_val {
                None => {
                    dbg!("TO VISIT IS EMPTY");
                    break;},
                Some(x) => {x},
            };
            let curr_pointer = curr_tup.pointer;

            let index = match curr_pointer {
                PagePointer::Leaf(_) => {
                    dbg!("LEAF REACHED?");
                    panic!();
                },
                PagePointer::Node(index) => index,
            };

            let mut curr_node = match self.node_handler.get_node(&index) {
                Ok(x) => x.clone(),
                Err(_) => InternalNode::default(),
                };

            if curr_tup.level > curr_depth {
                curr_depth = curr_tup.level;
                println!("DEPTH: {}/{}      | {}", curr_depth, max_depth, time.elapsed().as_secs());
                time = Instant::now();
            }

            match curr_tup.level >= max_depth {
                true => {//dbg!("MAX LEVEL REACHED"); //add in some leaf nodes and call it a day

                    let left_record_page = RecordPage::new(self.config.record_page_length, self.config.desc_length);
                    let right_record_page = RecordPage::new(self.config.record_page_length, self.config.desc_length);

                    let left_page_pointer = self.record_handler.add_page(&left_record_page).unwrap();
                    let right_page_pointer = self.record_handler.add_page(&right_record_page).unwrap();

                    let split_axis = (curr_tup.level) % self.config.desc_length;
                    let split_value = curr_tup.bounds[&split_axis].0 + (curr_tup.bounds[&split_axis].1 - curr_tup.bounds[&split_axis].0) / 2.0;

                    curr_node.split_axis = split_axis;
                    curr_node.split_value = split_value;

                    curr_node.left_child_pointer = left_page_pointer;
                    curr_node.right_child_pointer = right_page_pointer;

                    self.node_handler.update_node(&index, &curr_node).unwrap();

                },
                false => { //keep on splitting

                    let split_axis = (curr_tup.level) % self.config.desc_length;
                    let split_value = curr_tup.bounds[&split_axis].0 + (curr_tup.bounds[&split_axis].1 - curr_tup.bounds[&split_axis].0) / 2.0;

                    curr_node.split_axis = split_axis;
                    curr_node.split_value = split_value;

                    match self.node_handler.len() {
                        0 => {
                            self.node_handler.add_node(&curr_node).unwrap();
                        }
                        _ => {
                        }
                    }

                    let left_child_pointer = self.node_handler.add_node(&InternalNode::default()).unwrap();
                    let right_child_pointer = self.node_handler.add_node(&InternalNode::default()).unwrap();

                    curr_node.left_child_pointer = left_child_pointer.clone();
                    curr_node.right_child_pointer = right_child_pointer.clone();

                    self.node_handler.update_node(&index, &curr_node).unwrap();

                    let bounds = curr_tup.bounds;

                    let mut left_bounds = bounds.clone();
                    let bounds_at_axis = left_bounds.get(&split_axis).unwrap();
                    let new_bounds = (bounds_at_axis.0, split_value);

                    left_bounds.insert(split_axis, new_bounds);

                    let mut right_bounds = bounds.clone();
                    let bounds_at_axis = right_bounds.get(&split_axis).unwrap();
                    let new_bounds = (split_value, bounds_at_axis.1);

                    right_bounds.insert(split_axis, new_bounds);

                    let left_tup = NodeTuple {
                        pointer: left_child_pointer.clone(),
                        bounds: left_bounds,
                        level: curr_tup.level + 1,
                    };

                    let right_tup = NodeTuple {
                        pointer: right_child_pointer.clone(),
                        bounds: right_bounds,
                        level: curr_tup.level + 1,
                    };

                    to_visit.push_back(left_tup);
                    to_visit.push_back(right_tup);
                }
            }
        }
    }

    ///Internal method to take a single full RecordPage, find its median at the "next" axis, and
    ///split the records along that median. This is really the only place where new internal nodes
    ///are created.
    pub fn split(&mut self, page: RecordPage, this_pointer: &PagePointer, parent_pointer: &PagePointer, last_was_left: bool) -> Result<(), String> {


        //TODO: handle most of the values along the split axis being identical
        //randomly choose left or right?


        //determine the split axis
        let parent_node: Option<InternalNode> = match &parent_pointer {
            PagePointer::Leaf(_) => None,
            PagePointer::Node(index) => {

            match self.node_handler.get_node(index) {
                Ok(x) => Some(x.clone()),
                Err(_) => None,
                }
            }
        };


        let split_axis = match &parent_node {
            Some(x) => (x.split_axis + 1) % self.config.desc_length,
            None => 0,
        };


        //determine split value
        let n_records = page.len();

        let mut values: Vec<f32> = (0..n_records)
            .map(|offset| page.descriptor_component_at(offset, split_axis))
            .collect();

        //because f32 doesn't like being compared
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let median = match values.len() % 2 {
            0 => {
                let idx_b: usize  = values.len() / 2;
                let idx_a = idx_b - 1;

                (values[idx_a] + values[idx_b] as f32) / 2.0
            },
            _ => {
                let idx: usize  = values.len() / 2;
                values[idx]
            },
        };

        //make new left record page at current offset
        let mut left_record_page = RecordPage::new(self.config.record_page_length, self.config.desc_length);
        let mut right_record_page = RecordPage::new(self.config.record_page_length, self.config.desc_length);

        for offset in 0..n_records {

            let target = match page.descriptor_component_at(offset, split_axis) <= median {
                true => &mut left_record_page,
                false => &mut right_record_page,
            };

            target.copy_record_from(&page, offset)?;
        }

        let this_index = match this_pointer {
            PagePointer::Node(_) => panic!(),
            PagePointer::Leaf(x) => x,
        };
        self.record_handler.update_page(&left_record_page, this_index).unwrap();
        
        //right page was filled alongside the left one above
        let right_child_pointer = self.record_handler.add_page(&right_record_page).unwrap();

        //make new node
        let node = InternalNode {
            left_child_pointer: this_pointer.clone(),
            right_child_pointer,
            split_axis,
            split_value: median,
        };

        //write new node and get address
        let pointer = self.node_handler.add_node(&node).unwrap();

        match parent_node {
            Some(x) => {
                //update the parent with this pointer
                let mut updated_node = x.clone();


                if last_was_left {
                    updated_node.left_child_pointer = pointer.clone();
                }
                else {
                    updated_node.right_child_pointer = pointer.clone();
                }

                match parent_pointer {
                    PagePointer::Leaf(_) => panic!(),
                    PagePointer::Node(index) => {
                        self.node_handler.update_node(index, &updated_node).unwrap();
                    }
                }
            },
            None => {},
        }

        if self.root == *this_pointer {
            self.root = pointer;
        }


        Ok(())
    }
}

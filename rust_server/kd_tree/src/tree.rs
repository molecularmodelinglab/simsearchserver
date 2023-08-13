//! Implementation of kd-tree creation and querying
extern crate test;
use crate::data::{CompoundIdentifier, Descriptor, CompoundRecord, CompoundIndex};
use crate::database::Database;
use crate::node::{InternalNode, PagePointer};
use crate::page::RecordPage;
use crate::layout;
use crate::io::{FastNodePager,RecordPager};
use crate::data::{Parser};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use std::time::Instant;
use rand::{distributions::Alphanumeric, Rng};
use byteorder::{ByteOrder, BigEndian};


use std::fs::File;
use std::fs;
use std::io::prelude::*;

use std::path::Path;
use std::collections::VecDeque;

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


    pub fn to_vec(&self) -> Vec<u8> {

        let mut vec: Vec<u8> = Vec::with_capacity(self.get_record_size());

        let index_rep = self.index.to_be_bytes();
        vec.extend_from_slice(&index_rep);

        for i in 0..self.length {

            let mut slice = [0u8; 4];
            BigEndian::write_f32(&mut slice, self.descriptor.data[i]);
            vec.extend_from_slice(&slice);

        }

        assert!(vec.len() == self.get_record_size());
        
        return vec;
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TreeConfig {
    pub directory: String,
    pub desc_length: usize,
    pub record_page_length: usize,
    pub node_page_length: usize,
    pub num_records: Option<usize>,
}

impl TreeConfig {

    pub fn default() -> Self {
        return Self {
            directory: "/tmp/kd_tree".to_string(),
            desc_length: 8,
            record_page_length: 4096,
            node_page_length: 4096,
            num_records: None,
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
#[derive(Debug)]
pub struct Tree {
    pub node_handler: FastNodePager,
    pub record_handler: RecordPager,
    pub database: Database,
    pub root: PagePointer,
    pub config: TreeConfig,
}

#[derive(Debug)]
pub struct NearestNeighbors {
    pub distances: Vec<f32>,
    pub records: Vec<Option<CompoundRecord>>,
}

impl NearestNeighbors {

    fn from_top_hits(top_hits: TopHits, database: &mut Database) -> Self {

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

                distances.push(top_hits.distances[i]);
                records.push(Some(compound_record));

        }

        return Self {
            distances,
            records,
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
    pub distances: Vec<f32>,
    pub records: Vec<Option<TreeRecord>>,
    pub pointers: Vec<Option<PagePointer>>,
}

impl TopHits {

    ///Distances are initially set to max f32 value
    /// Behavior is undefined if we don't visit at least `max_lengths` records
    pub fn new(max_length: usize) -> Self {

        let mut distances: Vec<f32> = Vec::new();
        let mut records: Vec<Option<TreeRecord>> = Vec::new();
        let mut pointers: Vec<Option<PagePointer>> = Vec::new();
        for _ in 0..max_length {
            distances.push(f32::MAX);
            records.push(None);
            pointers.push(None);
        }
        return Self {
            max_length,
            distances,
            records,
            pointers,
        }
    }

    ///Internal method for adding a record to the list
    ///
    ///Undefined if called without checking if we can via `try_add`
    fn _add(&mut self, distance: f32, record: &TreeRecord, page_pointer: &PagePointer) -> Result<(), String> {
        //println!("ADDING");

        //find insertion point
        let mut insert_index: Option<usize> = None;
        for (i, item) in self.distances.iter().enumerate() {
            if item > &distance {
                insert_index = Some(i);
                break;
            }
        }

        let insert_index = match insert_index {
            None => {panic!()},
            Some(x) => x,
        };

        self.distances.insert(insert_index, distance);
        self.records.insert(insert_index, Some(record.clone()));
        self.pointers.insert(insert_index, Some(page_pointer.clone()));

        //trim to correct size
        self.distances.truncate(self.max_length);
        self.records.truncate(self.max_length);
        self.pointers.truncate(self.max_length);

        Ok(())
    }

    ///Public method to be called on every record for consideration as a neighbor
    pub fn try_add(&mut self, distance: f32, record: &TreeRecord, page_pointer: &PagePointer) -> Result<(), String> {

        let worst_best_distance = self.get_highest_dist();
        if distance < worst_best_distance {
            self._add(distance, record, page_pointer)?;
        }

        Ok(())
    }


    ///# Returns
    ///
    ///the highest distance of the list (e.g. the Nth highest distance where N=`max_length`
    ///
    ///Should be constant time access, it's just looking at the back of the vector?
    pub fn get_highest_dist(&self) -> f32 {

        return self.distances[self.max_length - 1];
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
            s = s + &format!("  \"distance\": \"{}\"", &self.distances[i]).to_string();
            s = s + "," + "\n";
        s += "},\n";
        }
        s += "}";

        return s;

    }

}

fn get_smiles(index: &CompoundIdentifier) -> String {

    return "not implemented".to_string();
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
        dbg!(&config);

        let node_filename = config.get_node_filename();
        let record_filename = config.get_record_filename();

        dbg!(&node_filename);
        dbg!(&record_filename);

        let node_handler = FastNodePager::from_file(&node_filename).unwrap();
        let record_handler = RecordPager::new(Path::new(&record_filename), config.record_page_length, config.desc_length, false).unwrap();

        let database = Database::open(&config.get_database_filename());

        return Self {
            node_handler,
            record_handler, 
            database,
            root: PagePointer::Node(0),
            config,
            };
    }

    pub fn force_create_with_config(config: TreeConfig) -> Self {

        fs::remove_dir_all(&config.directory);

        return Self::create_with_config(config);
    }


    pub fn create_with_config(config: TreeConfig) -> Self {

        let dir_path = Path::new(&config.directory);

        match dir_path.is_dir() {
            true => {panic!("Directory already exists: {}", config.directory)},
            false => {},
        }

        

        fs::create_dir(Path::new(&config.directory)).expect("could not create directory for tree");



        return Self::new(config);
    }

    pub fn flush(&mut self) {

        let node_filename = self.config.get_node_filename();
        self.node_handler.to_file(&node_filename).unwrap();
        self.record_handler.flush();

    }

    fn new(config: TreeConfig) -> Self {

        let record_filename = config.get_record_filename();
        let config_filename = config.get_config_filename();

        let node_handler = FastNodePager::new();
        let mut record_handler = RecordPager::new(Path::new(&record_filename), config.record_page_length, config.desc_length, true).unwrap();

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

                    //dbg!(&node);
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

    fn dist_to_axis(&self, split_axis: usize, split_value: f32, descriptor: &Descriptor) -> f32 {

        return (descriptor.data[split_axis] - split_value).abs()

    }

    pub fn print_record_lengths(&mut self) {

        for i in 0..self.record_handler.len() {

            let page = self.record_handler.get_record_page(&i).unwrap();
            println!("{:?}", page.len());

        }
    }

    pub fn num_nodes(&mut self) -> usize {

        return self.node_handler.num_nodes();

    }

    pub fn get_nearest_neighbors(&mut self, query_descriptor: &Descriptor, n: usize) -> NearestNeighbors {

        let top_hits = self.get_top_hits(query_descriptor, n);

        let nearest_neighbors = NearestNeighbors::from_top_hits(top_hits, &mut self.database);

        return nearest_neighbors;
    }

    ///Returns the `n` nearest neighbors of the provided `query_descriptor`
    ///
    ///Performance should worsen as `n` grows larger, as fewer branches of the tree can be pruned
    ///with more distant already-found points
    fn get_top_hits(&mut self, query_descriptor: &Descriptor, n: usize) -> TopHits {

        let mut hits = TopHits::new(n);

        let mut num_nodes_visited: usize = 0;
        let mut num_record_pages_visited: usize = 0;

        //direction is the one we go if we pass!!!
        let mut nodes_to_check: VecDeque<(PagePointer, NodeAction, Option<Direction>)> = VecDeque::new();

        let root_pointer = self.root.clone();

        nodes_to_check.push_front((root_pointer, NodeAction::Descend, None));


        loop {

            //dbg!(&nodes_to_check);
            let popped_val = nodes_to_check.pop_front();
            //dbg!(&popped_val);

            let curr_tup = match popped_val {
                None => {break;},
                Some(x) => {x},
            };

            let (curr_pointer, action, direction) = curr_tup;

            match action {

                NodeAction::Descend => {

                    match curr_pointer {
                        PagePointer::Leaf(index) => {

                            num_record_pages_visited += 1;

                            //let page: RecordPage = self.record_handler.get_record_page(&index).unwrap();
                            let page: RecordPage = self.record_handler.get_record_page_no_cache(&index).unwrap();

                            for record in page.get_records() {
                                let dist = query_descriptor.distance(&record.descriptor);

                                //println!("TRY ADD: {:?}", record.compound_identifier.to_string());

                                hits.try_add(dist, &record, &curr_pointer).unwrap();
                            }


                        },
                        PagePointer::Node(index) => {

                            num_nodes_visited += 1;

                            let node = self.node_handler.get_node(&index).unwrap().clone();

                            let axis = node.split_axis;
                            let this_value = &query_descriptor.data[axis];
                            let split_value = node.split_value;

                            match this_value <= &split_value {

                                true => {

                                    let descend_pointer = node.left_child_pointer;

                                    //push the current node and the direction we're going
                                    nodes_to_check.push_front((descend_pointer.clone(), NodeAction::Descend, None));

                                    //push the current node and the direction we ignored
                                    nodes_to_check.push_back((curr_pointer.clone(), NodeAction::CheckIgnoredBranch, Some(Direction::Right)));
                                },
                                false => {

                                    let descend_pointer = node.right_child_pointer;

                                    //push the current node and the direction we're going
                                    nodes_to_check.push_front((descend_pointer.clone(), NodeAction::Descend, None));

                                    //push the current node and the direction we ignored
                                    nodes_to_check.push_back((curr_pointer.clone(), NodeAction::CheckIgnoredBranch, Some(Direction::Left)));
                                },
                            }
                        },
                    }
                },

                NodeAction::CheckIgnoredBranch => {

                    match curr_pointer {
                        PagePointer::Leaf(_) => {panic!();},
                        PagePointer::Node(index) => {

                            let node = self.node_handler.get_node(&index).unwrap().clone();

                            let split_axis = node.split_axis;
                            let split_value = node.split_value;

                            //calc_distance to this axis and check it
                            let dist = self.dist_to_axis(split_axis, split_value, query_descriptor);
                            let threshold = hits.get_highest_dist();
                            //println!("DIST TO AXIS: {:?}", dist);

                            if dist < threshold { //we have to visit the supplied direction
                                let descend_pointer = match direction.unwrap() {
                                    Direction::Left => node.left_child_pointer,
                                    Direction::Right => node.right_child_pointer,
                                };
                                nodes_to_check.push_front((descend_pointer, NodeAction::Descend, None));
                            }
                        }
                    }

                },
            }
        }

        println!("NODES VISITED: {:?}", num_nodes_visited);
        println!("RECORD PAGES VISITED: {:?}", num_record_pages_visited);

        return hits;

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

                    //dbg!("ADD CHECK", &curr_pointer);
                    page.add_record(&tree_record).unwrap();

                    //dbg!("POST CHECK", &curr_pointer);
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

        //let records: Vec::<(CompoundIdentifier, f32)> = page.get_records().iter().map(|x| (x.compound_identifier.clone(), x.descriptor.data[split_axis])).collect();
        //dbg!(records);

        //determine split value
        let records = page.get_records();

        let mut values: Vec<_> = records.iter().map(|x| x.descriptor.data[split_axis]).collect();

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


        let left_record_idxs: Vec<_> = (0..records.len()).filter(|x| records[*x].descriptor.data[split_axis] <= median).collect();
        let right_record_idxs: Vec<_> = (0..records.len()).filter(|x| records[*x].descriptor.data[split_axis] > median).collect();

        assert_eq!(left_record_idxs.len() + right_record_idxs.len(), records.len());

        let mut left_records: Vec<TreeRecord> = Vec::with_capacity((records.len() / 2) + 1);
        let mut right_records: Vec<TreeRecord> = Vec::with_capacity((records.len() / 2) + 1);

        //consume records and allocate into left and right pages
        for x in records.into_iter() {
            let is_left = x.descriptor.data[split_axis] <= median; 
            match is_left {
                true => {left_records.push(x)},
                false => {right_records.push(x)},
            }
        }

        //make new left record page at current offset
        let mut left_record_page = RecordPage::new(self.config.record_page_length, self.config.desc_length);
        for record in left_records.iter() {
            left_record_page.add_record(record)?;
        }

        let this_index = match this_pointer {
            PagePointer::Node(_) => panic!(),
            PagePointer::Leaf(x) => x,
        };
        //self.record_handler.write_page_at_offset(&left_record_page, this_index).unwrap();
        self.record_handler.update_page(&left_record_page, this_index).unwrap();
        
        //make new right record page at next offset
        let mut right_record_page = RecordPage::new(self.config.record_page_length, self.config.desc_length);
        for record in right_records.iter() {
            right_record_page.add_record(record)?;
        }

        //let right_child_pointer = self.record_handler.write_page(&right_record_page).unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;
    use crate::data::{CompoundIdentifier, Descriptor};
    use kdam::tqdm;

    #[test]
    fn quick_tree_new() {

        let n: usize = 8;

        let mut config = TreeConfig::default();
        config.directory = "/tmp/qtn/".to_string();

        let mut tree = Tree::force_create_with_config(config);

        let cr = CompoundRecord::random(n);
        tree.add_record(&cr).unwrap();

        let cr = CompoundRecord::random(n);
        tree.add_record(&cr).unwrap();

        for _ in 0..2000 {

            let cr = CompoundRecord::random(n);
            tree.add_record(&cr).unwrap();
        }
    }

    #[test]
    fn quick_tree_find() {

        for n in [8,12,16] {
            
            let mut config = TreeConfig::default();
            config.desc_length = n;
            config.directory = "/tmp/aaab".to_string();

            let mut tree = Tree::force_create_with_config(config);

            let cr_to_find = CompoundRecord::random(n);();


            let cr = CompoundRecord::random(n);();
            tree.add_record(&cr).unwrap();

            let bad_record = CompoundRecord::random(n);();
            let answer = tree.record_in_tree(&bad_record).unwrap();
            assert_eq!(answer, false);

            let cr = CompoundRecord::random(n);();
            tree.add_record(&cr).unwrap();

            let bad_record = CompoundRecord::random(n);();
            let answer = tree.record_in_tree(&bad_record).unwrap();
            assert_eq!(answer, false);

            for _ in 0..2000 {

                let cr = CompoundRecord::random(n);();
                //dbg!(&cr);
                tree.add_record(&cr).unwrap();
            }

            let answer = tree.record_in_tree(&cr_to_find).unwrap();
            assert_eq!(answer, false);

            tree.add_record(&cr_to_find).unwrap();

            let answer = tree.record_in_tree(&cr_to_find).unwrap();
            assert_eq!(answer, true);

            for _ in 0..2000 {

                let cr = CompoundRecord::random(n);();
                tree.add_record(&cr).unwrap();
            }

            let answer = tree.record_in_tree(&cr_to_find).unwrap();
            assert_eq!(answer, true);

            let bad_record = CompoundRecord::random(n);();
            let answer = tree.record_in_tree(&bad_record).unwrap();
            assert_eq!(answer, false);

            tree.flush()
        }
    }

    #[test]
    fn quick_tree_nn() {

        for n in [8,12,16] {

            let mut config = TreeConfig::default();
            config.desc_length = n;
            config.directory = "test_data/aaaa".to_string();

            let mut tree = Tree::force_create_with_config(config.clone());

            let cr_to_find = CompoundRecord::random(n);

            let cr = CompoundRecord::random(n);
            tree.add_record(&cr).unwrap();

            let bad_record = CompoundRecord::random(n);
            let answer = tree.record_in_tree(&bad_record).unwrap();
            assert_eq!(answer, false);

            let cr = CompoundRecord::random(n);();
            tree.add_record(&cr).unwrap();

            let bad_record = CompoundRecord::random(n);
            let answer = tree.record_in_tree(&bad_record).unwrap();
            assert_eq!(answer, false);

            for _ in tqdm!(0..20000) {

                let cr = CompoundRecord::random(n);
                tree.add_record(&cr).unwrap();
            }

            let answer = tree.record_in_tree(&cr_to_find).unwrap();
            assert_eq!(answer, false);

            tree.add_record(&cr_to_find).unwrap();

            let answer = tree.record_in_tree(&cr_to_find).unwrap();
            assert_eq!(answer, true);

            for _ in 0..2000 {

                let cr = CompoundRecord::random(n);();
                tree.add_record(&cr).unwrap();
            }

            let answer = tree.record_in_tree(&cr_to_find).unwrap();
            assert_eq!(answer, true);

            let bad_record = CompoundRecord::random(n);
            let answer = tree.record_in_tree(&bad_record).unwrap();
            assert_eq!(answer, false);


            let _nn = tree.get_nearest_neighbors(&bad_record.descriptor, 1);
        }
    }

    #[test]
    fn build_verify_nn_accuracy() {

        let n = 8;

        use::std::fs::File;
        use std::io::prelude::*;

        let mut file = File::open("test_data/random_descriptors.txt").unwrap();
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
            let descriptor = Descriptor{data: s.clone(), length: n};

            assert_eq!(s.len(), n);

            let cr = CompoundRecord {
                compound_identifier: identifier,
                descriptor,
                smiles: "no smiles".to_string(),
                length: n,
            };

            records.push(cr);
        }

        dbg!(records.len());

        let mut config = TreeConfig::default();
        config.desc_length = n;
        config.directory = "test_data/bvnnacc/".to_string();

        let mut tree = Tree::force_create_with_config(config);

        for record in tqdm!(records.iter()) {

            tree.add_record(&record.clone()).unwrap();
        }

    }


    #[bench]
    fn benchmark_query_speed(b: &mut Bencher) {

        fn make_random_query(tree: &mut Tree) {

            let descriptor = Descriptor::random(tree.config.desc_length);

            let _nn = tree.get_nearest_neighbors(&descriptor, 20);
        }

        let mut config = TreeConfig::default();
        config.directory = "test_data/bqs".to_string();

        let mut tree = Tree::force_create_with_config(config);

        b.iter(|| make_random_query(&mut tree));




    }

    /*
    #[test]
    fn uniform_tree() {

        let mut config = TreeConfig::default();
        config.desc_length = 8;
        config.record_page_length = 4096;
        config.node_page_length = 256;

        config.directory = "test_data/qut/".to_string();

        let mut tree = Tree::force_create_with_config(config.clone());

        for _ in tqdm!(0..1e6 as i32) {
            let cr = TreeRecord::random(config.desc_length);
            tree.add_record(&cr).unwrap();
        }
        //tree.output_depths();
        println!("----------");

        dbg!(&tree.num_nodes());
        tree.print_record_lengths();
    }
    */



    #[test]
    fn query_verify_nn_accuracy(){

        let n = 8;
        use::std::fs::File;
        use std::io::prelude::*;
        let mut file = File::open("test_data/random_descriptors.txt").unwrap();
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
            let descriptor = Descriptor{data: s.clone(), length: 8};

            assert_eq!(s.len(), n);

            let cr = CompoundRecord {
                compound_identifier: identifier,
                descriptor,
                smiles: "no smiles".to_string(),
                length: n
            };

            records.push(cr);
        }

        dbg!(records.len());
        let descriptor = Descriptor {
            data: vec![
            0.8598341,
            0.6338788,
            0.68099475,
            0.8503834,
            0.58941144,
            0.84688795,
            0.61008036,
            0.88481283,
            ],
            length: n,
        };


        let correct_answer = vec![
            "5ZLD00MOT36ZK6DX",
            "WYE97K70U18Y9SA8",
            "ZID354YZTEAEWOZC",
            "DD8HKDRP2N4CPUO6",
            "PTFLJ2OZHFOG2DYL",
            "IPWJNLZNC0P879Q2",
            "WVLV4I96MD0LNN3N",
            "U7TSZERO313NXQ4U",
            "MBGRRIRL213LQFUO",
            "MJ255VPHOK7HW558",
            "HH7M5H7BGC3KYE6I",
            "NXSYU67FL5SUZPBZ",
            "ARDU41VA315KNZ83",
            "TVLSWP3H7GZ8HKRW",
            "B8OSVJGNI69DBHKC",
            "0G3FS1H1MAFCMAQP",
            "BKSQDVKLXK93DWN6",
            "VL6P6BBJQ9VT0CCH",
            "0P9XMJBDOZGSVBM6",
            "3G142EYTW4S1M51F",
            "4QK1E64E9U7FXDXD",
            "ERWUTVVNZOVNHO7B",
            "MWV7OONG9Q4H3V1A",
            "AJ9A47HXJ30EXTVG",
            "OOCZMI28YAZBZ0LO",
            "ID22JZRXE1XZCWM8",
            "RN7XN70ESJW1IAUF",
            "R87OPQE6O5XDR0BG",
            "7PHMBEBZ0W4GNUCU",
            "9MAHZ2P344HVHHGC",
            "Z0GHGZAHYLME8YXA",
            "AKHK9V00BIIVD1EY",
            "LJFEPE6VYX5PL9NV",
            "34Y7VBX74LKDMOEH",
            "HVVL8PGOTA4WNVF2",
            "X6LGJ1VGB2B4PIUD",
            "LS560FKNAVULNIZG",
            "JYO0U92T1J0G72I1",
            "16NZY2Z8D5FAACKT",
            "PJDZGXQ9XCK7YRCN",
            "6BMGSLFBFFUJZ2BE",
            "GFUT7B5EO34ZV7L5",
            "RYF3P46R6ZI0I9LK",
            "LAXTER0MXHK4IDV4",
            "XAW840BTPR7UG7TR",
            "JUA6LNX66CC66AZ7",
            "OL7RKTO4PIT1XPER",
            "PO4QNRJU4K5N19IP",
            "D4ROAW3YEDV1JNEO",
            "QZILPO1CS5JLY4VZ",
        ];

        let correct_answer: Vec<_> = correct_answer.into_iter().map(|x| CompoundIdentifier::from_string(x.to_string())).collect();

        let mut config = TreeConfig::default();
        config.desc_length = 8;
        config.directory = "test_data/qvnnacc/".to_string();

        let mut tree = Tree::force_create_with_config(config);

        let nn = tree.get_nearest_neighbors(&descriptor, 50);
        dbg!(&nn);
        dbg!(&nn.distances);
        let identifiers: Vec<_> = nn.records.into_iter().map(|x| x.clone().unwrap().compound_identifier.clone()).collect();
        dbg!(&identifiers);

        assert_eq!(identifiers, correct_answer);
    }

    #[test]
    fn slow_fuzzed_verify_nn_accuracy(){

        let n = 8;

        use::std::fs::File;
        use std::io::prelude::*;

        let mut file = File::open("test_data/random_descriptors.txt").unwrap();
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
            let descriptor = Descriptor{ data: s.clone(), length: 8};

            assert_eq!(s.len(), n);

            let cr = CompoundRecord {
                compound_identifier: identifier,
                descriptor,
                smiles: "no smiles".to_string(),
                length: n,
            };

            records.push(cr);
        }

        let descriptor = Descriptor {
            data: vec![
            0.8598341,
            0.6338788,
            0.68099475,
            0.8503834,
            0.58941144,
            0.84688795,
            0.61008036,
            0.88481283,
            ],
            length: n,

        };

        let correct_answer = vec![
            "5ZLD00MOT36ZK6DX",
            "WYE97K70U18Y9SA8",
            "ZID354YZTEAEWOZC",
            "DD8HKDRP2N4CPUO6",
            "PTFLJ2OZHFOG2DYL",
            "IPWJNLZNC0P879Q2",
            "WVLV4I96MD0LNN3N",
            "U7TSZERO313NXQ4U",
            "MBGRRIRL213LQFUO",
            "MJ255VPHOK7HW558",
            "HH7M5H7BGC3KYE6I",
            "NXSYU67FL5SUZPBZ",
            "ARDU41VA315KNZ83",
            "TVLSWP3H7GZ8HKRW",
            "B8OSVJGNI69DBHKC",
            "0G3FS1H1MAFCMAQP",
            "BKSQDVKLXK93DWN6",
            "VL6P6BBJQ9VT0CCH",
            "0P9XMJBDOZGSVBM6",
            "3G142EYTW4S1M51F",
            "4QK1E64E9U7FXDXD",
            "ERWUTVVNZOVNHO7B",
            "MWV7OONG9Q4H3V1A",
            "AJ9A47HXJ30EXTVG",
            "OOCZMI28YAZBZ0LO",
            "ID22JZRXE1XZCWM8",
            "RN7XN70ESJW1IAUF",
            "R87OPQE6O5XDR0BG",
            "7PHMBEBZ0W4GNUCU",
            "9MAHZ2P344HVHHGC",
            "Z0GHGZAHYLME8YXA",
            "AKHK9V00BIIVD1EY",
            "LJFEPE6VYX5PL9NV",
            "34Y7VBX74LKDMOEH",
            "HVVL8PGOTA4WNVF2",
            "X6LGJ1VGB2B4PIUD",
            "LS560FKNAVULNIZG",
            "JYO0U92T1J0G72I1",
            "16NZY2Z8D5FAACKT",
            "PJDZGXQ9XCK7YRCN",
            "6BMGSLFBFFUJZ2BE",
            "GFUT7B5EO34ZV7L5",
            "RYF3P46R6ZI0I9LK",
            "LAXTER0MXHK4IDV4",
            "XAW840BTPR7UG7TR",
            "JUA6LNX66CC66AZ7",
            "OL7RKTO4PIT1XPER",
            "PO4QNRJU4K5N19IP",
            "D4ROAW3YEDV1JNEO",
            "QZILPO1CS5JLY4VZ",
        ];

        let correct_answer: Vec<_> = correct_answer.into_iter().map(|x| CompoundIdentifier::from_string(x.to_string())).collect();

        use rand::thread_rng;
        use rand::seq::SliceRandom;

        for _ in 0..10 {

            records.shuffle(&mut thread_rng());


            let mut config = TreeConfig::default();
            config.desc_length = 8;
            config.directory = "test_data/nn_validation".to_string();

            let mut build_tree = Tree::force_create_with_config(config);

            for record in tqdm!(records.iter()) {

                build_tree.add_record(&record.clone()).unwrap();
            }

            build_tree.flush();


            let mut query_tree = Tree::read_from_directory("test_data/nn_validation/".to_string());

            let nn = query_tree.get_nearest_neighbors(&descriptor, 50);
            let identifiers: Vec<_> = nn.records.into_iter().map(|x| x.clone().unwrap().compound_identifier.clone()).collect();

            assert_eq!(identifiers, correct_answer);
        }
    }

    #[test]
    fn many_fuzzed_verify_nn_accuracy(){

        let n = 8;

        use::std::fs::File;
        use std::io::prelude::*;
        let mut file = File::open("test_data/small_random_descriptors.txt").unwrap();
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
            let descriptor = Descriptor {data: s.clone(), length: n};

            assert_eq!(s.len(), n);

            let cr = CompoundRecord {
                compound_identifier: identifier,
                smiles: "no smiles".to_string(),
                descriptor,
                length: n,
            };

            records.push(cr);
        }

        let descriptor = Descriptor {
            data: vec![0.5613212933959323,
                 0.5027493566508184,
                 0.7390574788950892,
                 0.4562167305584901,
                 0.02413149926370306,
                 0.8082303147232089,
                 0.762055388982211,
                 0.34713323654674944],
            length: n,
        };

        let correct_answer = vec![
           "VDSYOSB36FAKWD2A", "011KIZEK6CTTETKF", "X0ZDKRU5TVHI3WWH",
           "2K9VBRZ99OA797VH", "6NJO4GB3YOCQ3GBI", "P702HIO2HPRTSELC",
           "EB4GCJAHBNDN0DBN", "GM2TR9EL0PIV08N9", "I9MP78DM70A3VQZ6",
           "V3UTSP2J7LUMNWP2", "MUV3E634V1MFUC9X", "FZ4ZE0AG1TGZ091W",
           "6Z4UPG2DNPK23QLN", "G38K1PE4A7ES6U2S", "AHVE2P24VBSGR14X",
           "BKOX77ILZZQ0JTCJ", "6IET816V235S8CS8", "72181A2R9TO1HZBB",
           "21T5RLEMEY9PUJEX", "JH7J68CPBS9H20HA", "V5EYF5CT6BOBIPCX",
           "VIS7Z7JZBDAMI0DW", "S7E4Z0YB99O2BG09", "PB6I6IK22CKPL3GU",
           "6ZEOQV45TQ6FU6Z1", "B5KCEVK99YQML5GY", "LKW13X8KBYWLE71D",
           "82UNF7I8TB06DQHP", "OJOID5GNIRX0SC2R", "LGIFGG2SCEYCN3YE",
           "WN0GPONNZFVZS4KD", "BOJ7KG4PX2I42PL0", "W0Z7PR44B20VUY6J",
           "RMNC1MWQW3LZLNG7", "LU2DGTI12CY5R2AU", "0249P6QT4S3J5CPH",
           "34V5RHOJ19CGI2CJ", "BUTU4YHRTHDE00XO", "X6TRZOF18686RIRD",
           "V89CHTIJOK42XDG0", "GO4BSC1VHO4F1IGF", "SZDPXUHVB663JAJL",
           "3FFVXGL6JV5SSYCT", "IPYLLPTEQIN2UAL2", "G4NMB8WURS7GKEH6",
           "W6B4IXXKHP0JRYU4", "X0K3MXLDSU2L0KFN", "JEHVACT7QJ8D81CI",
           "E0NO9UXRAV6PQEUQ", "LLOA71UE951B3IRE"];

        let correct_answer = correct_answer.into_iter().map(|x| CompoundIdentifier::from_string(x.to_string())).collect::<Vec<_>>();

        use rand::thread_rng;
        use rand::seq::SliceRandom;

        for _ in tqdm!(0..1000) {

            records.shuffle(&mut thread_rng());

            let mut config = TreeConfig::default();
            config.desc_length = 8;
            config.directory = "test_data/qf".to_string();

            let mut build_tree = Tree::force_create_with_config(config);

            for record in records.iter() {

                build_tree.add_record(&record.clone()).unwrap();
            }

            build_tree.flush();

            let mut query_tree = Tree::read_from_directory("test_data/qf/".to_string());

            let nn = query_tree.get_nearest_neighbors(&descriptor, 50);

            let identifiers = nn.records.into_iter().map(|x| x.unwrap().compound_identifier.clone()).collect::<Vec<_>>();

            if identifiers != correct_answer {

                query_tree.output_depths();
                for i in 0..identifiers.len() {
                    println!("{}: {} | {}", i, correct_answer[i].to_string(), identifiers[i].to_string());
                }
                panic!();
             
            }
        }
    }


    /*
    #[test]
    fn quick_fuzzed_verify_nn_accuracy(){

        let n = 8;

        use::std::fs::File;
        use std::io::prelude::*;
        let mut file = File::open("test_data/small_random_descriptors.txt").unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        let mut records: Vec<TreeRecord> = Vec::new();

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

            //let s: Vec<f32> = s.into_iter().map(|x| x.try_into().unwrap()).collect();
            let s: Vec<f32> = s.into_iter().map(|x| x.parse::<f32>().unwrap()).collect();
            let descriptor = Descriptor {data: s.clone(), length: n};

            assert_eq!(s.len(), n);

            let cr = TreeRecord {
                compound_identifier: identifier,
                descriptor,
                dataset_identifier: 1,
                length: n,
            };

            records.push(cr);
        }

        let descriptor = Descriptor {
            data: vec![0.5613212933959323,
                 0.5027493566508184,
                 0.7390574788950892,
                 0.4562167305584901,
                 0.02413149926370306,
                 0.8082303147232089,
                 0.762055388982211,
                 0.34713323654674944],
            length: n,
        };

        let correct_answer = vec![
           "VDSYOSB36FAKWD2A", "011KIZEK6CTTETKF", "X0ZDKRU5TVHI3WWH",
           "2K9VBRZ99OA797VH", "6NJO4GB3YOCQ3GBI", "P702HIO2HPRTSELC",
           "EB4GCJAHBNDN0DBN", "GM2TR9EL0PIV08N9", "I9MP78DM70A3VQZ6",
           "V3UTSP2J7LUMNWP2", "MUV3E634V1MFUC9X", "FZ4ZE0AG1TGZ091W",
           "6Z4UPG2DNPK23QLN", "G38K1PE4A7ES6U2S", "AHVE2P24VBSGR14X",
           "BKOX77ILZZQ0JTCJ", "6IET816V235S8CS8", "72181A2R9TO1HZBB",
           "21T5RLEMEY9PUJEX", "JH7J68CPBS9H20HA", "V5EYF5CT6BOBIPCX",
           "VIS7Z7JZBDAMI0DW", "S7E4Z0YB99O2BG09", "PB6I6IK22CKPL3GU",
           "6ZEOQV45TQ6FU6Z1", "B5KCEVK99YQML5GY", "LKW13X8KBYWLE71D",
           "82UNF7I8TB06DQHP", "OJOID5GNIRX0SC2R", "LGIFGG2SCEYCN3YE",
           "WN0GPONNZFVZS4KD", "BOJ7KG4PX2I42PL0", "W0Z7PR44B20VUY6J",
           "RMNC1MWQW3LZLNG7", "LU2DGTI12CY5R2AU", "0249P6QT4S3J5CPH",
           "34V5RHOJ19CGI2CJ", "BUTU4YHRTHDE00XO", "X6TRZOF18686RIRD",
           "V89CHTIJOK42XDG0", "GO4BSC1VHO4F1IGF", "SZDPXUHVB663JAJL",
           "3FFVXGL6JV5SSYCT", "IPYLLPTEQIN2UAL2", "G4NMB8WURS7GKEH6",
           "W6B4IXXKHP0JRYU4", "X0K3MXLDSU2L0KFN", "JEHVACT7QJ8D81CI",
           "E0NO9UXRAV6PQEUQ", "LLOA71UE951B3IRE"];

        let correct_answer = correct_answer.into_iter().map(|x| CompoundIdentifier::from_string(x.to_string())).collect::<Vec<_>>();

        use rand::thread_rng;
        use rand::seq::SliceRandom;

        for _ in tqdm!(0..10) {

            records.shuffle(&mut thread_rng());

            let mut config = TreeConfig::default();
            config.desc_length = 8;
            config.directory = "test_data/qf".to_string();

            let mut build_tree = Tree::force_create_with_config(config.clone());

            for record in records.iter() {

                build_tree.add_record(&record.clone()).unwrap();
            }

            build_tree.flush();

            let mut query_tree = Tree::read_from_directory(config.directory.clone());


            let nn = query_tree.get_nearest_neighbors(&descriptor, 50);

            let identifiers: Vec<_> = nn.records.into_iter().map(|x| x.clone().unwrap().compound_identifier.clone()).collect();

            if identifiers != correct_answer {

                query_tree.output_depths();
                for i in 0..identifiers.len() {
                    println!("{}: {} | {}", i, correct_answer[i].to_string(), identifiers[i].to_string());
                }
                panic!();
             
            }
        }
    }
    */



    /*
    #[test]
    fn bil_test_speed(){

        //let db_filename = "/home/josh/db/1_bil_test/".to_string();
        let node_filename = "/home/josh/db/100mil_test_fixed_strings/node".to_string();
        //let node_filename = "/home/josh/tmpfs_mount_point/node".to_string();
        let record_filename = "/home/josh/db/100mil_test_fixed_strings/record".to_string();
        //let record_filename = "/home/josh/big_tmpfs/record".to_string();
        //let mut tree = Arc::new(Mutex::new(tree::Tree::from_filenames(node_filename.clone(), record_filename.clone())));
        let mut tree = Tree::from_directory(
        let mut tree = Tree::from_filenames(node_filename.clone(), 
                                            record_filename.clone(), 
                                            8, 
                                            65536,
                                            8192,
                                            false);


        let random_arr: [f32; 8] = rand::random();
        let descriptor = Descriptor { data: Vec::from(random_arr), length: 8 };
        dbg!(&descriptor);


        //let descriptor = Descriptor::from_vec(vec![0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]);
        println!("HERE");
        println!("HERE2");
        let nn = tree.get_nearest_neighbors(&descriptor, 10);

        dbg!(&nn);
     
    }
    */


    /*
    #[ignore] //depends on layout of already written tree
    #[test]
    fn slow_memtree_load() {

        let n = 8;
        let tree = Tree::from_filenames("/home/josh/db/100mil_8k_page/node".to_string(), "/home/josh/db/100mil_8k_page/record".to_string(), n, true);

    }   
    */






}



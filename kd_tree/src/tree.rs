//! Implementation of kd-tree creation and querying
extern crate test;
use crate::node::{InternalNode, CompoundRecord, CompoundIdentifier, Descriptor, ItemOffset, PageAddress};
use crate::page::{RecordPage, NodePage, PageType };
use crate::io::{NodePager, FastNodePager,RecordPager, PagePointer, CachedPager};
use std::collections::HashMap;
use crate::layout;
use ascii::{AsAsciiStr, AsciiString};
use serde::{Serialize, Deserialize};
use std::time::{Duration, Instant};

use std::fs::File;
use std::fs;
use std::io::prelude::*;

use std::path::Path;
use std::collections::VecDeque;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TreeConfig {
    pub directory: String,
    pub desc_length: usize,
    pub record_page_length: usize,
    pub node_page_length: usize,
}

impl TreeConfig {

    pub fn default() -> Self {
        return Self {
            directory: "/tmp/kd_tree".to_string(),
            desc_length: 8,
            record_page_length: 4096,
            node_page_length: 4096,
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



}

/// Struct to represent the kd-tree
///
/// Can be either for reading or just querying given a directory on disk. Internal nodes and leaf
/// nodes are stored in separate files and paged separately.
#[derive(Debug)]
pub struct Tree {
    pub node_pager: FastNodePager,
    pub record_pager: RecordPager,
    pub root: PagePointer,
    pub config: TreeConfig,
}

///struct for keeping N- top closest points
///
///handles distance sorting and truncating to N items
#[derive(Debug)]
pub struct TopHits {
    pub max_length: usize,
    pub distances: Vec<f32>,
    pub records: Vec<Option<CompoundRecord>>,
    pub pointers: Vec<Option<PagePointer>>,
}

fn get_smiles(identifier: &CompoundIdentifier) -> String {
    let smiles = "not implemented".to_string();
    smiles
}


impl TopHits {

    ///Distances are initially set to max f32 value
    /// Behavior is undefined if we don't visit at least `max_lengths` records
    pub fn new(max_length: usize) -> Self {

        let mut distances: Vec<f32> = Vec::new();
        let mut records: Vec<Option<CompoundRecord>> = Vec::new();
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
    fn _add(&mut self, distance: f32, record: &CompoundRecord, page_pointer: &PagePointer) -> Result<(), String> {
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
    pub fn try_add(&mut self, distance: f32, record: &CompoundRecord, page_pointer: &PagePointer) -> Result<(), String> {

        //println!("ATTEMPTING TO ADD: {:?}", record.compound_identifier);
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

    //pub fn to_json(&self) -> Vec<u8> {
    pub fn to_json(&self) -> String {

        let mut s = String::new();
        s += "{";
        for i in 0..self.records.len() {

            let record = match &self.records[i] {
                None => {panic!()},
                Some(x) => x.clone(),
            };
            //let identifier_str: AsciiString = AsciiString::from_ascii(record.compound_identifier.0.clone()).unwrap();
            //let identifier_str: AsciiString = AsciiString::from_ascii(record.compound_identifier.0.clone()).unwrap();
            let identifier_string = record.compound_identifier.to_string();
            s = s + &format!("  \"{:?}\": {{\n", identifier_string);
            s = s + &format!("  \"distance\": \"{}\"", &self.distances[i]).to_string();
            s = s + "," + "\n";
        s += "},\n";
        }
        s += "}";
        //return s.as_bytes().to_vec();
        return s;

    }

    pub fn to_yaml(&self) -> String {

        let mut s = String::new();
        for i in 0..self.records.len() {

            let record = match &self.records[i] {
                None => {panic!()},
                Some(x) => x.clone(),
            };
            let identifier_string = record.compound_identifier.to_string();
            let smiles = get_smiles(&record.compound_identifier);
            s = s + &format!("  - {:?}: \n", identifier_string);
        
            s = s + &format!("    - dataset_id: {}\n", &record.dataset_identifier);
            s = s + &format!("    - smiles: {}\n", &smiles);
            s = s + &format!("    - distance: {}\n", &self.distances[i]).to_string();
        }
        //return s.as_bytes().to_vec();
        return s;

    }

}

pub enum Direction {
    Left,
    Right,
}

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
        //let mut node_pager = FastNodePager::new(Path::new(&node_filename), config.node_page_length, false).unwrap();
        let mut node_pager = FastNodePager::from_file(&node_filename).unwrap();
        let mut record_pager = RecordPager::new(Path::new(&record_filename), config.record_page_length, config.desc_length, false).unwrap();

        /*
        let (cached_node_pager, use_cached_nodes) : (Option<CachedPager>, bool) = match config.cache_nodes_for_query {
            
            true => {

                (Some(CachedPager::from_filename(Path::new(&node_filename), config.node_page_length).unwrap()), true)

            },
            false => (None, false),
        };
        */
              
        return Self {
            node_pager,
            record_pager, 
            root: PagePointer {
                page_type: PageType::Node,
                page_address: PageAddress(0),
                node_offset: ItemOffset(0),
            },
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
        self.node_pager.to_file(&node_filename);

    }

    fn new(config: TreeConfig) -> Self {

        let node_filename = config.directory.clone() + "/" + "node";
        let record_filename = config.directory.clone() + "/" + "record";
        let config_filename = config.directory.clone() + "/" + "config.yaml";

        dbg!(&node_filename);
        //let mut node_pager = NodePager::new(Path::new(&node_filename), config.node_page_length, true).unwrap();
        //let mut node_pager = FastNodePager::new(config.node_page_length);
        let mut node_pager = FastNodePager::new();
        let mut record_pager = RecordPager::new(Path::new(&record_filename), config.record_page_length, config.desc_length, true).unwrap();

        let first_record_page = RecordPage::new(config.record_page_length, config.desc_length);
        record_pager.write_page(&first_record_page).unwrap();

        //let first_node = InternalNode::default();
        //node_pager.add_node(&first_node).unwrap();

        config.to_file(config_filename);

        return Self {
            node_pager,
            record_pager, 
            root: PagePointer {
                page_type: PageType::Leaf,
                page_address: PageAddress(0),
                node_offset: ItemOffset(0),
            },
            config,
        };
    }

    pub fn output_depths(&mut self) {

        let mut nodes_to_check: VecDeque<(PagePointer, usize)> = VecDeque::new();

        let root_pointer = self.root.clone();

        nodes_to_check.push_back((root_pointer, 0));

        loop {

            //dbg!(nodes_to_check.len());

            let popped_val = nodes_to_check.pop_back();

            let curr_tup = match popped_val {
                None => {break;},
                Some(x) => {x},
            };

            let (curr_pointer, count_so_far) = curr_tup;

            match curr_pointer.page_type {
                PageType::Leaf => {
                    println!("{}", count_so_far + 1);

                    let page = self.record_pager.get_record_page(&curr_pointer.page_address).unwrap();
                    let records = page.get_records();
                    println!("RECORD PAGE {}", curr_pointer.page_address.0);
                    for record in records {
                        println!("\tCOMPOUND: {}", record.compound_identifier.to_string());
                    }
                },
                PageType::Node => {


                    let node = self.node_pager.node_from_pointer(&curr_pointer).unwrap().clone();

                    //dbg!(&node);

                    let left_pointer = PagePointer {
                                page_type: node.left_child_type,
                                page_address: node.left_child_page_address,
                                node_offset: node.left_child_node_offset,
                    };

 
                    let right_pointer = PagePointer {
                                page_type: node.right_child_type,
                                page_address: node.right_child_page_address,
                                node_offset: node.right_child_node_offset,

                    };

                    println!("{:?},{:?} ({:?},{:?})", curr_pointer.page_address, curr_pointer.node_offset, &node.split_axis, &node.split_value);
                        println!("\tLEFT:  {:?},{:?} [{:?}]", left_pointer.page_address, left_pointer.node_offset, left_pointer.page_type);
                        println!("\tRIGHT: {:?},{:?} [{:?}]", right_pointer.page_address, right_pointer.node_offset, right_pointer.page_type);

                    nodes_to_check.push_back((left_pointer, count_so_far + 1));
                    nodes_to_check.push_back((right_pointer, count_so_far + 1));
                },
            }
        }




    }

    ///Returns whether or not the exact provided descriptor is in the tree
    pub fn record_in_tree(&mut self, record: &CompoundRecord) -> Result<bool, String> {

        let mut curr_pointer: PagePointer = self.root.clone();

        loop {
            match curr_pointer.page_type {
                PageType::Leaf => {

                    let page: RecordPage = self.record_pager.get_record_page(&curr_pointer.page_address).unwrap();
                    return Ok(page.descriptor_in_page(&record.descriptor));

                },
                PageType::Node => {

                    let node = self.node_pager.node_from_pointer(&curr_pointer).unwrap().clone();

                    let axis = node.split_axis;
                    let this_value = record.descriptor.data[axis];
                    let split_value = node.split_value;

                    match this_value <= split_value {

                        true => {

                            curr_pointer = PagePointer {
                                page_type: node.left_child_type,
                                page_address: node.left_child_page_address,
                                node_offset: node.left_child_node_offset,

                            }
                        },
                        false => {

                            curr_pointer = PagePointer {
                                page_type: node.right_child_type,
                                page_address: node.right_child_page_address,
                                node_offset: node.right_child_node_offset,
                            }
                        },
                    }
                }
            }
        }
    }

    fn dist_to_axis(&self, split_axis: usize, split_value: f32, descriptor: &Descriptor) -> f32 {

        return (descriptor.data[split_axis] - split_value).abs()

    }

    pub fn print_record_lengths(&mut self) {

        for i in 0..self.record_pager.len() {

            let page = self.record_pager.get_record_page(&PageAddress(i)).unwrap();
            println!("{:?}", page.len());

        }
    }



    pub fn num_nodes(&mut self) -> usize {

        return self.node_pager.num_nodes();


        /*
        let mut count = 0;
        for i in 0..self.node_pager.cursor.0 {
            let page = self.node_pager.get_node_page(&PageAddress(i)).unwrap();
            count += page.num_nodes();
        }


        return count;
        */
    }


    ///Returns the `n` nearest neighbors of the provided `query_descriptor`
    ///
    ///Performance should worsen as `n` grows larger, as fewer branches of the tree can be pruned
    ///with more distant already-found points
    pub fn get_nearest_neighbors(&mut self, query_descriptor: &Descriptor, n: usize) -> TopHits {

        let mut hits = TopHits::new(n);

        let mut num_nodes_visited: usize = 0;
        let mut num_record_pages_visited: usize = 0;

        //direction is the one we go if we pass!!!
        let mut nodes_to_check: VecDeque<(PagePointer, NodeAction, Option<Direction>)> = VecDeque::new();

        let root_pointer = self.root.clone();

        nodes_to_check.push_front((root_pointer, NodeAction::Descend, None));

        loop {

            let popped_val = nodes_to_check.pop_front();

            let curr_tup = match popped_val {
                None => {break;},
                Some(x) => {x},
            };

            let (curr_pointer, action, direction) = curr_tup;

            match action {

                NodeAction::Descend => {


                    match curr_pointer.page_type {
                        PageType::Leaf => {

                            num_record_pages_visited += 1;

                            let page: RecordPage = self.record_pager.get_record_page(&curr_pointer.page_address.clone()).unwrap();

                            for record in page.get_records() {
                                let dist = query_descriptor.distance(&record.descriptor);

                                hits.try_add(dist, &record, &curr_pointer).unwrap();
                            }


                        },
                        PageType::Node => {

                            num_nodes_visited += 1;

                            let node = self.node_pager.node_from_pointer(&curr_pointer).unwrap().clone();

                            let axis = node.split_axis;
                            let this_value = &query_descriptor.data[axis];
                            let split_value = node.split_value;

                            match this_value <= &split_value {

                                true => {

                                    let descend_pointer = PagePointer {
                                        page_type: node.left_child_type,
                                        page_address: node.left_child_page_address,
                                        node_offset: node.left_child_node_offset,

                                    };

                                    //push the current node and the direction we're going
                                    nodes_to_check.push_front((descend_pointer.clone(), NodeAction::Descend, None));

                                    //push the current node and the direction we ignored
                                    nodes_to_check.push_back((curr_pointer.clone(), NodeAction::CheckIgnoredBranch, Some(Direction::Right)));
                                },
                                false => {

                                    let descend_pointer = PagePointer {
                                        page_type: node.right_child_type,
                                        page_address: node.right_child_page_address,
                                        node_offset: node.right_child_node_offset,
                                    };

                                    //push the current node and the direction we're going
                                    nodes_to_check.push_front((descend_pointer.clone(), NodeAction::Descend, None));

                                    //push the current node and the direction we ignored
                                    //nodes_to_check.push_front((curr_pointer.clone(), NodeAction::CheckIgnoredBranch, Some(Direction::Left)));
                                    nodes_to_check.push_back((curr_pointer.clone(), NodeAction::CheckIgnoredBranch, Some(Direction::Left)));
                                },
                            }
                        },
                    }
                },

                NodeAction::CheckIgnoredBranch => {

                    let node = self.node_pager.node_from_pointer(&curr_pointer).unwrap().clone();

                    let split_axis = node.split_axis;
                    let split_value = node.split_value;

                    //calc_distance to this axis and check it
                                       let dist = self.dist_to_axis(split_axis, split_value, query_descriptor);
                    let threshold = hits.get_highest_dist();
                    //println!("DIST TO AXIS: {:?}", dist);

                    if dist < threshold { //we have to visit the supplied direction
                        let descend_pointer = match direction.unwrap() {
                            Direction::Left => {

                                PagePointer {
                                    page_type: node.left_child_type,
                                    page_address: node.left_child_page_address,
                                    node_offset: node.left_child_node_offset,
                                }

                            },
                            Direction::Right => {

                                PagePointer {
                                    page_type: node.right_child_type,
                                    page_address: node.right_child_page_address,
                                    node_offset: node.right_child_node_offset,
                                }

                            },

                        };
                        nodes_to_check.push_front((descend_pointer, NodeAction::Descend, None));
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

        //dbg!(&self.root);
        /*
        let mut curr_pointer: PagePointer = match &self.root {

            None => { PagePointer {
                page_type: PageType::Node,
                page_address: PageAddress(0),
                node_offset: ItemOffset(0),
                }
            },
            Some(x) => x.clone(),
        };
        */

        //dbg!(&curr_pointer);
        let mut curr_pointer = self.root.clone();

        //should only persist if there are no nodes yet
        let mut last_pointer =  PagePointer {
            page_type: PageType::Leaf,
            page_address: PageAddress(0),
            node_offset: ItemOffset(0),
        };

        let mut last_was_left = true;

        //let curr_address =  &curr_pointer.page_address.clone();

        loop {
            //println!("WE ARE AT {:?}|{:?}|{:?}", curr_pointer.page_type, curr_pointer.page_address, curr_pointer.node_offset);
            match curr_pointer.page_type {
                PageType::Leaf => {

                    let mut page: RecordPage = self.record_pager.get_record_page(&curr_pointer.page_address).unwrap();

                    page.add_record(record).unwrap();
                    //dbg!(page.get_capacity());
                    //dbg!(page.tail);
                    //println!("ADDING RECORD: {}", record.compound_identifier.to_string());


                    match page.is_full() {
                        true => { //println!("NEED TO SPLIT");
                            let _ = &self.split(page, &curr_pointer, &last_pointer, last_was_left);},
                        false => { self.record_pager.write_page_at_offset(&page, &curr_pointer.page_address).unwrap(); },
                    }

                    break;
                },
                PageType::Node => {

                    let node = self.node_pager.node_from_pointer(&curr_pointer).unwrap().clone();

                    let axis = node.split_axis;
                    let this_value = record.descriptor.data[axis];
                    let split_value = node.split_value;

                    match this_value <= split_value {

                        true => {
                            last_pointer = curr_pointer.clone();
                            last_was_left = true;

                            curr_pointer = PagePointer {
                                page_type: node.left_child_type,
                                page_address: node.left_child_page_address,
                                node_offset: node.left_child_node_offset,

                            }
                        },
                        false => {
                            last_pointer = curr_pointer.clone();
                            last_was_left = false;

                            curr_pointer = PagePointer {
                                page_type: node.right_child_type,
                                page_address: node.right_child_page_address,
                                node_offset: node.right_child_node_offset,
                            }


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
            parent_pointer: Option<PagePointer>,
            bounds: HashMap<usize, (f32, f32)>,
            level: usize,
        }

        let mut to_visit: VecDeque<NodeTuple> = VecDeque::new();

        self.root = PagePointer {
                page_type: PageType::Node,
                page_address: PageAddress(0),
                node_offset: ItemOffset(0),
            };

        let mut curr_depth = 0;
        let curr_pointer = self.root.clone();

        let mut bounds: HashMap<usize, (f32, f32)> = HashMap::new();

        for i in 0..self.config.desc_length {
            bounds.insert(i, (lower_bound, upper_bound));
        }

        let root_tup = NodeTuple {
            pointer: curr_pointer.clone(),
            parent_pointer: None,
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

            match curr_pointer.page_type {
                PageType::Leaf => {
                    dbg!("LEAF REACHED?");
                    panic!();
                },
                PageType::Node => {},
            }

            //dbg!(&curr_pointer);

            let mut curr_node = match self.node_pager.node_from_pointer(&curr_pointer) {
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

                    let left_page_address = self.record_pager.write_page(&left_record_page).unwrap();
                    let right_page_address = self.record_pager.write_page(&right_record_page).unwrap();

                    //println!("RECORD LEFT: {:?}, {:?}", &left_child_pointer.page_address, &left_child_pointer.node_offset);
                    //println!("RIGHT: {:?}, {:?}", &right_child_pointer.page_address, &right_child_pointer.node_offset);

                    let split_axis = (curr_tup.level) % self.config.desc_length;
                    let split_value = curr_tup.bounds[&split_axis].0 + (curr_tup.bounds[&split_axis].1 - curr_tup.bounds[&split_axis].0) / 2.0;
                    //dbg!(&split_axis, &split_value);

                    curr_node.split_axis = split_axis;
                    curr_node.split_value = split_value;


                    curr_node.left_child_type = PageType::Leaf;
                    curr_node.left_child_page_address = left_page_address;
                    curr_node.left_child_node_offset = ItemOffset(0);


                    curr_node.right_child_type = PageType::Leaf;
                    curr_node.right_child_page_address = right_page_address;
                    curr_node.right_child_node_offset = ItemOffset(0);

                    self.node_pager.update_node(&curr_pointer, &curr_node).unwrap();

                },
                false => { //keep on splitting

                    let split_axis = (curr_tup.level) % self.config.desc_length;
                    let split_value = curr_tup.bounds[&split_axis].0 + (curr_tup.bounds[&split_axis].1 - curr_tup.bounds[&split_axis].0) / 2.0;
                    //dbg!(&split_axis, &split_value);

                    curr_node.split_axis = split_axis;
                    curr_node.split_value = split_value;

                    match self.node_pager.len() {
                        0 => {
                            self.node_pager.add_node(&curr_node).unwrap();
                        }
                        _ => {
                        }
                    }


                    let left_child_pointer = self.node_pager.add_node(&InternalNode::default()).unwrap();
                    let right_child_pointer = self.node_pager.add_node(&InternalNode::default()).unwrap();

                    //dbg!(&left_child_pointer);
                    //dbg!(&right_child_pointer);

                    //println!("LEFT: {:?}, {:?}", &left_child_pointer.page_address, &left_child_pointer.node_offset);
                    //println!("RIGHT: {:?}, {:?}", &right_child_pointer.page_address, &right_child_pointer.node_offset);

                    curr_node.left_child_type = PageType::Node;
                    curr_node.left_child_page_address = left_child_pointer.page_address.clone();
                    curr_node.left_child_node_offset = left_child_pointer.node_offset.clone();

                    curr_node.right_child_type = PageType::Node;
                    curr_node.right_child_page_address = right_child_pointer.page_address.clone();
                    curr_node.right_child_node_offset = right_child_pointer.node_offset.clone();

                    self.node_pager.update_node(&curr_pointer, &curr_node).unwrap();

                    let bounds = curr_tup.bounds;

                    let mut left_bounds = bounds.clone();
                    let bounds_at_axis = left_bounds.get(&split_axis).unwrap();
                    let new_bounds = (bounds_at_axis.0, split_value);
                    //dbg!("LEFT", &split_axis, &bounds_at_axis, &new_bounds);
                    left_bounds.insert(split_axis, new_bounds);

                    let mut right_bounds = bounds.clone();
                    let bounds_at_axis = right_bounds.get(&split_axis).unwrap();
                    let new_bounds = (split_value, bounds_at_axis.1);
                    //dbg!("RIGHT", &split_axis, &bounds_at_axis, &new_bounds);
                    right_bounds.insert(split_axis, new_bounds);

                    let left_tup = NodeTuple {
                        pointer: left_child_pointer.clone(),
                        parent_pointer: Some(curr_pointer.clone()),
                        bounds: left_bounds,
                        level: curr_tup.level + 1,
                    };

                    let right_tup = NodeTuple {
                        pointer: right_child_pointer.clone(),
                        parent_pointer: Some(curr_pointer.clone()),
                        bounds: right_bounds,
                        level: curr_tup.level + 1,
                    };

                    to_visit.push_back(left_tup);
                    to_visit.push_back(right_tup);
                }
            }



        }
        /*

                    let node = page.get_node_at(curr_pointer.node_offset.clone()).unwrap();

                    let axis = node.split_axis;
                    let this_value = record.descriptor.data[axis];
                    let split_value = node.split_value;

                    match this_value <= split_value {

                        true => {
                            last_pointer = curr_pointer.clone();
                            last_was_left = true;

                            curr_pointer = PagePointer {
                                page_type: node.left_child_type,
                                page_address: node.left_child_page_address,
                                node_offset: node.left_child_node_offset,

                            }
                        },
                        false => {
                            last_pointer = curr_pointer.clone();
                            last_was_left = false;

                            curr_pointer = PagePointer {
                                page_type: node.right_child_type,
                                page_address: node.right_child_page_address,
                                node_offset: node.right_child_node_offset,
                            }


                        },
                    }
                }


            }
            
        }
        */

    }


    /*
    fn empty_split(&mut self, this_pointer: &PagePointer, parent_pointer: &PagePointer, last_was_left: bool, bound_map: HashMap<usize, (f32, f32)>) -> Result<(), String> {

        //determine the split axis
        let split_axis = match self.root {
            None => {0},
            Some(_) => {
                    let parent_page = self.node_pager.get_node_page(&parent_pointer.page_address).unwrap();
                    let parent_node = parent_page.get_node_at(parent_pointer.node_offset.clone()).unwrap();
                    (parent_node.split_axis + 1) % self.config.desc_length
            },
        };

        //determine split value
        let records = page.get_records();

        //dbg!(&records);

        let mut values: Vec<_> = records.iter().map(|x| x.descriptor.data[split_axis]).collect();

        //because f32 doesn't like being compared
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());

        //dbg!(&values);

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

        //let mut left_records: Vec<CompoundRecord> = Vec::new();
        //let mut right_records: Vec<CompoundRecord> = Vec::new();

        let mut left_records: Vec<CompoundRecord> = Vec::with_capacity((records.len() / 2) + 1);
        let mut right_records: Vec<CompoundRecord> = Vec::with_capacity((records.len() / 2) + 1);

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

        self.record_pager.write_page_at_offset(&left_record_page, &this_pointer.page_address).unwrap();
        
        //make new right record page at next offset
        let mut right_record_page = RecordPage::new(self.config.record_page_length, self.config.desc_length);
        for record in right_records.iter() {
            right_record_page.add_record(record)?;
        }

        let right_child_address = self.record_pager.write_page(&right_record_page).unwrap();

        //make new node
        let node = InternalNode {
            parent_page_address: PageAddress(0), //deprecated
            parent_node_offset: ItemOffset(0), //deprecated
            left_child_page_address: this_pointer.page_address.clone(),
            left_child_node_offset: this_pointer.node_offset.clone(), //not used
            left_child_type: PageType::Leaf,
            right_child_page_address: right_child_address,
            right_child_node_offset: ItemOffset(0), //not used
            right_child_type: PageType::Leaf,
            split_axis,
            split_value: median,
        };

        //write new node and get address
        let pointer = self.add_new_node(&node).unwrap();

        //update the parent with this pointer
        if self.root != None {
            let mut parent_page = self.node_pager.get_node_page(&parent_pointer.page_address).unwrap();
            let mut parent_node = parent_page.get_node_at(parent_pointer.node_offset.clone()).unwrap();


            if last_was_left {
                parent_node.left_child_page_address = pointer.page_address.clone();
                parent_node.left_child_node_offset = pointer.node_offset.clone();
                parent_node.left_child_type = PageType::Node;
            }
            else {
                parent_node.right_child_page_address = pointer.page_address.clone();
                parent_node.right_child_node_offset = pointer.node_offset.clone();
                parent_node.right_child_type = PageType::Node;
            }

            parent_page.write_node_at(parent_node, parent_pointer.node_offset.clone()).unwrap();
            self.node_pager.write_page_at_offset(&parent_page, &parent_pointer.page_address).unwrap();
        }
            //let parent_page = self.node_pager.get_node_page(&parent_pointer.page_address).unwrap();
            //let parent_node = parent_page.get_node_at(parent_pointer.node_offset.clone()).unwrap();

        //let first_pointer = PagePointer {
        //    page_type: PageType::Leaf,
        //    page_address: PageAddress(0),
        //    node_offset: ItemOffset(0), //not used for leaf
        //};

        if self.root == None {
            //println!("UPDATING ROOT");
            self.root = Some(pointer);
        }


        Ok(())
    }
    */


    ///Internal method to take a single full RecordPage, find its median at the "next" axis, and
    ///split the records along that median. This is really the only place where new internal nodes
    ///are created.
    pub fn split(&mut self, page: RecordPage, this_pointer: &PagePointer, parent_pointer: &PagePointer, last_was_left: bool) -> Result<(), String> {

        //determine the split axis
        let parent_node: Option<InternalNode> = match self.node_pager.node_from_pointer(&parent_pointer)
        {
            Ok(x) => Some(x.clone()),
            Err(_) => None,
        };


        
        let split_axis = match &parent_node {
            Some(x) => (x.split_axis + 1) % self.config.desc_length,
            None => 0,
        };


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

        let mut left_records: Vec<CompoundRecord> = Vec::with_capacity((records.len() / 2) + 1);
        let mut right_records: Vec<CompoundRecord> = Vec::with_capacity((records.len() / 2) + 1);

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

        self.record_pager.write_page_at_offset(&left_record_page, &this_pointer.page_address).unwrap();
        
        //make new right record page at next offset
        let mut right_record_page = RecordPage::new(self.config.record_page_length, self.config.desc_length);
        for record in right_records.iter() {
            right_record_page.add_record(record)?;
        }

        let right_child_address = self.record_pager.write_page(&right_record_page).unwrap();

        //make new node
        let node = InternalNode {
            left_child_page_address: this_pointer.page_address.clone(),
            left_child_node_offset: this_pointer.node_offset.clone(), //not used
            left_child_type: PageType::Leaf,
            right_child_page_address: right_child_address,
            right_child_node_offset: ItemOffset(69), //not used
            right_child_type: PageType::Leaf,
            split_axis,
            split_value: median,
        };

        //write new node and get address
        let pointer = self.node_pager.add_node(&node).unwrap();

        match parent_node {
            Some(x) => {
                //update the parent with this pointer
                let mut updated_node = x.clone();

                if last_was_left {
                    updated_node.left_child_page_address = pointer.page_address.clone();
                    updated_node.left_child_node_offset = pointer.node_offset.clone();
                    updated_node.left_child_type = PageType::Node;
                }
                else {
                    updated_node.right_child_page_address = pointer.page_address.clone();
                    updated_node.right_child_node_offset = pointer.node_offset.clone();
                    updated_node.right_child_type = PageType::Node;
                }

                self.node_pager.update_node(&parent_pointer, &updated_node).unwrap();
            },
            None => {},
        }

        if self.root == *this_pointer {
            //println!("UPDATING ROOT");
            self.root = pointer;
        }


        Ok(())
    }

    /*
    pub fn add_new_node(&mut self, node: &InternalNode) -> Result<PagePointer, String> {

        let pointer = self.node_pager.add_node(node).unwrap();
        return Ok(pointer);
    }
    */
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;
    use crate::node::{CompoundRecord, CompoundIdentifier, Descriptor};
    //use rand::distributions::Alphanumeric;
    use kdam::tqdm;
    //use rand::prelude::*;

    #[test]
    fn quick_tree_new() {

        let n: usize = 8;

        let mut config = TreeConfig::default();
        config.directory = "test_data/qtn/".to_string();


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
            config.directory = "test_data/aaab".to_string();

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
        }
    }

    #[test]
    fn quick_tree_nn() {

        for n in [8,12,16] {

            let mut config = TreeConfig::default();
            config.desc_length = n;
            config.directory = "test_data/aaaa".to_string();

            let mut tree = Tree::force_create_with_config(config.clone());
            //dbg!(&tree);


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

            //let s: Vec<f32> = s.into_iter().map(|x| x.try_into().unwrap()).collect();
            let s: Vec<f32> = s.into_iter().map(|x| x.parse::<f32>().unwrap()).collect();
            let descriptor = Descriptor{data: s.clone(), length: n};

            assert_eq!(s.len(), n);

            let cr = CompoundRecord {
                compound_identifier: identifier,
                descriptor,
                dataset_identifier: 1,
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
    #[test]
    fn uniform_tree() {

        let mut config = TreeConfig::default();
        config.desc_length = 8;
        config.record_page_length = 4096;
        config.node_page_length = 256;

        config.directory = "test_data/qut/".to_string();

        let mut tree = Tree::force_create_with_config(config.clone());

        //tree.uniform_layout(22, 0.0, 1.0);

        /*
        dbg!(&tree.node_pager.store[0]);
        dbg!(&tree.node_pager.store[1]);
        dbg!(&tree.node_pager.store[2]);
        dbg!(&tree.root);
        */

        let cr = CompoundRecord::random(config.desc_length);

        /*
        for i in 0..300 {
            let cr = CompoundRecord::random(config.desc_length);
            tree.add_record(&cr).unwrap();
        }
        */
        //let (node, page) = tree.node_pager.data_from_pointer(&tree.root.unwrap()).unwrap();

        //tree.output_depths();

        for i in tqdm!(0..1e6 as i32) {
            let cr = CompoundRecord::random(config.desc_length);
            tree.add_record(&cr).unwrap();
        }
        //tree.output_depths();
        println!("----------");

        dbg!(&tree.num_nodes());
        tree.print_record_lengths();
    }



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

            //let s: Vec<f32> = s.into_iter().map(|x| x.try_into().unwrap()).collect();
            let s: Vec<f32> = s.into_iter().map(|x| x.parse::<f32>().unwrap()).collect();
            let descriptor = Descriptor{data: s.clone(), length: 8};

            assert_eq!(s.len(), n);

            let cr = CompoundRecord {
                compound_identifier: identifier,
                descriptor,
                dataset_identifier: 1,
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
        //tree.record_pager.print_records();
        //tree.node_pager.print_nodes();
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

            //let s: Vec<f32> = s.into_iter().map(|x| x.try_into().unwrap()).collect();
            let s: Vec<f32> = s.into_iter().map(|x| x.parse::<f32>().unwrap()).collect();
            let descriptor = Descriptor{ data: s.clone(), length: 8};

            assert_eq!(s.len(), n);

            let cr = CompoundRecord {
                compound_identifier: identifier,
                descriptor,
                dataset_identifier: 1,
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

            //let s: Vec<f32> = s.into_iter().map(|x| x.try_into().unwrap()).collect();
            let s: Vec<f32> = s.into_iter().map(|x| x.parse::<f32>().unwrap()).collect();
            let descriptor = Descriptor {data: s.clone(), length: n};

            assert_eq!(s.len(), n);

            let cr = CompoundRecord {
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

        for _ in tqdm!(0..1000) {

            records.shuffle(&mut thread_rng());

            let mut config = TreeConfig::default();
            config.desc_length = 8;
            config.directory = "test_data/qf".to_string();

            let mut build_tree = Tree::force_create_with_config(config);

            for record in records.iter() {

                build_tree.add_record(&record.clone()).unwrap();
            }

            //let mut query_tree = Tree::read_from_directory("test_data/qf/".to_string());
            let mut query_tree = build_tree;

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


    #[test]
    fn quick_fuzzed_verify_nn_accuracy(){

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

            //let s: Vec<f32> = s.into_iter().map(|x| x.try_into().unwrap()).collect();
            let s: Vec<f32> = s.into_iter().map(|x| x.parse::<f32>().unwrap()).collect();
            let descriptor = Descriptor {data: s.clone(), length: n};

            assert_eq!(s.len(), n);

            let cr = CompoundRecord {
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

            let stem = config.directory.clone();
            let node_filename = stem + "node";

            //build_tree.output_depths();
            //dbg!(&build_tree.node_pager.store[0]);
            //dbg!(&build_tree.node_pager.store[1]);
            //panic!();
            build_tree.flush();

            //let mut query_tree = Tree::read_from_directory("test_data/qf/".to_string());
            let mut query_tree = Tree::read_from_directory(config.directory.clone());
            //println!("HERE3");


            /*
            for node in query_tree.node_pager.store {
                println!("{:?}" , node);
            }
            */


            //dbg!(&build_tree.node_pager.store[0]);
            //dbg!(&query_tree.node_pager.store[0]);
            //panic!();
            //query_tree.output_depths();
            let nn = query_tree.get_nearest_neighbors(&descriptor, 50);
            //println!("HERE4");

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



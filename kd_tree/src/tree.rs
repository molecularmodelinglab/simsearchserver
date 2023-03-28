//! Implementation of kd-tree creation and querying
extern crate test;
use crate::node::{InternalNode, CompoundRecord, PageAddress, Descriptor, NodeOffset};
use crate::page::{LeafPage, NodePage, PageType };
use crate::io::{Pager, PagePointer};

use std::path::Path;
use std::collections::VecDeque;

/// Struct to represent the kd-tree
///
/// Can be either for reading or just querying given a directory on disk. Internal nodes and leaf
/// nodes are stored in separate files and paged separately.
#[derive(Debug)]
pub struct Tree {
    pub node_pager: Pager,
    pub record_pager: Pager,
    pub dirname: String,
    pub free_node_pages: VecDeque<PageAddress>,
    pub root: Option<PagePointer>,

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

impl TopHits {

    ///Distances are initially set to max f32 value
    ///
    ///Behavior is undefined if we don't visit at least `max_lengths` records
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

    ///Given a target directory, either use the existing files in that directory for querying or
    ///construction of a new tree 
    pub fn new(directory_name: String, create: bool) -> Self {

        let node_filename = directory_name.clone() + "/" + "node";
        let record_filename = directory_name.clone() + "/" + "record";

        let mut node_pager = Pager::new(Path::new(&node_filename), create).unwrap();
        let mut record_pager = Pager::new(Path::new(&record_filename), create).unwrap();

        let free_node_pages = VecDeque::from([PageAddress(0)]);
              
        match create {
            true => {
         
                let first_page = LeafPage::new();

                record_pager.write_page(&first_page).unwrap();

                let first_page = NodePage::new();
                node_pager.write_page(&first_page).unwrap();
                    

                return Self {
                    node_pager,
                    record_pager, 
                    dirname: directory_name.clone(),
                    free_node_pages,
                    //free_record_pages: free_record_pages,
                    root: None,
                };
            },
            false => {
                return Self {
                    node_pager,
                    record_pager, 
                    dirname: directory_name.clone(),
                    free_node_pages,
                    root: Some(PagePointer {
                        page_type: PageType::Node,
                        page_address: PageAddress(0),
                        node_offset: NodeOffset(0),
                    }),
                    };
                },
            };

    }

    ///Returns whether or not the exact provided descriptor is in the tree
    pub fn record_in_tree(&mut self, record: &CompoundRecord) -> Result<bool, String> {
        let mut curr_pointer: PagePointer = match &self.root {

            None => { PagePointer {
                page_type: PageType::Leaf,
                page_address: PageAddress(0),
                node_offset: NodeOffset(0),
                }
            },
            Some(x) => x.clone(),
        };


        loop {
            match curr_pointer.page_type {
                PageType::Leaf => {

                    let page: LeafPage = self.record_pager.get_record_page(&curr_pointer.page_address).unwrap();
                    return Ok(page.descriptor_in_page(&record.descriptor));

                },
                PageType::Node => {

                    let page = self.node_pager.get_node_page(&curr_pointer.page_address).unwrap();
                    let node = page.get_node_at(curr_pointer.node_offset.clone()).unwrap();

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

    ///Returns the `n` nearest neighbors of the provided `query_descriptor`
    ///
    ///Performance should worsen as `n` grows larger, as fewer branches of the tree can be pruned
    ///with more distant already-found points
    pub fn get_nearest_neighbors(&mut self, query_descriptor: &Descriptor, n: usize) -> TopHits {

        let mut hits = TopHits::new(n);

        //direction is the one we go if we pass!!!
        let mut nodes_to_check: VecDeque<(PagePointer, NodeAction, Option<Direction>)> = VecDeque::new();

        let root_pointer: PagePointer = match &self.root {

            None => { panic!() },
            Some(x) => x.clone(),
        };


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

                            let page: LeafPage = self.record_pager.get_record_page(&curr_pointer.page_address.clone()).unwrap();

                            for record in page.get_records() {
                                let dist = query_descriptor.distance(&record.descriptor);

                                hits.try_add(dist, &record, &curr_pointer).unwrap();
                            }


                        },
                        PageType::Node => {

                            let page = self.node_pager.get_node_page(&curr_pointer.page_address).unwrap();
                            let node = page.get_node_at(curr_pointer.node_offset.clone()).unwrap();
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
                                    nodes_to_check.push_front((curr_pointer.clone(), NodeAction::CheckIgnoredBranch, Some(Direction::Right)));
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
                                    nodes_to_check.push_front((curr_pointer.clone(), NodeAction::CheckIgnoredBranch, Some(Direction::Left)));
                                },
                            }
                        },
                    }
                },

                NodeAction::CheckIgnoredBranch => {

                    let page = self.node_pager.get_node_page(&curr_pointer.page_address.clone()).unwrap();
                    let node = page.get_node_at(curr_pointer.node_offset.clone()).unwrap();

                    let split_axis = node.split_axis;
                    let split_value = node.split_value;

                    //calc_distance to this axis and check it
                    
                    fn dist_to_axis(split_axis: usize, split_value: f32, descriptor: &Descriptor) -> f32 {

                        return (descriptor.data[split_axis] - split_value).abs()
                 
                    }

                    let dist = dist_to_axis(split_axis, split_value, query_descriptor);
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

        return hits;

    }

    ///Adds the records to the tree. Descends down the tree until a leaf node is found, and appends
    ///the records to that node. If this fills the node, the node is split at its median and two
    ///half-filled leaf nodes are created. A new internal node is created to point to these two
    ///children.
    pub fn add_record(&mut self, record: &CompoundRecord) -> Result<(), String> {

        let mut curr_pointer: PagePointer = match &self.root {

            None => { PagePointer {
                page_type: PageType::Leaf,
                page_address: PageAddress(0),
                node_offset: NodeOffset(0),
                }
            },
            Some(x) => x.clone(),
        };

        //should only persist if there are no nodes yet
        let mut last_pointer =  PagePointer {
            page_type: PageType::Node,
            page_address: PageAddress(0),
            node_offset: NodeOffset(0),
        };

        let mut last_was_left = true;

        //let curr_address =  &curr_pointer.page_address.clone();

        loop {
            //println!("WE ARE AT {:?}|{:?}|{:?}", curr_pointer.page_type, curr_pointer.page_address, curr_pointer.node_offset);
            match curr_pointer.page_type {
                PageType::Leaf => {

                    let mut page: LeafPage = self.record_pager.get_record_page(&curr_pointer.page_address).unwrap();

                    page.add_record(record)?;


                    match page.is_full() {
                        true => { //dbg!("NEED TO SPLIT");
                            let _ = &self.split(page, &curr_pointer, &last_pointer, last_was_left);},
                        false => { self.record_pager.write_page_at_offset(&page, &curr_pointer.page_address).unwrap(); },
                    }

                    break;
                },
                PageType::Node => {

                    let page = self.node_pager.get_node_page(&curr_pointer.page_address).unwrap();
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

        Ok(())
    }

    ///Internal method to take a single full LeafPage, find its median at the "next" axis, and
    ///split the records along that median. This is really the only place where new internal nodes
    ///are created.
    pub fn split(&mut self, page: LeafPage, this_pointer: &PagePointer, parent_pointer: &PagePointer, last_was_left: bool) -> Result<(), String> {

        //println!("SPLITTING");
        //dbg!(&this_pointer.page_address);


        //determine the split axis
        let split_axis = match self.root {
            None => {0},
            Some(_) => {
                    let parent_page = self.node_pager.get_node_page(&parent_pointer.page_address).unwrap();
                    let parent_node = parent_page.get_node_at(parent_pointer.node_offset.clone()).unwrap();
                    (parent_node.split_axis + 1) % 8
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

        let mut left_records: Vec<CompoundRecord> = Vec::new();
        let mut right_records: Vec<CompoundRecord> = Vec::new();

        //consume records and allocate into left and right pages
        for x in records.into_iter() {
            let is_left = x.descriptor.data[split_axis] <= median; 
            match is_left {
                true => {left_records.push(x)},
                false => {right_records.push(x)},
            }
        }

        //make new left record page at current offset
        let mut left_record_page = LeafPage::new();
        for record in left_records.iter() {
            left_record_page.add_record(record)?;
        }

        self.record_pager.write_page_at_offset(&left_record_page, &this_pointer.page_address).unwrap();
        
        //make new right record page at next offset
        let mut right_record_page = LeafPage::new();
        for record in right_records.iter() {
            right_record_page.add_record(record)?;
        }

        let right_child_address = self.record_pager.write_page(&right_record_page).unwrap();

        //make new node
        let node = InternalNode {
            parent_page_address: PageAddress(0), //deprecated
            parent_node_offset: NodeOffset(0), //deprecated
            left_child_page_address: this_pointer.page_address.clone(),
            left_child_node_offset: this_pointer.node_offset.clone(), //not used
            left_child_type: PageType::Leaf,
            right_child_page_address: right_child_address,
            right_child_node_offset: NodeOffset(0), //not used
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
        //    node_offset: NodeOffset(0), //not used for leaf
        //};

        if self.root == None {
            //println!("UPDATING ROOT");
            self.root = Some(pointer);
        }


        Ok(())
    }

    ///Internal method that's called during the split procedure. The new node needs to be assigned
    ///to a node page with free space, or assigned to a newly allocated node page. Returns a
    ///pointer to the newly created node wherever it's placed.
    pub fn add_new_node(&mut self, node: &InternalNode) -> Result<PagePointer, String> {

        //find a page with space for nodes
        //
        let mut chosen_address: Option<PageAddress> = None;

        //dbg!("PAGES TO CHECK");
        //dbg!(&self.free_node_pages);
        let mut num_pops: usize = 0;
        for possibly_free_page_address in &self.free_node_pages {
            //dbg!(&possibly_free_page_address);

            //println!("POSSBLE FREE ADDRESS: {:?}", possibly_free_page_address);
            let possibly_free_page = self.node_pager.get_node_page(possibly_free_page_address).unwrap();
            //println!("IS READ");
            //dbg!(&possibly_free_page);

            match possibly_free_page.is_full() {
                true => {//todo remove this page address from the list of options
                    //println!("IT'S FULL: {:?}", possibly_free_page_address);
                    num_pops += 1;
                    },
                false => {
                    chosen_address = Some(possibly_free_page_address.clone()); 
                    //println!("IT HAS ROOM, PICKING: {:?}", possibly_free_page_address);
                    break;},
            }
        }

        for _ in 0..num_pops {
            //println!("POPPING");
            self.free_node_pages.pop_front();
        }

        match chosen_address {
            None => { //we never found a free page?
                let mut page = NodePage::new();
                let offset = page.add_node(node).unwrap();

                let page_address = self.node_pager.write_page(&page).unwrap();

                self.free_node_pages.push_back(page_address.clone());

                let pointer = PagePointer {
                    page_type: PageType::Node,
                    page_address,
                    node_offset: offset,
                };

                return Ok(pointer);


            },

            Some(chosen_address) => {
                let mut chosen_page = self.node_pager.get_node_page(&chosen_address).unwrap();
                let offset = chosen_page.add_node(node).unwrap();

                self.node_pager.write_page_at_offset(&chosen_page, &chosen_address).unwrap();
                let pointer = PagePointer {
                    page_type: PageType::Node,
                    page_address: chosen_address,
                    node_offset: offset,
                };
                return Ok(pointer);

            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;
    use crate::node::{CompoundRecord, CompoundIdentifier, Descriptor};
    use rand::distributions::Alphanumeric;
    use kdam::tqdm;
    use rand::prelude::*;

    #[test]
    fn quick_tree_new() {

        let mut tree = Tree::new("test_data/tree_new/".to_string(), true);

        fn get_random_record() -> CompoundRecord {

            let random_arr: [f32; 8] = rand::random();
            let mut rng = thread_rng();
            let chars: String = (0..16).map(|_| rng.sample(Alphanumeric) as char).collect();

            let descriptor = Descriptor { data: random_arr };

            let identifier = CompoundIdentifier::from_string(chars);

            let cr = CompoundRecord {
                dataset_identifier: 0,
                compound_identifier: identifier,
                descriptor,
            };

            return cr;
        }

        let cr = get_random_record();
        tree.add_record(&cr).unwrap();

        let cr = get_random_record();
        tree.add_record(&cr).unwrap();

        for _ in 0..2000 {

            let cr = get_random_record();
            tree.add_record(&cr).unwrap();
        }
    }

    #[test]
    fn quick_tree_find() {

        let mut tree = Tree::new("test_data/aaab/".to_string(), true);
        //dbg!(&tree);


        let cr_to_find = get_random_record();

        fn get_random_record() -> CompoundRecord {

            let random_arr: [f32; 8] = rand::random();
            //let random_chars: [u8; 16] = rand::random();
            let mut rng = thread_rng();
            let chars: String = (0..16).map(|_| rng.sample(Alphanumeric) as char).collect();

            let descriptor = Descriptor { data: random_arr };

            let identifier = CompoundIdentifier::from_string(chars);

            let cr = CompoundRecord {
                dataset_identifier: 0,
                compound_identifier: identifier,
                descriptor,
            };

            return cr;
        }

        let cr = get_random_record();
        tree.add_record(&cr).unwrap();

        let bad_record = get_random_record();
        let answer = tree.record_in_tree(&bad_record).unwrap();
        assert_eq!(answer, false);

        let cr = get_random_record();
        tree.add_record(&cr).unwrap();

        let bad_record = get_random_record();
        let answer = tree.record_in_tree(&bad_record).unwrap();
        assert_eq!(answer, false);

        for _ in 0..2000 {

            let cr = get_random_record();
            //dbg!(&cr);
            tree.add_record(&cr).unwrap();
        }

        let answer = tree.record_in_tree(&cr_to_find).unwrap();
        assert_eq!(answer, false);

        tree.add_record(&cr_to_find).unwrap();

        let answer = tree.record_in_tree(&cr_to_find).unwrap();
        assert_eq!(answer, true);

        for _ in 0..2000 {

            let cr = get_random_record();
            tree.add_record(&cr).unwrap();
        }

        let answer = tree.record_in_tree(&cr_to_find).unwrap();
        assert_eq!(answer, true);

        let bad_record = get_random_record();
        let answer = tree.record_in_tree(&bad_record).unwrap();
        assert_eq!(answer, false);


    }

    #[test]
    fn quick_tree_nn() {

        let mut tree = Tree::new("test_data/aaaa/".to_string(), true);
        //dbg!(&tree);


        let cr_to_find = get_random_record();

        fn get_random_record() -> CompoundRecord {

            let random_arr: [f32; 8] = rand::random();
            //let random_chars: [u8; 16] = rand::random();
            let mut rng = thread_rng();
            let chars: String = (0..16).map(|_| rng.sample(Alphanumeric) as char).collect();

            let descriptor = Descriptor { data: random_arr };

            let identifier = CompoundIdentifier::from_string(chars);

            let cr = CompoundRecord {
                dataset_identifier: 0,
                compound_identifier: identifier,
                descriptor,
            };

            return cr;
        }

        let cr = get_random_record();
        tree.add_record(&cr).unwrap();

        let bad_record = get_random_record();
        let answer = tree.record_in_tree(&bad_record).unwrap();
        assert_eq!(answer, false);

        let cr = get_random_record();
        tree.add_record(&cr).unwrap();

        let bad_record = get_random_record();
        let answer = tree.record_in_tree(&bad_record).unwrap();
        assert_eq!(answer, false);

        for _ in tqdm!(0..20000) {

            let cr = get_random_record();
            tree.add_record(&cr).unwrap();
        }

        let answer = tree.record_in_tree(&cr_to_find).unwrap();
        assert_eq!(answer, false);

        tree.add_record(&cr_to_find).unwrap();

        let answer = tree.record_in_tree(&cr_to_find).unwrap();
        assert_eq!(answer, true);

        for _ in 0..2000 {

            let cr = get_random_record();
            tree.add_record(&cr).unwrap();
        }

        let answer = tree.record_in_tree(&cr_to_find).unwrap();
        assert_eq!(answer, true);

        let bad_record = get_random_record();
        let answer = tree.record_in_tree(&bad_record).unwrap();
        assert_eq!(answer, false);

        let _nn = tree.get_nearest_neighbors(&bad_record.descriptor, 1);

    }


    #[test]
    fn build_verify_nn_accuracy() {


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
            let identifier = CompoundIdentifier(s.next().unwrap().to_string());
            let s: Vec<_> = s.collect();

            //let s: Vec<f32> = s.into_iter().map(|x| x.try_into().unwrap()).collect();
            let s: Vec<f32> = s.into_iter().map(|x| x.parse::<f32>().unwrap()).collect();
            let descriptor = Descriptor::from_vec(s.clone());

            assert_eq!(s.len(), 8);

            let cr = CompoundRecord {
                compound_identifier: identifier,
                descriptor,
                dataset_identifier: 1,
            };

            records.push(cr);
        }

        dbg!(records.len());


        let mut tree = Tree::new("test_data/bvnnacc/".to_string(), true);

        for record in tqdm!(records.iter()) {

            tree.add_record(&record.clone()).unwrap();
        }

    }


    #[bench]
    fn benchmark_query_speed(b: &mut Bencher) {

        fn make_random_query(tree: &mut Tree) {

            let data: [f32; 8] = random();
            let descriptor: Descriptor = Descriptor {data};

            let _nn = tree.get_nearest_neighbors(&descriptor, 20);
        }

        let mut tree = Tree::new("test_data/bqs/".to_string(), false);
        b.iter(|| make_random_query(&mut tree));




    }


    #[test]
    fn query_verify_nn_accuracy(){
        /*

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
            let identifier = CompoundIdentifier(s.next().unwrap().to_string());
            let s: Vec<_> = s.collect();

            //let s: Vec<f32> = s.into_iter().map(|x| x.try_into().unwrap()).collect();
            let s: Vec<f32> = s.into_iter().map(|x| x.parse::<f32>().unwrap()).collect();
            let descriptor = Descriptor::from_vec(s.clone());

            assert_eq!(s.len(), 8);

            let cr = CompoundRecord {
                compound_identifier: identifier,
                descriptor,
                dataset_identifier: 1,
            };

            records.push(cr);
        }

        dbg!(records.len());
        */
        let descriptor = Descriptor::from_vec(
            vec![
            0.8598341,
            0.6338788,
            0.68099475,
            0.8503834,
            0.58941144,
            0.84688795,
            0.61008036,
            0.88481283,
            ]

        );

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


        let mut tree = Tree::new("test_data/bvnnacc/".to_string(), false);

        let nn = tree.get_nearest_neighbors(&descriptor, 50);
        dbg!(&nn);
        dbg!(&nn.distances);
        let identifiers: Vec<_> = nn.records.into_iter().map(|x| x.clone().unwrap().compound_identifier.0.clone()).collect();
        dbg!(&identifiers);

        assert_eq!(identifiers, correct_answer);
        //tree.record_pager.print_records();
        //tree.node_pager.print_nodes();
    }


    #[test]
    fn slow_fuzzed_verify_nn_accuracy(){

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
            let identifier = CompoundIdentifier(s.next().unwrap().to_string());
            let s: Vec<_> = s.collect();

            //let s: Vec<f32> = s.into_iter().map(|x| x.try_into().unwrap()).collect();
            let s: Vec<f32> = s.into_iter().map(|x| x.parse::<f32>().unwrap()).collect();
            let descriptor = Descriptor::from_vec(s.clone());

            assert_eq!(s.len(), 8);

            let cr = CompoundRecord {
                compound_identifier: identifier,
                descriptor,
                dataset_identifier: 1,
            };

            records.push(cr);
        }

        let descriptor = Descriptor::from_vec(
            vec![
            0.8598341,
            0.6338788,
            0.68099475,
            0.8503834,
            0.58941144,
            0.84688795,
            0.61008036,
            0.88481283,
            ]

        );

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

        use rand::thread_rng;
        use rand::seq::SliceRandom;

        for _ in 0..5 {

            records.shuffle(&mut thread_rng());


            let mut build_tree = Tree::new("test_data/nn_validation/".to_string(), true);

            for record in tqdm!(records.iter()) {

                build_tree.add_record(&record.clone()).unwrap();
            }


            let mut query_tree = Tree::new("test_data/nn_validation/".to_string(), false);

            let nn = query_tree.get_nearest_neighbors(&descriptor, 50);
            let identifiers: Vec<_> = nn.records.into_iter().map(|x| x.clone().unwrap().compound_identifier.0.clone()).collect();

            assert_eq!(identifiers, correct_answer);
        }
    }

    #[test]
    fn quick_fuzzed_verify_nn_accuracy(){

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
            let identifier = CompoundIdentifier(s.next().unwrap().to_string());
            let s: Vec<_> = s.collect();

            //let s: Vec<f32> = s.into_iter().map(|x| x.try_into().unwrap()).collect();
            let s: Vec<f32> = s.into_iter().map(|x| x.parse::<f32>().unwrap()).collect();
            let descriptor = Descriptor::from_vec(s.clone());

            assert_eq!(s.len(), 8);

            let cr = CompoundRecord {
                compound_identifier: identifier,
                descriptor,
                dataset_identifier: 1,
            };

            records.push(cr);
        }

        let descriptor = Descriptor::from_vec(
            vec![0.5613212933959323,
                 0.5027493566508184,
                 0.7390574788950892,
                 0.4562167305584901,
                 0.02413149926370306,
                 0.8082303147232089,
                 0.762055388982211,
                 0.34713323654674944]
        );

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

        use rand::thread_rng;
        use rand::seq::SliceRandom;

        for _ in 0..5 {

            records.shuffle(&mut thread_rng());


            let mut build_tree = Tree::new("test_data/qf/".to_string(), true);

            for record in tqdm!(records.iter()) {

                build_tree.add_record(&record.clone()).unwrap();
            }


            let mut query_tree = Tree::new("test_data/qf/".to_string(), false);

            let nn = query_tree.get_nearest_neighbors(&descriptor, 50);
            let identifiers: Vec<_> = nn.records.into_iter().map(|x| x.clone().unwrap().compound_identifier.0.clone()).collect();

            assert_eq!(identifiers, correct_answer);
        }
    }









}



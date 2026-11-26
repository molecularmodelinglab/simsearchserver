//! Implementation of kd-tree creation and querying
use crate::data::{Descriptor, CompoundRecord, CompoundIndex};
use crate::database::{Database, ImmutDatabase};
use crate::node::{InternalNode, PagePointer};
use crate::page::RecordPage;
use crate::layout;
use crate::io::{ImmutNodePager, FastNodePager, RecordPager, GetNode};
use crate::data::{Parser};
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
    pub fn get_nearest_neighbors(&self, query_descriptor: &Descriptor, n: usize) -> NearestNeighbors {

        let top_hits = self.get_top_hits(query_descriptor, n);

        let nearest_neighbors = NearestNeighbors::from_top_hits(top_hits, &self.database);

        return nearest_neighbors;
    }

    /// The `n` nearest neighbors of `query_descriptor`.
    fn get_top_hits(&self, query_descriptor: &Descriptor, n: usize) -> TopHits {

        let mut hits = TopHits::new(n);
        let query_started = Instant::now();

        let desc_length = self.config.desc_length;

        // Flat arena so the heap entries stay small and copy-free.
        let mut offsets: Vec<f32> = vec![0.0; desc_length];
        let mut arena: Vec<f32> = vec![0.0; desc_length];

        // Raw bits of non-negative floats order like the floats themselves.
        let mut frontier: BinaryHeap<Reverse<(u32, u8, usize, usize)>> = BinaryHeap::new();

        let (kind, index) = encode_pointer(&self.root);
        frontier.push(Reverse((0u32, kind, index, 0)));

        while let Some(Reverse((bound_bits, kind, index, offsets_at))) = frontier.pop() {

            // A near child never moves the query away, so both hold for the descent.
            let bound = f32::from_bits(bound_bits);

            if bound >= hits.get_highest_squared_dist() {
                continue;
            }

            offsets.copy_from_slice(&arena[offsets_at..offsets_at + desc_length]);

            let mut current = decode_pointer(kind, index);

            loop {

                match current {

                    PagePointer::Leaf(leaf_index) => {

                        hits.record_pages_visited += 1;

                        let fetch_started = Instant::now();
                        let page = self.record_handler.page_ref(&leaf_index).unwrap();
                        // The page's own box bounds every axis at once.
                        let bounds_distance = page.squared_distance_to_bounds(query_descriptor);
                        hits.page_fetch_nanos += fetch_started.elapsed().as_nanos() as u64;

                        if bounds_distance >= hits.get_highest_squared_dist() {
                            hits.record_pages_pruned += 1;
                            break;
                        }

                        let scan_started = Instant::now();
                        hits.records_scanned += page.len();

                        for offset in 0..page.len() {

                            let squared = page.squared_distance_at(offset, query_descriptor);

                            if squared >= hits.get_highest_squared_dist() {
                                continue;
                            }

                            let record = page.get_record_at(offset).unwrap();

                            hits.try_add(squared, &record);
                        }

                        hits.scan_nanos += scan_started.elapsed().as_nanos() as u64;

                        break;
                    },

                    PagePointer::Node(node_index) => {

                        let node_started = Instant::now();
                        hits.nodes_visited += 1;

                        let node = self.node_handler.get_node(&node_index).unwrap().clone();

                        let delta = query_descriptor.data[node.split_axis] - node.split_value;

                        let (near, far) = match delta <= 0.0 {
                            true => (node.left_child_pointer, node.right_child_pointer),
                            false => (node.right_child_pointer, node.left_child_pointer),
                        };

                        // |delta| can only exceed the offset already recorded, so this cannot double count.
                        let previous = offsets[node.split_axis];
                        // The heap key only orders non-negative floats.
                        let far_bound = (bound - previous * previous + delta * delta).max(0.0);

                        if far_bound < hits.get_highest_squared_dist() {
                            let (far_kind, far_index) = encode_pointer(&far);
                            let offsets_at = arena.len();
                            arena.extend_from_slice(&offsets);
                            arena[offsets_at + node.split_axis] = delta.abs();
                            frontier.push(Reverse((
                                far_bound.to_bits(), far_kind, far_index, offsets_at)));
                        }

                        current = near;
                        hits.node_nanos += node_started.elapsed().as_nanos() as u64;
                    },
                }
            }
        }

        hits.total_nanos = query_started.elapsed().as_nanos() as u64;

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
    
    pub fn random(rng: &mut impl Rng, length: usize) -> TreeRecord {

        let descriptor = Descriptor::random(rng, length);
        let index = rng.gen_range(0..10000);

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
            .copy_from_slice(&self.index.to_le_bytes());

        for i in 0..self.length {
            let at = layout::DESCRIPTOR_START + i * 4;
            LittleEndian::write_f32(&mut buf[at..at + 4], self.descriptor.data[i]);
        }
    }
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

/// A kd-tree on disk: internal nodes and leaf pages in separate files.
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
    /// Record indexes, for looking a hit up in a store keyed by them.
    pub indexes: Vec<CompoundIndex>,
    pub record_pages_visited: usize,
    pub record_pages_pruned: usize,
    pub nodes_visited: usize,
    pub records_scanned: usize,
    pub page_fetch_nanos: u64,
    pub scan_nanos: u64,
    pub insert_nanos: u64,
    pub node_nanos: u64,
    pub total_nanos: u64,
}

impl NearestNeighbors {

    fn from_top_hits(top_hits: TopHits, database: &ImmutDatabase) -> Self {

        let mut distances: Vec<f32> = Vec::new();
        let mut records: Vec<Option<CompoundRecord>> = Vec::new();
        let mut indexes: Vec<CompoundIndex> = Vec::new();

        // Sorted ascending, and only as many as were found, so k > n is not a panic.
        for candidate in top_hits.hits.into_sorted_vec() {

                distances.push(candidate.squared_distance.sqrt());
                indexes.push(candidate.record.index);

                records.push(match database.query(&candidate.record.index) {
                    Some(entry) => Some(CompoundRecord {
                        smiles: entry.smiles.clone(),
                        compound_identifier: entry.identifier.clone(),
                        descriptor: candidate.record.descriptor.clone(),
                        length: candidate.record.length,
                    }),
                    // Indexed record missing from the database: drop the hit, not the query.
                    None => None,
                });

        }

        return Self {
            distances,
            records,
            indexes,
            record_pages_visited: top_hits.record_pages_visited,
            record_pages_pruned: top_hits.record_pages_pruned,
            nodes_visited: top_hits.nodes_visited,
            records_scanned: top_hits.records_scanned,
            page_fetch_nanos: top_hits.page_fetch_nanos,
            scan_nanos: top_hits.scan_nanos,
            insert_nanos: top_hits.insert_nanos,
            node_nanos: top_hits.node_nanos,
            total_nanos: top_hits.total_nanos,
        }
    }
}


/// Ordered by distance so a heap evicts the worst without a scan.
#[derive(Debug)]
struct Candidate {
    squared_distance: f32,
    record: TreeRecord,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        return self.squared_distance == other.squared_distance;
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        return Some(self.cmp(other));
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        return self.squared_distance.total_cmp(&other.squared_distance);
    }
}

/// The `n` closest records seen so far, as a bounded max-heap.
#[derive(Debug)]
pub struct TopHits {
    pub max_length: usize,
    pub nodes_visited: usize,
    pub record_pages_visited: usize,
    pub record_pages_pruned: usize,
    pub records_scanned: usize,
    pub page_fetch_nanos: u64,
    pub scan_nanos: u64,
    pub insert_nanos: u64,
    pub node_nanos: u64,
    pub total_nanos: u64,
    hits: BinaryHeap<Candidate>,
}

impl TopHits {

    pub fn new(max_length: usize) -> Self {

        return Self {
            max_length,
            nodes_visited: 0,
            record_pages_visited: 0,
            record_pages_pruned: 0,
            records_scanned: 0,
            page_fetch_nanos: 0,
            scan_nanos: 0,
            insert_nanos: 0,
            node_nanos: 0,
            total_nanos: 0,
            hits: BinaryHeap::with_capacity(max_length),
        }
    }

    pub fn try_add(&mut self, squared_distance: f32, record: &TreeRecord) {

        if self.max_length == 0 {
            return;
        }

        let started = Instant::now();

        if self.hits.len() < self.max_length {
            self.hits.push(Candidate { squared_distance, record: record.clone() });
        } else if squared_distance < self.hits.peek().expect("a full heap has a root").squared_distance {
            self.hits.pop();
            self.hits.push(Candidate { squared_distance, record: record.clone() });
        }

        self.insert_nanos += started.elapsed().as_nanos() as u64;
    }

    /// The pruning threshold; nothing is ruled out until `max_length` hits are held.
    pub fn get_highest_squared_dist(&self) -> f32 {

        if self.max_length == 0 {
            return 0.0;
        }

        if self.hits.len() < self.max_length {
            return f32::MAX;
        }

        return self.hits.peek().expect("a full heap has a root").squared_distance;
    }

    pub fn len(&self) -> usize {
        return self.hits.len();
    }
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
    pub fn num_nodes(&mut self) -> usize {

        return self.node_handler.num_nodes();

    }
    /// Appends to the leaf the descriptor belongs in, splitting it when full.
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
                        Err(e) => panic!("could not add a record to a leaf page: {}", e),
                    }

                    match page.is_full() {
                        true => {
                            let _ = &self.split(page, &curr_pointer, &last_pointer, last_was_left);},
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
    /// Splits a full page in two and links them under a new internal node.
    pub fn split(&mut self, page: RecordPage, this_pointer: &PagePointer, parent_pointer: &PagePointer, last_was_left: bool) -> Result<(), String> {


        let parent_node: Option<InternalNode> = match &parent_pointer {
            PagePointer::Leaf(_) => None,
            PagePointer::Node(index) => {

            match self.node_handler.get_node(index) {
                Ok(x) => Some(x.clone()),
                Err(_) => None,
                }
            }
        };

        let n_records = page.len();

        // Cycling axes by depth can pick one holding a single repeated value.
        let mut split_axis = 0;
        let mut widest = f32::NEG_INFINITY;

        for axis in 0..self.config.desc_length {

            let mut low = f32::INFINITY;
            let mut high = f32::NEG_INFINITY;

            for offset in 0..n_records {
                let value = page.descriptor_component_at(offset, axis);
                low = low.min(value);
                high = high.max(value);
            }

            if high - low > widest {
                widest = high - low;
                split_axis = axis;
            }
        }

        let mut values: Vec<f32> = (0..n_records)
            .map(|offset| page.descriptor_component_at(offset, split_axis))
            .collect();

        values.sort_by(|a, b| a.partial_cmp(b).expect("descriptor values are finite"));

        let largest = values[n_records - 1];

        // A median equal to the largest value would leave the right page empty.
        let mut median = values[n_records / 2];

        if median >= largest {
            median = match values.iter().rev().find(|value| **value < largest) {
                Some(value) => *value,
                None => return Err(format!(
                    "cannot split a page whose {} records share one descriptor value on \
                     every axis; the page size is too small for this many duplicates",
                    n_records)),
            };
        }

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
        
        let right_child_pointer = self.record_handler.add_page(&right_record_page).unwrap();

        let node = InternalNode {
            left_child_pointer: this_pointer.clone(),
            right_child_pointer,
            split_axis,
            split_value: median,
        };

        let pointer = self.node_handler.add_node(&node).unwrap();

        match parent_node {
            Some(x) => {
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

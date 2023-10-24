use std::fs::File;
use std::io::prelude::*;
use crate::node::{PagePointer};
use crate::data::{CompoundRecord};
use crate::tree::{Tree, TreeConfig, TreeRecord};
use crate::io::{GetNode};
use crate::page::RecordPage;
use std::collections::{HashMap, VecDeque};

use serde::{Serialize, Deserialize};
use serde_json::Result;

#[derive(Debug, Serialize, Deserialize)]
pub struct QuerySet {
    queries: Vec<(RangeQuery, f32)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RangeQuery {
    map: HashMap<usize, RangeNode>,
    num_axes: usize,
    lower_bound: f32,
    upper_bound: f32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RangeNode {
    lower_bound: f32,
    upper_bound: f32,
}

impl RangeNode {
    
    pub fn random() -> Self {
        let bound_a = rand::random::<f32>();
        let bound_b = rand::random::<f32>();

        if bound_a < bound_b {
            return Self {
                upper_bound: bound_b,
                lower_bound: bound_a,
            }
        }
        else if bound_b < bound_a {
            return Self {
                upper_bound: bound_a,
                lower_bound: bound_b,
            }
        }
        else {
            panic!();
        }
    }

    pub fn to_string(&self) -> String {
        return format!("{},{}", self.lower_bound, self.upper_bound);
    }
}

impl RangeQuery {

    pub fn random(num_axes: usize, lower_bound: f32, upper_bound: f32) -> Self {

        //random length between 3 and 8
        let length = rand::random::<usize>() % 5 + 3;

        let mut hm: HashMap<usize, RangeNode> = HashMap::new();
        for i in 0..length {
     
            let node = RangeNode::random();
            hm.insert(i, node);
        }

        let query = RangeQuery {
            map: hm,
            num_axes: 16,
            lower_bound,
            upper_bound,
        };

        return query;
    }

    pub fn random_with_size(num_axes: usize, size: usize, lower_bound: f32, upper_bound: f32) -> Self {

       let length = size;

        let mut hm: HashMap<usize, RangeNode> = HashMap::new();
        for i in 0..length {
     
            let node = RangeNode::random();
            hm.insert(i, node);
        }

        let query = RangeQuery {
            map: hm,
            num_axes: 16,
            lower_bound,
            upper_bound,
        };

        return query;
    }

    pub fn to_string(&self) -> String {

        let mut s = "".to_string();
        for (axis, node) in self.map.iter() {
            s += &format!("{},{}|", axis, node.to_string());
        }

        return s;


    }

    pub fn from_string(s: &str) -> Self {

        let global_lower_bound = -1.0;
        let global_upper_bound = 1.0;
        let num_axes = 16;

        let mut map: HashMap<usize, RangeNode> = HashMap::new();
        let bounds = s.split("|").collect::<Vec<&str>>();
        for bound in bounds {
            dbg!(&bound);

            if bound.len() == 0 {
                break;
            }

            let fields = bound.split(",").collect::<Vec<&str>>();

            let axis = fields[0].parse::<usize>().unwrap();
            let lb = fields[1].parse::<f32>().unwrap();
            let ub = fields[2].parse::<f32>().unwrap();

            let node = RangeNode {
                lower_bound: lb,
                upper_bound: ub,
            };

            map.insert(axis, node);

        }

        return RangeQuery{
            map,
            num_axes,
            lower_bound: global_lower_bound,
            upper_bound: global_upper_bound,
        };
        
    }

    pub fn area(&self) -> f32 {

        let mut area = 1.0;
        for i in 0..self.num_axes {
            let node = self.map.get(&i);

            let this_length = match node {
                None => {self.upper_bound - self.lower_bound},
                Some(x) => {
                    x.upper_bound - x.lower_bound
                }
            };
                
        area *= this_length;
        }

        return area;
    }

    pub fn proportion(&self) -> f32 {

        let area = self.area();

        let space_area = (self.upper_bound - self.lower_bound).powi(self.num_axes as i32);

        dbg!(&area);
        dbg!(&space_area);

        return area / space_area;
    }
}

impl QuerySet {

    pub fn to_string(&self) -> String {

        return serde_json::to_string(&self).unwrap();

    }

    pub fn to_file(&self, filename: &str) {

        dbg!(&filename);
        let mut file = std::fs::File::create(filename).unwrap();
        let s = serde_json::to_string(&self).unwrap();
        file.write_all(s.as_bytes()).unwrap();
    }

    pub fn from_string(s: &str) -> Self {
        return serde_json::from_str(s).unwrap();
    }
    /*
    pub fn from_string(s: &str) -> Self {
        
        let chunks = s.split("\n").collect::<Vec<&str>>();
        dbg!(&chunks);


        let mut v: Vec<(RangeQuery, f32)> = Vec::new();
        for chunk in chunks {
            if chunk == "" {
                continue;
            }
            let fields = chunk.split("&").collect::<Vec<&str>>();
            let bound_string = fields[0];
            let prob = fields[1].parse::<f32>().unwrap();
            let query = RangeQuery::from_string(bound_string);
            v.push((query, prob));
        }

        let mut qs = QuerySet{queries: v};
        qs.sort();
        return qs;
    }
    */

    pub fn sort(&mut self) {
        self.queries.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    }

}

pub fn check_record(query: &RangeQuery, record: &TreeRecord) -> bool {


    for i in 0..record.descriptor.data.len() {

        let range_node = query.map.get(&i);

        match range_node {
            None => {continue},
            Some(x) => {

                let descriptor_value = record.descriptor.data[i];

                if descriptor_value > x.upper_bound {
                    return false;
                }

                if descriptor_value < x.lower_bound {
                    return false;
                }
            }
        }
    }

    return true;
}

pub fn run_range_query(tree: &mut Tree , query: &RangeQuery) -> Vec<CompoundRecord> {

        dbg!(&query);

        let mut hits: Vec<CompoundRecord>= Vec::new();

        let mut num_nodes_visited: usize = 0;
        let mut num_record_pages_visited: usize = 0;

        //direction is the one we go if we pass!!!
        let mut nodes_to_check: VecDeque<PagePointer> = VecDeque::new();

        let root_pointer = tree.root.clone();

        nodes_to_check.push_front(root_pointer);

        let mut candidate_count: usize = 0;
        let mut hit_count: usize = 0;


        loop {

            //dbg!(&nodes_to_check);
            let popped_val = nodes_to_check.pop_front();
            //dbg!(&popped_val);

            let curr_pointer = match popped_val {
                None => {break;},
                Some(x) => {x},
            };

            match curr_pointer {
                PagePointer::Leaf(index) => {

                    num_record_pages_visited += 1;

                    let page = tree.get_record_page(&index);

                    for record in page.get_records() {
                        //println!("CANDIDATE: {:?}", record.compound_identifier.to_string());
                        //println!("DESC: {:?}", record.descriptor);
                        candidate_count += 1;

                        if check_record(&query, &record) {
                            //hits.push(record.clone());
                            //println!("HIT: {:?}", record.index.to_string());
                            //println!("HIT: {:?}", record.descriptor);
                            hit_count += 1;
                        }
                    }
                },
                PagePointer::Node(index) => {
                    //dbg!("HERE");

                    num_nodes_visited += 1;

                    let node = tree.node_handler.get_node(&index).unwrap().clone();

                    let axis = node.split_axis;
                    let split_value = node.split_value;
                   


                    let range_node = query.map.get(&axis);

                    match range_node {
                        None => {
                            nodes_to_check.push_front(node.right_child_pointer.clone());
                            nodes_to_check.push_front(node.left_child_pointer.clone());
                    },
                        Some(x) => {

                            let mut skip_left_node = false;
                            let mut skip_right_node = false;

                            if split_value > x.upper_bound {
                                skip_right_node = true;
                            }
                            if split_value < x.lower_bound {
                                skip_left_node = true;

                            }

                            if !skip_right_node {
                                
                                nodes_to_check.push_front(node.right_child_pointer.clone());

                            }

                            if !skip_left_node {
                                nodes_to_check.push_front(node.left_child_pointer.clone());
                            }

                        }
                    }
                }
            }
        }

        let prop_pages = num_record_pages_visited as f32 / tree.record_handler.len() as f32;
        let calc_prop = query.proportion();

        println!("NODES VISITED: {:?}", num_nodes_visited);
        println!("RECORD PAGES VISITED: {:?}", num_record_pages_visited);
        println!("PROP RECORD PAGES VISITED: {:?}", prop_pages);
        println!("CALCULATED AREA PROP: {:?}", calc_prop);
        println!("CANDIDATES: {:?}", candidate_count);
        println!("HITS: {:?}", hit_count);

        return hits;



}

#[cfg(test)]
mod tests {
    use super::*;
    use kdam::tqdm;

    #[test]
    fn quick_range_query() {

        let path = RangeQuery::random(16, -1.0, 1.0);
        dbg!(&path);

        let s = path.to_string();
        dbg!(&s);

        let s_path = RangeQuery::from_string(&s);
        dbg!(&s_path);


    }

    #[test]
    fn serialization() {

        let query = RangeQuery::random(16, -1.0, 1.0);

        dbg!(serde_json::to_string(&query).unwrap());

        let mut v: Vec<(RangeQuery, f32)> = Vec::new();

        for _ in 0..10 {
            let path = RangeQuery::random(16, -1.0, 1.0);
            //random prob between 0 and 1
            let prob = rand::random::<f32>();
            v.push((path, prob));
        }

        let qs = QuerySet{queries: v};

        qs.to_file("ayy.txt");

        let s = qs.to_string();

        let fs = QuerySet::from_string(&s);
        dbg!(&fs);

    }

    #[test]
    fn quick_query_set() {

        let mut v: Vec<(RangeQuery, f32)> = Vec::new();

        for _ in 0..10 {
            let path = RangeQuery::random(16, -1.0, 1.0);
            //random prob between 0 and 1
            let prob = rand::random::<f32>();
            v.push((path, prob));
        }
        let qs = QuerySet{queries: v};
    }
    #[test]
    fn random_range_query() {

        for n in [16] {

            let mut config = TreeConfig::default();
            config.desc_length = n;
            config.directory = "test_data/rrq".to_string();

            let mut tree = Tree::force_create_with_config(config.clone());

            for _ in tqdm!(0..200000) {

                let cr = CompoundRecord::random(n);
                tree.add_record(&cr).unwrap();
            }

            for size in 3..5 {

                for i in tqdm!(0..100) {

                    let query = RangeQuery::random_with_size(16, size, -1.0, 1.0);
                    dbg!(&query);
                    run_range_query(&mut tree, &query);
                }
            }
        }
    }
}




//! A page is a 4kb byte array that is either a collection of internal nodes or a single leaf node
//! that is a collection of compound records.
//!
//!

use crate::node::{NodeOffset, InternalNode, CompoundRecord, Descriptor};
use crate::layout;

#[derive(Debug, Clone, PartialEq)]
pub enum PageType {
    Node = 1,
    Leaf = 2,
}

pub trait Pageable {
    fn get_data(&self) -> &[u8; layout::PAGE_SIZE];
    fn get_type(&self) -> PageType;
}

#[derive(Debug)]
pub struct NodePage {

    pub data: Box<[u8; layout::PAGE_SIZE]>,
    pub tail: Option<NodeOffset>,
}

impl Pageable for NodePage {

    fn get_data(&self) -> &[u8; layout::PAGE_SIZE] {
        &self.data
    }

    fn get_type(&self) -> PageType {
        return PageType::Node;

    }
}

impl NodePage {

    pub fn new() -> Self {

        let mut arr: [u8; layout::PAGE_SIZE] = [0; layout::PAGE_SIZE];

        arr[layout::PAGE_TYPE_OFFSET] = PageType::Node as u8;
        arr[layout::IS_EMPTY_OFFSET] = 1 as u8;

        //dbg!(arr[layout::PAGE_TYPE_OFFSET]);
        let page = Self {
            data: Box::new(arr),
            tail: None,
        };

        return page

    }

    pub fn from_arr(arr: [u8; layout::PAGE_SIZE]) -> Self {

        //read tail from disk
        let tail = arr[layout::TAIL_OFFSET] as usize;

        //read empty from disk
        let is_empty = arr[layout::IS_EMPTY_OFFSET] as usize;

        let tail_val = match is_empty {
            1 => None,
            2 => Some(NodeOffset(tail)),
            _ => {println!("BAD EMPTY VAL: {:?}", is_empty);
                    dbg!(arr);
                panic!();},
        };

        let page = Self {
            data: Box::new(arr),
            tail: tail_val,
        };

        return page

    }

    pub fn get_capacity(&self) -> usize {

        return (layout::PAGE_SIZE - layout::PAGE_DATA_START) / layout::NODE_SIZE;

    }

    pub fn num_nodes(&self) -> usize {

        match &self.tail {
            None => {return 0;},
            Some(x) => {return x.0 + 1},
        }
        /*
        match self.tail.0 {
            0 => return 0,
            _ => return self.tail.0 / layout::NODE_SIZE,
        }
        */

    }

    pub fn add_node(&mut self, node: &InternalNode) -> Result<NodeOffset, String> {

        match self.push_node(node) {
            Ok(_) => {return Ok(NodeOffset(self.tail.clone().unwrap().0)) },
            Err(s) => panic!("{}", s),
        };


    }

    pub fn push_node(&mut self, node: &InternalNode) ->Result<(), String> {

        match self.is_full() {
            true => return Err("Node is full".to_string()),
            false => {},
        }

        //dbg!("TAIL BEFORE PUSH");
        //dbg!(&self.tail);

        let count = match &self.tail {
            None => 0,
            Some(x) => x.0 + 1,
        };


        let start = layout::PAGE_DATA_START + (count * layout::NODE_SIZE);
        let slice = &mut self.data[start..start + layout::NODE_SIZE];

        slice.copy_from_slice(&node.to_arr());

        //println!("TAIL STARTS AS: {:?}", self.tail);
        self.tail = match &self.tail {
            None => Some(NodeOffset(0)),
            Some(x) => Some(NodeOffset(x.0 + 1)),
        };

        //ensure tail value is updated
        self.data[layout::TAIL_OFFSET] = self.tail.clone().unwrap().0 as u8;

        //ensure is_empty value is updated
        self.data[layout::IS_EMPTY_OFFSET] = 2 as u8;
        
        Ok(())

    }

    pub fn is_full(&self) -> bool {


        /*
        match &self.tail {
            None => {return false;},
            Some(x) => {
                return x.0 >= self.get_capacity();
            }
        }
        */

        return self.num_nodes() >= self.get_capacity();
        //return (self.tail.0 * layout::NODE_SIZE) + layout::PAGE_DATA_START > layout::PAGE_SIZE;

    }

    pub fn get_node_at(&self, offset: NodeOffset) -> Result<InternalNode, String> {
        //println!("CURR TAIL: {:?}", self.tail);

        let tail = match &self.tail {
            None => {return Err("There's no tail value yet?".to_string())},
            Some(x) => x,
        };

        if offset.0 > tail.0 {
            return Err("You're asking too much".to_string());
        }
        //dbg!(&offset);
        let mut start = layout::PAGE_DATA_START;
        start += offset.0 * layout::NODE_SIZE;
        //dbg!(&start);
        let slice = &self.data[start..start + layout::NODE_SIZE];

        Ok(InternalNode::from_slice(slice).unwrap())

    }

    pub fn write_node_at(&mut self, node: InternalNode, offset: NodeOffset) -> Result<(), String> {
        let mut start = layout::PAGE_DATA_START;
        start += offset.0 * layout::NODE_SIZE;
        let slice = &mut self.data[start..start + layout::NODE_SIZE];

        slice.copy_from_slice(&node.to_arr());
        Ok(())
    }

    pub fn get_nodes(&self) -> Vec::<InternalNode> {

        let mut v: Vec::<InternalNode> = Vec::new();

        for offset in 0..1000 {

            let node_result = self.get_node_at(NodeOffset(offset));

            match node_result {

                Ok(record) => v.push(record),
                Err(_) => break,
            }
        }

        return v;

    }



}

#[cfg(test)]
mod tests {
    //use crate::node::CompoundIdentifier;

    use super::*;

    #[test]
    fn quick_new_nodepage_works() {
        let _np = NodePage::new();
    }

    #[test]
    fn nodepage_capacity() {
        let np = NodePage::new();
        dbg!(np.get_capacity());
    }


    #[test]
    fn quick_add_node_trivial_works() {
        let mut np = NodePage::new();
        let node = InternalNode::default();

        assert_eq!(np.num_nodes(), 0);
        np.add_node(&node).unwrap();

        assert_eq!(np.num_nodes(), 1);
        np.add_node(&node).unwrap();
        assert_eq!(np.num_nodes(), 2);
    }

    #[test]
    fn quick_panic_node_full_works() {
        let mut np = NodePage::new();
        let node = InternalNode::default();

        for i in 0..1024 {
            match np.push_node(&node) {

                Ok(_) => {},
                Err(_) => {assert_eq!(i,np.get_capacity()); break;},
            }

        }
    }


    #[test]
    fn quick_test_new_leafpage_works() {
        let _lp = LeafPage::new();
    }

    #[test]
    fn quick_add_compound_record() {
        let mut lp = LeafPage::new();

        let cr = CompoundRecord::default();

        lp.add_record(&cr).unwrap();
        assert_eq!(lp.len(), 1);

        let cr = CompoundRecord::default();
        lp.add_record(&cr).unwrap();
        assert_eq!(lp.len(), 2);

        let cr = CompoundRecord::default();
        lp.add_record(&cr).unwrap();
        let cr = CompoundRecord::default();
        lp.add_record(&cr).unwrap();
        assert_eq!(lp.len(), 4);
    }

    #[test]
    fn quick_test_get_capacity() {

        let mut lp = LeafPage::new();
        dbg!(&lp.get_capacity());
        //dbg!(lp.get_capacity());

        for i in 0..100 {
            let cr = CompoundRecord::default();
            let result = lp.add_record(&cr);
            //dbg!(lp.len(), &result);

            match result {
                Ok(_) => {},
                Err(_) => {

                    assert_eq!(lp.get_capacity(), i);
                    break;
                
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct LeafPage {

    pub data: Box<[u8; layout::PAGE_SIZE]>,
    pub tail: usize,
}

impl Pageable for LeafPage {
    fn get_data(&self) -> &[u8; layout::PAGE_SIZE] {
        return &self.data;
    }

    fn get_type(&self) -> PageType {
        return PageType::Leaf;
    }
}

impl LeafPage {

    pub fn new() -> Self {
        
        let mut s = Self {
            data: Box::new([0; layout::PAGE_SIZE]),
            tail: 0
        };

        s.data[layout::PAGE_TYPE_OFFSET] = PageType::Leaf as u8;
        return s;

    }

    pub fn from_arr(arr: [u8; layout::PAGE_SIZE]) -> Self {

        //read tail from disk
        let tail = arr[layout::TAIL_OFFSET] as usize;

        let page = Self {
            data: Box::new(arr),
            tail,
        };

        return page;


        /*

        let page = Self {
            data: Box::new(arr),
            tail: NodeOffset(tail),
        };

        return page
        */

    }

    pub fn descriptor_in_page(&self, query_desc: &Descriptor) -> bool {

        for record in self.get_records() {
            if record.descriptor == *query_desc {
                return true;
            }
        }


        return false;

    }


    pub fn get_records(&self) -> Vec::<CompoundRecord> {

        let mut v: Vec::<CompoundRecord> = Vec::new();

        for offset in 0..200 {

            let record_result = self.get_record_at(offset);

            match record_result {

                Ok(record) => v.push(record),
                Err(_) => break,
            }
        }

        return v;

    }


    pub fn add_record(&mut self, record: &CompoundRecord) -> Result<(), String> {

        match self.is_full() {
            true => return Err("Node is full".to_string()),
            false => {},
        }

        let start = layout::PAGE_DATA_START + (self.tail * layout::COMPOUND_RECORD_SIZE);
        let size = layout::COMPOUND_RECORD_SIZE;

        let slice = &mut self.data[start..start + size];

        slice.copy_from_slice(&record.to_arr());
        self.tail += 1;

        //ensure tail value is updated
        self.data[layout::TAIL_OFFSET] = self.tail as u8;
        
        Ok(())
    }

    pub fn get_record_at(&self, offset: usize) -> Result<CompoundRecord, String> {

        if offset >= self.tail {
            return Err("Provided offset greater than number of nodes".to_string());
        }
        else {
            let start = layout::PAGE_DATA_START + (offset * layout::COMPOUND_RECORD_SIZE);
            let size = layout::COMPOUND_RECORD_SIZE;
            let slice = &self.data[start..start + size];

            let cr = CompoundRecord::from_slice(slice);
            return cr;
        }


    }

    pub fn is_full(&self) -> bool {

        return self.tail >= self.get_capacity();

    }

    pub fn get_capacity(&self) -> usize {

        return (layout::PAGE_SIZE - layout::PAGE_DATA_START) / layout::COMPOUND_RECORD_SIZE;

    }

    pub fn len(&self) -> usize {
        return self.tail;
    }


}







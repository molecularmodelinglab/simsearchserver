//! A page is a byte array that is either a collection of internal nodes or a single leaf node
//! that is a collection of compound records.
//!
//!

use crate::node::{ItemOffset, InternalNode, CompoundRecord, Descriptor};
use crate::layout;

#[derive(Debug, Clone, PartialEq)]
pub enum PageType {
    Node = 1,
    Leaf = 2,
}

/*
pub trait Pageable {
    fn get_data(&self) -> &[u8; layout::PAGE_SIZE];
    fn get_type(&self) -> PageType;
}
*/

#[derive(Debug)]
pub struct NodePage {

    //pub data: Box<[u8; layout::NODE_PAGE_SIZE]>,
    pub data: Vec<u8>,
    pub tail: Option<ItemOffset>,
    pub page_length: usize,
}

//TODO: strip const generic? why does it care about descriptor length?
impl NodePage {

    pub fn new(page_length: usize) -> Self {

        //let mut arr: [u8; layout::NODE_PAGE_SIZE] = [0; layout::NODE_PAGE_SIZE];
        let mut arr = vec![0u8; page_length];

        arr[layout::PAGE_TYPE_OFFSET] = PageType::Node as u8;
        arr[layout::IS_EMPTY_OFFSET] = 1 as u8;

        //dbg!(arr[layout::PAGE_TYPE_OFFSET]);
        let page = Self {
            data: arr,
            tail: None,
            page_length,
        };

        return page

    }

    pub fn get_data(&self) -> &[u8] {
        &self.data
    }

    pub fn from_arr(arr: &[u8], page_length: usize) -> Self {

        let tail = i32::from_be_bytes(arr[layout::TAIL_OFFSET..layout::TAIL_OFFSET+layout::TAIL_SIZE].try_into().unwrap()) as usize;

        let mut vec = vec![0u8; page_length];
        vec.copy_from_slice(arr);

        //read empty from disk
        let is_empty = arr[layout::IS_EMPTY_OFFSET] as usize;

        let tail_val = match is_empty {
            1 => None,
            2 => Some(ItemOffset(tail as u32)),
            _ => {println!("BAD EMPTY VAL: {:?}", is_empty);
                    dbg!(arr);
                panic!();},
        };

        let page = Self {
            data: vec,
            tail: tail_val,
            page_length,
        };

        return page

    }

    pub fn get_capacity(&self) -> usize {

        //return (layout::NODE_PAGE_SIZE - layout::PAGE_DATA_START) / layout::NODE_SIZE;
        return (self.page_length - layout::PAGE_DATA_START) / layout::NODE_SIZE;

    }

    pub fn num_nodes(&self) -> usize {

        match &self.tail {
            None => {return 0;},
            Some(x) => {return x.0 as usize + 1},
        }
        /*
        match self.tail.0 {
            0 => return 0,
            _ => return self.tail.0 / layout::NODE_SIZE,
        }
        */

    }

    pub fn add_node(&mut self, node: &InternalNode) -> Result<ItemOffset, String> {

        match self.push_node(node) {
            Ok(_) => {return Ok(ItemOffset(self.tail.clone().unwrap().0)) },
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


        let start = layout::PAGE_DATA_START + (count as usize * layout::NODE_SIZE);
        let slice = &mut self.data[start..start + layout::NODE_SIZE];

        slice.copy_from_slice(&node.to_arr());

        //println!("TAIL STARTS AS: {:?}", self.tail);
        self.tail = match &self.tail {
            None => Some(ItemOffset(0)),
            Some(x) => Some(ItemOffset(x.0 + 1)),
        };

        //dbg!(&self.tail);

        //ensure tail value is updated
        //self.data[layout::TAIL_OFFSET] = self.tail.clone().unwrap().0 as u8;
        self.data[layout::TAIL_OFFSET..layout::TAIL_OFFSET + layout::TAIL_SIZE].copy_from_slice(&self.tail.as_ref().unwrap().0.to_be_bytes());

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

    pub fn get_node_at(&self, offset: &ItemOffset) -> Result<InternalNode, String> {

        //dbg!(&self.get_data()[0..10]);
        //println!("CURR TAIL: {:?}", self.tail);

        let tail = match &self.tail {
            None => {return Err("There's no tail value yet?".to_string())},
            Some(x) => x,
        };

        //dbg!(&offset);
        //dbg!(&tail);
        if offset.0 > tail.0 {
            return Err("You're asking too much".to_string());
        }
        //dbg!(&offset);
        let mut start = layout::PAGE_DATA_START;
        start += offset.0 as usize * layout::NODE_SIZE;
        //dbg!(layout::NODE_SIZE);
        //dbg!(&start);
        let slice = &self.data[start..start + layout::NODE_SIZE];

        Ok(InternalNode::from_slice(slice).unwrap())

    }

    pub fn write_node_at(&mut self, node: InternalNode, offset: ItemOffset) -> Result<(), String> {
        let mut start = layout::PAGE_DATA_START;
        start += offset.0 as usize * layout::NODE_SIZE;
        let slice = &mut self.data[start..start + layout::NODE_SIZE];

        slice.copy_from_slice(&node.to_arr());
        Ok(())
    }

    pub fn get_nodes(&self) -> Vec::<InternalNode> {

        //let mut v: Vec::<InternalNode> = Vec::new();
        let mut v: Vec::<InternalNode> = Vec::with_capacity(self.get_capacity());

        for offset in 0..10000 {

            let node_result = self.get_node_at(&ItemOffset(offset));

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
        let _np = NodePage::new(4096);
    }

    #[test]
    fn nodepage_capacity() {
        let np = NodePage::new(4096);
        dbg!(np.get_capacity());
    }


    #[test]
    fn quick_add_node_trivial_works() {
        let mut np = NodePage::new(4096);
        let node = InternalNode::default();

        assert_eq!(np.num_nodes(), 0);
        np.add_node(&node).unwrap();

        assert_eq!(np.num_nodes(), 1);
        np.add_node(&node).unwrap();
        assert_eq!(np.num_nodes(), 2);
    }

    #[test]
    fn quick_panic_node_full_works() {
        let mut np = NodePage::new(4096);
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
        let n = 8;
        let record_page_length = 4096;
        let _lp = RecordPage::new(record_page_length, n);
    }

    #[test]
    fn quick_add_compound_record() {
        let n = 8;
        let record_page_length = 4096;
        let mut lp = RecordPage::new(record_page_length, n);

        dbg!(&lp);

        let cr = CompoundRecord::default(n);

        lp.add_record(&cr).unwrap();
        assert_eq!(lp.len(), 1);

        let cr = CompoundRecord::default(n);
        lp.add_record(&cr).unwrap();
        assert_eq!(lp.len(), 2);

        let cr = CompoundRecord::default(n);
        lp.add_record(&cr).unwrap();
        let cr = CompoundRecord::default(n);
        lp.add_record(&cr).unwrap();
        assert_eq!(lp.len(), 4);
    }

    #[test]
    fn quick_test_get_capacity() {

        let n = 8;
        let record_page_length = 4096;
        let mut lp = RecordPage::new(record_page_length, n);
        dbg!(&lp.get_capacity());

        for i in 0..100 {
            let cr = CompoundRecord::default(n);
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
pub struct RecordPage {

    //pub data: Box<[u8; layout::RECORD_PAGE_SIZE]>,
    pub data: Vec<u8>,
    pub tail: Option<ItemOffset>,
    pub desc_length: usize,
    pub page_length: usize,
}

impl RecordPage {

    pub fn new(page_length: usize, desc_length: usize) -> Self {
        
        let mut s = Self {
            //data: Box::new([0; layout::RECORD_PAGE_SIZE]),
            //data: Vec::with_capacity(length),
            data: vec![0u8; page_length],
            tail: Some(ItemOffset(0)),
            desc_length,
            page_length,
        };

        s.data[layout::PAGE_TYPE_OFFSET] = PageType::Leaf as u8;

        /*
        dbg!(s.desc_length);
        dbg!(CompoundRecord::compute_record_size(s.desc_length));
        dbg!(s.get_capacity());
        */

        return s;

    }

    //pub fn get_data(&self) -> &[u8; layout::RECORD_PAGE_SIZE] {
    pub fn get_data(&self) -> &Vec<u8> {
        &self.data
    }

    pub fn from_arr(arr: &[u8], page_length: usize, desc_length: usize) -> Self {

        //read tail from disk
        //let tail = arr[layout::TAIL_OFFSET] as usize;

        let tail = Some(ItemOffset(u32::from_be_bytes(arr[layout::TAIL_OFFSET..layout::TAIL_OFFSET+layout::TAIL_SIZE].try_into().unwrap())));

        //let mut vec = Vec::with_capacity(length);
        let mut vec = vec![0u8; page_length];
        vec.copy_from_slice(arr);

        let page = Self {
            data: vec,
            tail,
            desc_length,
            page_length,
        };
        return page;
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

        //let mut v: Vec::<CompoundRecord> = Vec::new();
        let mut v: Vec::<CompoundRecord> = Vec::with_capacity(self.get_capacity());

        for offset in 0..10000 {

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

        let start = layout::PAGE_DATA_START + (self.tail.as_ref().unwrap().0 as usize * record.get_record_size());
        let size = record.get_record_size();

        let slice = &mut self.data[start..start + size];

        //println!("TAIL BEFORE ADD: {}", self.tail.as_ref().unwrap().0);
        //slice.copy_from_slice(&record.to_arr());
        slice.copy_from_slice(&record.to_vec());
        self.tail.as_mut().unwrap().0 += 1;

        //ensure tail value is updated
        self.data[layout::TAIL_OFFSET..layout::TAIL_OFFSET + layout::TAIL_SIZE].copy_from_slice(&self.tail.as_ref().unwrap().0.to_be_bytes());

        //println!("TAIL AFTER ADD: {}", self.tail.as_ref().unwrap().0);
        //println!("CAPACITY: {}", self.get_capacity());
        //let tail = i32::from_be_bytes(arr[layout::TAIL_OFFSET..layout::TAIL_OFFSET+4].try_into().unwrap()) as usize;
        
        Ok(())
    }

    pub fn get_record_at(&self, offset: usize) -> Result<CompoundRecord, String> {

        if offset >= self.tail.as_ref().unwrap().0 as usize {
            return Err("Provided offset greater than number of nodes".to_string());
        }
        else {
            let start = layout::PAGE_DATA_START + (offset * CompoundRecord::compute_record_size(self.desc_length)); 
            let size = CompoundRecord::compute_record_size(self.desc_length);
            let slice = &self.data[start..start + size];

            let cr = CompoundRecord::from_slice(slice, self.desc_length);
            return cr;
        }


    }

    pub fn is_full(&self) -> bool {

        return self.tail.as_ref().unwrap().0 as usize >= self.get_capacity();

    }

    pub fn get_capacity(&self) -> usize {

        //return (layout::RECORD_PAGE_SIZE - layout::PAGE_DATA_START) / CompoundRecord::compute_record_size(self.desc_length);
        return (self.page_length - layout::PAGE_DATA_START) / CompoundRecord::compute_record_size(self.desc_length);

    }

    pub fn len(&self) -> usize {
        return self.tail.as_ref().unwrap().0 as usize;
    }


}







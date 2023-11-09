//! A page is a byte array that is either a collection of internal nodes or a single leaf node
//! that is a collection of compound records.
//!
//!

use crate::tree::{TreeRecord};
use crate::data::{Descriptor};
use crate::layout;

#[derive(Debug, Clone, PartialEq)]
pub enum PageType {
    Node = 1,
    Leaf = 2,
}

#[derive(Debug, Clone)]
pub struct RecordPage {

    pub data: Vec<u8>,
    pub tail: Option<usize>,
    pub desc_length: usize,
    pub page_length: usize,
}

impl RecordPage {

    pub fn new(page_length: usize, desc_length: usize) -> Self {
        
        let mut s = Self {
            data: vec![0u8; page_length],
            tail: Some(0),
            desc_length,
            page_length,
        };

        s.data[layout::PAGE_TYPE_OFFSET] = PageType::Leaf as u8;

        return s;
    }

    pub fn get_data(&self) -> &Vec<u8> {
        &self.data
    }

    pub fn from_arr(arr: &[u8], page_length: usize, desc_length: usize) -> Self {

        let tail = Some(u32::from_be_bytes(arr[layout::TAIL_OFFSET..layout::TAIL_OFFSET+layout::TAIL_SIZE].try_into().unwrap()) as usize);

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


    pub fn get_records(&self) -> Vec::<TreeRecord> {

        let mut v: Vec::<TreeRecord> = Vec::with_capacity(self.get_capacity());

        for offset in 0..10000 {

            let record_result = self.get_record_at(offset);

            match record_result {

                Ok(record) => v.push(record),
                Err(_) => break,
            }
        }

        return v;
    }


    pub fn add_record(&mut self, record: &TreeRecord) -> Result<(), String> {

        //dbg!("ADD CHECK");
        match self.is_full() {
            true => {return Err("Record page is full".to_string())},
            false => {},
        }

        let start = layout::PAGE_DATA_START + (self.tail.unwrap() * record.get_record_size());
        let size = record.get_record_size();

        let slice = &mut self.data[start..start + size];

        slice.copy_from_slice(&record.to_vec());
        self.tail = match self.tail {
            Some(x) => Some(x + 1),
            None => Some(0),
        };

        let coerced_tail: u32 = self.tail.unwrap().try_into().unwrap();

        //ensure tail value is updated
        self.data[layout::TAIL_OFFSET..layout::TAIL_OFFSET + layout::TAIL_SIZE].copy_from_slice(&coerced_tail.to_be_bytes());

        
        Ok(())
    }

    pub fn get_record_at(&self, offset: usize) -> Result<TreeRecord, String> {

        if offset >= self.tail.unwrap() {
            return Err("Provided offset greater than number of nodes".to_string());
        }
        else {
            let start = layout::PAGE_DATA_START + (offset * TreeRecord::compute_record_size(self.desc_length)); 
            let size = TreeRecord::compute_record_size(self.desc_length);
            let slice = &self.data[start..start + size];

            let cr = TreeRecord::from_slice(slice, self.desc_length);
            return cr;
        }


    }

    pub fn is_full(&self) -> bool {
        //println!("tail: {}, capacity: {}", self.tail.unwrap(), self.get_capacity());

        return self.tail.unwrap() as usize >= self.get_capacity();

    }

    pub fn get_capacity(&self) -> usize {

        return (self.page_length - layout::PAGE_DATA_START) / TreeRecord::compute_record_size(self.desc_length);

    }

    pub fn len(&self) -> usize {
        return self.tail.unwrap() as usize;
    }
}

#[cfg(test)]
mod tests {

    use super::*;

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

        let cr = TreeRecord::default(n);

        lp.add_record(&cr).unwrap();
        assert_eq!(lp.len(), 1);

        let cr = TreeRecord::default(n);
        lp.add_record(&cr).unwrap();
        assert_eq!(lp.len(), 2);

        let cr = TreeRecord::default(n);
        lp.add_record(&cr).unwrap();
        let cr = TreeRecord::default(n);
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
            let cr = TreeRecord::default(n);
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








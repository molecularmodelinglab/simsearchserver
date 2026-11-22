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

// Byte-level accessors, shared by the owned `RecordPage` and the borrowed
// `RecordPageRef`. Everything here works directly on page bytes so the query
// path can scan a page without allocating.

/// Number of records written to this page
#[inline]
pub fn page_tail(data: &[u8]) -> usize {
    let bytes = data[layout::TAIL_OFFSET..layout::TAIL_OFFSET + layout::TAIL_SIZE]
        .try_into()
        .expect("tail field is TAIL_SIZE bytes");

    return u32::from_be_bytes(bytes) as usize;
}

/// Byte range of the record at `offset` within the page
#[inline]
pub fn record_slice(data: &[u8], offset: usize, desc_length: usize) -> &[u8] {
    let size = TreeRecord::compute_record_size(desc_length);
    let start = layout::PAGE_DATA_START + (offset * size);

    return &data[start..start + size];
}

/// Squared Euclidean distance between `query` and the descriptor stored in
/// `record`, read straight from the page bytes
#[inline]
pub fn squared_distance(record: &[u8], query: &Descriptor) -> f32 {
    let mut sum = 0.0f32;

    for i in 0..query.length {
        let at = layout::DESCRIPTOR_START + i * 4;
        let bytes = record[at..at + 4].try_into().expect("descriptor component is 4 bytes");
        let value = f32::from_be_bytes(bytes);
        let delta = query.data[i] - value;
        sum += delta * delta;
    }

    return sum;
}

/// Read-only view over a leaf page that borrows its bytes
pub struct RecordPageRef<'a> {
    data: &'a [u8],
    tail: usize,
    desc_length: usize,
}

impl<'a> RecordPageRef<'a> {

    pub fn from_slice(data: &'a [u8], desc_length: usize) -> Self {
        return Self { tail: page_tail(data), data, desc_length };
    }

    pub fn len(&self) -> usize {
        return self.tail;
    }

    #[inline]
    pub fn squared_distance_at(&self, offset: usize, query: &Descriptor) -> f32 {
        return squared_distance(record_slice(self.data, offset, self.desc_length), query);
    }

    /// Materializes a record. Called only for the few that make the top-k.
    pub fn get_record_at(&self, offset: usize) -> Result<TreeRecord, String> {

        if offset >= self.tail {
            return Err("Provided offset greater than number of records".to_string());
        }

        return TreeRecord::from_slice(record_slice(self.data, offset, self.desc_length), self.desc_length);
    }
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

    /// Takes ownership of a buffer already holding the page.
    pub fn from_vec(data: Vec<u8>, page_length: usize, desc_length: usize) -> Self {

        let tail = Some(page_tail(&data));

        return Self {
            data,
            tail,
            desc_length,
            page_length,
        };
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

        // Written straight into the page
        record.write_to(&mut self.data[start..start + size]);

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

        return TreeRecord::from_slice(record_slice(&self.data, offset, self.desc_length), self.desc_length);
    }

    /// Squared distance to the record at `offset`, without materializing it.
    #[inline]
    pub fn squared_distance_at(&self, offset: usize, query: &Descriptor) -> f32 {
        return squared_distance(record_slice(&self.data, offset, self.desc_length), query);
    }

    /// One descriptor component of the record at `offset`
    #[inline]
    pub fn descriptor_component_at(&self, offset: usize, axis: usize) -> f32 {
        let record = record_slice(&self.data, offset, self.desc_length);
        let at = layout::DESCRIPTOR_START + axis * 4;
        let bytes = record[at..at + 4].try_into().expect("component is 4 bytes");

        return f32::from_be_bytes(bytes);
    }

    /// Appends the record at `offset` in `source`, copying its bytes directly
    pub fn copy_record_from(&mut self, source: &RecordPage, offset: usize) -> Result<(), String> {

        if self.is_full() {
            return Err("Record page is full".to_string());
        }

        assert!(source.desc_length == self.desc_length);

        let size = TreeRecord::compute_record_size(self.desc_length);
        let start = layout::PAGE_DATA_START + (self.tail.unwrap() * size);

        let bytes = record_slice(&source.data, offset, source.desc_length);
        self.data[start..start + size].copy_from_slice(bytes);

        self.tail = Some(self.tail.unwrap() + 1);

        let coerced_tail: u32 = self.tail.unwrap().try_into().unwrap();
        self.data[layout::TAIL_OFFSET..layout::TAIL_OFFSET + layout::TAIL_SIZE]
            .copy_from_slice(&coerced_tail.to_be_bytes());

        return Ok(());
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








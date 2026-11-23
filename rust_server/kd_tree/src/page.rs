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
    let start = layout::page_data_start(desc_length) + (offset * size);

    return &data[start..start + size];
}

#[inline]
fn bounds_at(data: &[u8], axis: usize) -> (f32, f32) {
    let at = layout::BOUNDS_OFFSET + axis * 8;
    let min = f32::from_le_bytes(data[at..at + 4].try_into().expect("bound is 4 bytes"));
    let max = f32::from_le_bytes(data[at + 4..at + 8].try_into().expect("bound is 4 bytes"));

    return (min, max);
}

/// Squared distance from `query` to the nearest point of the page's bounding
/// box: zero inside it, and a lower bound on the distance to every record on
/// the page. An empty page has an inverted box and so reports infinity.
#[inline]
pub fn squared_distance_to_bounds(data: &[u8], query: &Descriptor) -> f32 {

    let mut sum = 0.0f32;

    for axis in 0..query.length {
        let (min, max) = bounds_at(data, axis);
        let q = query.data[axis];

        let delta = if q < min { min - q } else if q > max { q - max } else { 0.0 };

        sum += delta * delta;
    }

    return sum;
}

fn write_bounds(data: &mut [u8], axis: usize, min: f32, max: f32) {
    let at = layout::BOUNDS_OFFSET + axis * 8;
    data[at..at + 4].copy_from_slice(&min.to_le_bytes());
    data[at + 4..at + 8].copy_from_slice(&max.to_le_bytes());
}

/// Widens the page's box to contain `descriptor`.
fn extend_bounds(data: &mut [u8], descriptor: &[f32]) {

    for (axis, value) in descriptor.iter().enumerate() {
        let (min, max) = bounds_at(data, axis);
        write_bounds(data, axis, min.min(*value), max.max(*value));
    }
}

/// Squared Euclidean distance between `query` and the descriptor stored in
/// `record`, read straight from the page bytes
#[inline]
pub fn squared_distance(record: &[u8], query: &Descriptor) -> f32 {

    // Sliced once, then walked in fixed 4-byte chunks zipped against the query.
    // Indexing per component instead (`record[at..at + 4]`, `query.data[i]`)
    // emits a bounds check per component per record
    let start = layout::DESCRIPTOR_START;
    let bytes = &record[start..start + query.length * 4];

    // Four independent accumulators, four components at a time.
    let mut acc = [0.0f32; 4];

    let mut values = bytes.chunks_exact(16);
    let mut queries = query.data.chunks_exact(4);

    for (v, q) in values.by_ref().zip(queries.by_ref()) {

        let a = f32::from_le_bytes([v[0], v[1], v[2], v[3]]);
        let b = f32::from_le_bytes([v[4], v[5], v[6], v[7]]);
        let c = f32::from_le_bytes([v[8], v[9], v[10], v[11]]);
        let d = f32::from_le_bytes([v[12], v[13], v[14], v[15]]);

        let da = q[0] - a;
        let db = q[1] - b;
        let dc = q[2] - c;
        let dd = q[3] - d;

        acc[0] += da * da;
        acc[1] += db * db;
        acc[2] += dc * dc;
        acc[3] += dd * dd;
    }

    let mut sum = acc[0] + acc[1] + acc[2] + acc[3];

    // Descriptor lengths that are not a multiple of four finish here.
    for (v, q) in values.remainder().chunks_exact(4).zip(queries.remainder().iter()) {
        let value = f32::from_le_bytes(v.try_into().expect("chunks_exact(4) yields 4 bytes"));
        let delta = q - value;
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

    #[inline]
    pub fn squared_distance_to_bounds(&self, query: &Descriptor) -> f32 {
        return squared_distance_to_bounds(self.data, query);
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

        // Inverted, so the first record written sets both ends.
        for axis in 0..desc_length {
            write_bounds(&mut s.data, axis, f32::INFINITY, f32::NEG_INFINITY);
        }

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

        let start = layout::page_data_start(self.desc_length) + (self.tail.unwrap() * record.get_record_size());
        let size = record.get_record_size();

        // Written straight into the page
        record.write_to(&mut self.data[start..start + size]);

        extend_bounds(&mut self.data, &record.descriptor.data);

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

    #[inline]
    pub fn squared_distance_to_bounds(&self, query: &Descriptor) -> f32 {
        return squared_distance_to_bounds(&self.data, query);
    }

    /// One descriptor component of the record at `offset`
    #[inline]
    pub fn descriptor_component_at(&self, offset: usize, axis: usize) -> f32 {
        let record = record_slice(&self.data, offset, self.desc_length);
        let at = layout::DESCRIPTOR_START + axis * 4;
        let bytes = record[at..at + 4].try_into().expect("component is 4 bytes");

        return f32::from_le_bytes(bytes);
    }

    /// Appends the record at `offset` in `source`, copying its bytes directly
    pub fn copy_record_from(&mut self, source: &RecordPage, offset: usize) -> Result<(), String> {

        if self.is_full() {
            return Err("Record page is full".to_string());
        }

        assert!(source.desc_length == self.desc_length);

        let size = TreeRecord::compute_record_size(self.desc_length);
        let start = layout::page_data_start(self.desc_length) + (self.tail.unwrap() * size);

        let bytes = record_slice(&source.data, offset, source.desc_length);
        self.data[start..start + size].copy_from_slice(bytes);

        let descriptor: Vec<f32> = (0..self.desc_length)
            .map(|axis| source.descriptor_component_at(offset, axis))
            .collect();

        extend_bounds(&mut self.data, &descriptor);

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

        return (self.page_length - layout::page_data_start(self.desc_length)) / TreeRecord::compute_record_size(self.desc_length);

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
    fn bounds_contain_every_record_on_the_page() {

        let n = 8;
        let mut page = RecordPage::new(4096, n);

        assert_eq!(page.squared_distance_to_bounds(&Descriptor::random(n)), f32::INFINITY,
                   "an empty page must bound nothing");

        let mut records: Vec<TreeRecord> = Vec::new();
        for _ in 0..20 {
            let record = TreeRecord { index: 0, descriptor: Descriptor::random(n), length: n };
            page.add_record(&record).unwrap();
            records.push(record);
        }

        for record in &records {
            assert_eq!(page.squared_distance_to_bounds(&record.descriptor), 0.0,
                       "a stored descriptor must lie inside the box");
        }

        for _ in 0..50 {
            let query = Descriptor::random(n);
            let bound = page.squared_distance_to_bounds(&query);

            let nearest = (0..page.len())
                .map(|offset| page.squared_distance_at(offset, &query))
                .fold(f32::INFINITY, f32::min);

            assert!(bound <= nearest + 1e-6,
                    "bound {} exceeded the nearest record at {}", bound, nearest);
        }
    }

    #[test]
    fn copied_records_extend_the_bounds() {

        let n = 8;
        let mut source = RecordPage::new(4096, n);

        for _ in 0..10 {
            source.add_record(&TreeRecord {
                index: 0, descriptor: Descriptor::random(n), length: n,
            }).unwrap();
        }

        let mut target = RecordPage::new(4096, n);
        for offset in 0..source.len() {
            target.copy_record_from(&source, offset).unwrap();
        }

        for offset in 0..target.len() {
            let descriptor = Descriptor {
                data: (0..n).map(|axis| target.descriptor_component_at(offset, axis)).collect(),
                length: n,
            };
            assert_eq!(target.squared_distance_to_bounds(&descriptor), 0.0);
        }
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








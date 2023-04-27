//! Handles read and write for whole pages on disk
//!
//!

use crate::error::Error;
use crate::node::{CompoundIdentifier, InternalNode, PagePointer};
use crate::page::RecordPage;
use byteorder::{ByteOrder, BigEndian};
use crate::layout;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

#[derive(Debug)]
pub struct RecordPager {
    file: File,
    pub cursor: usize, //this is the next available slot
    pub desc_length: usize,
    pub page_length: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::InternalNode;
    
    #[test]
    fn quick_nodes_to_file_and_back_works() {


        for num_nodes in [1, 10, 100, 1000, 1234] {

            for _ in 0..10 {

                let mut pager = FastNodePager::new();

                for i in 0..num_nodes {
                    let mut node = InternalNode::default();
                    node.split_value = i as f32;
                    pager.add_node(&node).unwrap();
                }
                
                let filename = "test_data/node".to_string();
                pager.to_file(&filename).unwrap();
                pager = FastNodePager::from_file(&filename).unwrap();

                assert_eq!(pager.store.len(), num_nodes);
            }
        }

    }
}

#[derive(Debug)]
pub struct FastNodePager {
    pub store: Vec<InternalNode>,

}

impl FastNodePager {

    pub fn new() -> FastNodePager {

        return Self {
            store: Vec::new(),
        };

    }

    pub fn len(&self) -> usize {
        return self.store.len();
    }

    fn calc_offset(index: usize) -> usize {

        return layout::FILE_DATA_START + (index * layout::NODE_SIZE);

    }

    pub fn to_file(&self, filename: &String) -> Result<(), Error> {

        let path = Path::new(filename);

        let mut fd = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(true)
                    .open(path).unwrap();

        for i in 0..self.store.len() {
            let node = self.store.get(i).unwrap();
            let start = Self::calc_offset(i);
            let slice = node.to_arr();

            fd.seek(SeekFrom::Start(start as u64))?;
            fd.write(&slice).unwrap();
            //println!("W {:?} {:?}", i, start);
            //println!("\t{:?}", node);
        }

        let cursor = self.store.len() - 1;
        //update cursor on disk
        let mut cursor_arr: [u8; layout::HEADER_CURSOR_SIZE] = [0x00; layout::HEADER_CURSOR_SIZE];

        BigEndian::write_u64(&mut cursor_arr, cursor as u64);

        fd.seek(SeekFrom::Start(layout::HEADER_CURSOR_START as u64))?;
        fd.write(&cursor_arr)?;

        Ok(())

    }

    pub fn from_file(filename: &String) -> Result<FastNodePager, Error> {

        let path = Path::new(filename);

        let mut fd = OpenOptions::new()
                    .create(false)
                    .read(true)
                    .write(false)
                    .truncate(false)
                    .open(path)?;

        let mut cursor_arr: [u8; layout::HEADER_CURSOR_SIZE] = [0x00; layout::HEADER_CURSOR_SIZE];
        fd.seek(SeekFrom::Start(layout::HEADER_CURSOR_START as u64))?;
        fd.read_exact(&mut cursor_arr)?;

        let mut pager = Self::new();

        let attempted_usize = layout::Value::try_from(cursor_arr);
        let layout::Value(value) = attempted_usize.unwrap();
        
        for i in 0..(value + 1) {
            let mut node_arr: [u8; layout::NODE_SIZE] = [0x00; layout::NODE_SIZE];
            let start = Self::calc_offset(i);
            //println!("R {:?} {:?}", i, start);
            fd.seek(SeekFrom::Start(start as u64))?;
            fd.read_exact(&mut node_arr)?;
            let node = InternalNode::from_slice(&node_arr).unwrap();
            pager.store.push(node);

        }


        
        Ok(pager)

    }


    pub fn num_nodes(&self) -> usize {
        return self.store.len();
    }

    //TODO: return only node references, user can clone if they need to
    pub fn get_node(&mut self, index: &usize) -> Result<&InternalNode, String> {

        //let node = self.map.get(&pointer.to_tuple()).unwrap();
        let ret = match self.store.get(index.clone()) {
            Some(node) => Ok(node),
            None => Err(format!("Node not found at address: {:?}", index)),
        };

        return ret;
    }

    pub fn add_node(&mut self, node: &InternalNode) -> Result<PagePointer, Error> {

        //self.map.insert(self.next_pointer.to_tuple(), node.clone());
        self.store.push(node.clone());

        let added_index = self.store.len() - 1;

        Ok(PagePointer::Node(added_index))
    }

    pub fn update_node(&mut self, index: &usize, new_node: &InternalNode) -> Result<(), Error> {

        //self.map.insert(pointer.to_tuple(), new_node.clone());
        self.store[index.clone()] = new_node.clone();
        //println!("UPDATED NODE ADDRESS: {:?}", pointer.to_tuple());

        return Ok(())
    }
}


impl RecordPager {

    pub fn new(path: &Path, page_length: usize, desc_length: usize, create: bool) -> Result<Self, Error> {

        match create {
            true => {
                let fd = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(true)
                    .open(path)?;

                return Ok(Self {
                    file: fd,
                    cursor: 0,
                    desc_length,
                    page_length,
                })
            },
            false => {
                let mut fd = OpenOptions::new()
                    .create(false)
                    .read(true)
                    .write(false)
                    .truncate(false)
                    .open(path)?;

                    //fn coerce_pointer(value: &[u8]) -> [u8; layout::PTR_SIZE] {
                    //    value.try_into().expect("slice with incorrect length")
                    //}
                    let mut cursor_from_disk: [u8; layout::HEADER_CURSOR_SIZE] = [0x00; layout::HEADER_CURSOR_SIZE];
                    fd.seek(SeekFrom::Start(layout::HEADER_CURSOR_START as u64))?;
                    fd.read_exact(&mut cursor_from_disk)?;

                    let attempted_usize = layout::Value::try_from(cursor_from_disk);
                    let layout::Value(value) = attempted_usize.unwrap();
                    
                    return Ok(Self {
                        file: fd,
                        cursor: value,
                        desc_length,
                        page_length,
                    })

            }
        }

    }


    pub fn calc_offset(&self, address: &usize) -> u64 {

        //let offset = (address.0 as usize * layout::RECORD_PAGE_SIZE) as u64;
        let offset = (address * self.page_length) as u64;
        return layout::FILE_DATA_START as u64 + offset;

    }

    pub fn get_record_page(&mut self, address: &usize) -> Result<RecordPage, Error> {

        //let mut page: [u8; self.length] = [0x00; self.length];
        let mut page: Vec<u8>  = vec![0; self.page_length];

        let start = self.calc_offset(address);
        self.file.seek(SeekFrom::Start(start))?;

        match self.file.read_exact(&mut page) {
            Ok(_) => {},
            Err(_) => {
                println!("Failed to read record page at {:?}", &address);
            }

        };

        let page = RecordPage::from_arr(&page, self.page_length, self.desc_length);

        return Ok(page);
    }

    pub fn print_records(&mut self) {

        let mut records: Vec<(CompoundIdentifier, usize)> = Vec::new();
        let mut curr_address = 0;

        loop {

            if curr_address > self.cursor {
                break
            }

            let page: RecordPage = self.get_record_page(&curr_address).unwrap();

            let this_records = page.get_records();

            let this_amended_records: Vec<_> = this_records.into_iter().map(|x| (x.compound_identifier, curr_address)).collect();

            records.extend(this_amended_records);

            curr_address += 1;


        }

        for record in records.iter() {
            println!("DEBUG RECORD: {:?}", record);
        }

        println!("NUMBER OF RECORDS FOUND: {:?}", records.len());
        println!("LAST RECORD FOUND: {:?}", records[records.len() - 1]);

    }

    pub fn len(&self) -> usize {
        return self.cursor;
    }

    pub fn write_page(&mut self, page: &RecordPage) -> Result<PagePointer, Error> {

        let start = self.calc_offset(&self.cursor);
        self.file.seek(SeekFrom::Start(start))?;

        let data = page.get_data();

        self.file.write(data)?;

        let res = self.cursor.clone();
        self.cursor += 1;
        
        //update cursor on disk
        let mut cursor_arr: [u8; layout::HEADER_CURSOR_SIZE] = [0x00; layout::HEADER_CURSOR_SIZE];

        BigEndian::write_u64(&mut cursor_arr, self.cursor as u64);

        self.file.seek(SeekFrom::Start(layout::HEADER_CURSOR_START as u64))?;
        self.file.write(&cursor_arr)?;

        Ok(PagePointer::Leaf(res))
    }

    pub fn write_page_at_offset(&mut self, page: &RecordPage, address: &usize) -> Result<(), Error> {
        let start = self.calc_offset(address);
        self.file.seek(SeekFrom::Start(start))?;

        let data = page.get_data();

        self.file.write(data)?;
        //let res = self.cursor.clone();

        Ok(())
    }

}

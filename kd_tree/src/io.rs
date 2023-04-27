//! Handles read and write for whole pages on disk
//!
//!

use crate::error::Error;
//use crate::node::{PageAddress, InternalNode, ItemOffset, Descriptor, CompoundRecord, CompoundIdentifier};
use crate::node::{PageAddress, CompoundIdentifier, InternalNode, PagePointer};
use crate::page::RecordPage;
use byteorder::{ByteOrder, BigEndian};
use crate::layout;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
//use std::collections::HashMap;

//use std::fmt;


/*
#[derive(Debug)]
pub struct NodePager {
    file: File,
    pub cursor: usize, //this is the next available slot
    pub page_length: usize,
}
*/

#[derive(Debug)]
pub struct RecordPager {
    file: File,
    pub cursor: usize, //this is the next available slot
    pub desc_length: usize,
    pub page_length: usize,
}
/*
impl NodePager {

    pub fn new(path: &Path, page_length: usize, create: bool) -> Result<Self, Error> {

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
                    cursor: PageAddress(0),
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
                        cursor: PageAddress(value),
                        page_length,
                    })

            }
        }

    }

    pub fn calc_offset(&self, address: &PageAddress) -> u64 {

        let offset = (address.0 * self.page_length) as u64;
        return layout::FILE_DATA_START as u64 + offset;

    }

    pub fn get_node_page(&mut self, address: &PageAddress) -> Result<NodePage, Error> {

        //let mut page: [u8; layout::NODE_PAGE_SIZE] = [0x00; layout::NODE_PAGE_SIZE];
        let mut page = vec![0u8; self.page_length];

        let start = self.calc_offset(address);
        self.file.seek(SeekFrom::Start(start))?;
        self.file.read_exact(&mut page)?;

        let page = NodePage::from_arr(&page, self.page_length);

        return Ok(page);
    }

    pub fn print_nodes(&mut self) {


        let mut curr_address = 0;

        loop {

            if curr_address >= self.cursor.0 {
                break
            }

            let page: NodePage = self.get_node_page(&PageAddress(curr_address)).unwrap();

            let this_nodes = page.get_nodes();

            for (idx, node) in this_nodes.iter().enumerate() {

                println!("DEBUG NODE: {:?}|{:?} -> {:?}", curr_address, idx, node.pretty());
            }

            curr_address += 1;


        }
    }

    pub fn write_page(&mut self, page: &NodePage) -> Result<PageAddress, Error> {

        let start = self.calc_offset(&self.cursor);
        self.file.seek(SeekFrom::Start(start))?;

        let data = page.get_data();

        self.file.write(data)?;

        let res = self.cursor.clone();
        self.cursor.0 += 1;
        
        //update cursor on disk
        let mut cursor_arr: [u8; layout::HEADER_CURSOR_SIZE] = [0x00; layout::HEADER_CURSOR_SIZE];

        BigEndian::write_u64(&mut cursor_arr, self.cursor.0 as u64);

        self.file.seek(SeekFrom::Start(layout::HEADER_CURSOR_START as u64))?;
        self.file.write(&cursor_arr)?;

        Ok(res)
    }

    pub fn write_page_at_offset(&mut self, page: &NodePage, address: &PageAddress) -> Result<PageAddress, Error> {
        let start = self.calc_offset(address);
        self.file.seek(SeekFrom::Start(start))?;

        let data = page.get_data();

        self.file.write(data)?;
        let res = self.cursor.clone();

        Ok(res)
    }

    pub fn node_from_pointer(&mut self, pointer: &PagePointer) -> Result<InternalNode, String> {

        let page = self.get_node_page(&pointer.page_address).unwrap();
        let node = page.get_node_at(&pointer.node_offset)?;

        return Ok(node);
    }

    pub fn data_from_pointer(&mut self, pointer: &PagePointer) -> Result<(InternalNode, NodePage), String> {

        let page = self.get_node_page(&pointer.page_address).unwrap();
        let node = page.get_node_at(&pointer.node_offset)?;

        return Ok((node, page));
    }


    pub fn add_node(&mut self, node: &InternalNode) -> Result<PagePointer, Error> {

        let cursor = self.cursor.clone();
        //dbg!(&self.cursor);
        let page_address = PageAddress(cursor.0 - 1 );
        let mut page = self.get_node_page(&page_address).unwrap();
        //dbg!(&page.get_capacity());

        let (mut page, page_address) =  match page.is_full() {
            true => {
                //dbg!("FULL");
                let new_page = NodePage::new(self.page_length);
                let new_address = self.write_page(&new_page)?;
                //self.cursor.0 += 1;
                (new_page, new_address)
            },
            false => {
                (page, page_address)
            }
        };

        let offset = page.add_node(node);

        self.write_page_at_offset(&page, &page_address)?;

        let return_pointer = PagePointer {
            page_type: PageType::Node,
            page_address: page_address,
            node_offset: offset.unwrap(),
        };
        //dbg!(&return_pointer);

        Ok(return_pointer)
    }

    pub fn update_node(&mut self, pointer: &PagePointer, new_node: &InternalNode) -> Result<(), Error> {


        let (mut node, mut page) = self.data_from_pointer(pointer).unwrap();

        page.write_node_at(new_node.clone(), pointer.node_offset.clone()).unwrap();
        self.write_page_at_offset(&page, &pointer.page_address)?;



        /*
        dbg!(&pointer);
        dbg!(&node);
        dbg!(&new_node);
        */

        //let (mut node, mut page) = self.data_from_pointer(pointer).unwrap();
        //dbg!(&node);
        //panic!();

        return Ok(())
    }


    
}
*/

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


    /*
    #[test]
    fn quick_leafpage_to_file_and_back_works() {
        let mut pager = RecordPager::new(Path::new("test_data/kdtree.records.2"), true).unwrap();

        let descriptor_array: [f32; layout::DESCRIPTOR_LENGTH] = 
                                        [1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8];
        let cr = CompoundRecord {
            dataset_identifier: 0,
            compound_identifier: CompoundIdentifier::from_str("ZINC1234"),
            descriptor: Descriptor{data: descriptor_array},
        };

        let mut lp = RecordPage::new();

        lp.add_record(&cr).unwrap();
        lp.add_record(&cr).unwrap();
        lp.add_record(&cr).unwrap();
        lp.add_record(&cr).unwrap();
        lp.add_record(&cr).unwrap();

        pager.write_page(&lp).unwrap();
        pager.write_page(&lp).unwrap();
        pager.write_page(&lp).unwrap();
        pager.write_page(&lp).unwrap();

        let page = pager.get_record_page(&PageAddress(0)).unwrap();

        let record = page.get_record_at(0).unwrap();
        assert_eq!(record, cr);

        let record = page.get_record_at(3).unwrap();
        assert_eq!(record, cr);
    }
    */


    /*
    #[test]
    fn quick_leafpage_writes_and_overwrites() {
        let mut pager = RecordPager::new(Path::new("test_data/kdtree.records.1"), true).unwrap();

        let descriptor_array: [f32; layout::DESCRIPTOR_LENGTH] = 
                                        [1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8];
        let cr1 = CompoundRecord {
            dataset_identifier: 0,
            compound_identifier: CompoundIdentifier::from_str("ZINC1234"),
            descriptor: Descriptor{data: descriptor_array},
        };
                          [1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8];
        let cr2 = CompoundRecord {
            dataset_identifier: 0,
            compound_identifier: CompoundIdentifier::from_str("ENAMINE1234"),
            descriptor: Descriptor{data: descriptor_array},
        };


        let mut lp1 = RecordPage::new();
        let mut lp2 = RecordPage::new();

        lp1.add_record(&cr1).unwrap();
        lp1.add_record(&cr1).unwrap();
        lp1.add_record(&cr1).unwrap();
        lp1.add_record(&cr1).unwrap();
        lp1.add_record(&cr1).unwrap();

        lp2.add_record(&cr2).unwrap();
        lp2.add_record(&cr2).unwrap();
        lp2.add_record(&cr2).unwrap();
        lp2.add_record(&cr2).unwrap();
        lp2.add_record(&cr2).unwrap();


        pager.write_page(&lp1).unwrap();
        pager.write_page(&lp1).unwrap();
        pager.write_page(&lp1).unwrap();
        pager.write_page(&lp1).unwrap();
        pager.write_page(&lp1).unwrap();


        let page = pager.get_record_page(&PageAddress(0)).unwrap();

        let record = page.get_record_at(0).unwrap();
        assert_eq!(record, cr1);

        pager.write_page_at_offset(&lp2, &PageAddress(3)).unwrap();

        let page = pager.get_record_page(&PageAddress(3)).unwrap();

        let record = page.get_record_at(2).unwrap();
        assert_eq!(record, cr2);

        let page = pager.get_record_page(&PageAddress(2)).unwrap();

        let record = page.get_record_at(2).unwrap();
        assert_eq!(record, cr1);


        let page = pager.get_record_page(&PageAddress(4)).unwrap();

        let record = page.get_record_at(0).unwrap();
        assert_eq!(record, cr1);
    }
    */


}


/*

#[derive(Debug)]
pub struct CachedPager {
    pub nodes: Vec<InternalNode>,
    pub address_map: HashMap<(usize, usize), usize>,
}

impl CachedPager {

    pub fn from_filename(filename: &Path, page_length: usize) -> Result<CachedPager, Error> {

        dbg!(filename); let mut node_pager = NodePager::new(filename, page_length, false)?;

        //TODO: make this capacity calculation correct
        let mut all_nodes: Vec<InternalNode> = Vec::with_capacity(node_pager.cursor.0 * 300);
        let mut address_map: HashMap<(usize, usize), usize> = HashMap::new();

        for i in 0..node_pager.cursor.0 {
            //println!("I: {}", i);

            let page = node_pager.get_node_page(&PageAddress(i)).unwrap();
            let nodes = page.get_nodes();

            for (j, node) in nodes.iter().enumerate() {
                //println!("\tJ: {}", j);
                let key = (i as usize, j as usize); 

                all_nodes.push(node.clone());

                address_map.insert(key, all_nodes.len() - 1);

            }
        }

        Ok(CachedPager {
            nodes: all_nodes,
            address_map,
        })
    }

    pub fn get_node_from_address(&self, address: &PageAddress, offset: &ItemOffset) -> Result<InternalNode, String> {
        let address = address.0 as usize;
        let offset = offset.0 as usize;

        return Ok(self.nodes[self.address_map[&(address, offset)]].clone());

    }

}
*/


#[derive(Debug)]
pub struct FastNodePager {
    //pub map: HashMap<(usize, usize), InternalNode>,
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

        let mut records: Vec<(CompoundIdentifier, PageAddress)> = Vec::new();
        let mut curr_address = 0;

        loop {

            if curr_address > self.cursor {
                break
            }

            let page: RecordPage = self.get_record_page(&curr_address).unwrap();

            let this_records = page.get_records();

            let this_amended_records: Vec<_> = this_records.into_iter().map(|x| (x.compound_identifier, PageAddress(curr_address))).collect();

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

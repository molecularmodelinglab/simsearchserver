//! Handles read and write for whole pages on disk
//!
//!

use crate::error::Error;
//use crate::node::{PageAddress, InternalNode, NodeOffset, Descriptor, CompoundRecord, CompoundIdentifier};
use crate::node::{PageAddress, NodeOffset, CompoundIdentifier, InternalNode};
use crate::page::{NodePage, LeafPage, Pageable, PageType};
use byteorder::{ByteOrder, BigEndian};
use crate::layout;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::collections::HashMap;

use std::fmt;


#[derive(Debug)]
pub struct Pager {
    file: File,
    pub cursor: PageAddress, //this is the next available slot
}

#[derive(Debug, Clone, PartialEq)]
pub struct PagePointer {
    pub page_type: PageType,
    pub page_address: PageAddress,
    pub node_offset: NodeOffset,
}

impl PagePointer {

    pub fn pretty(&self) -> String {

        let s = format!("{:?}|{:?}|{:?}", self.page_type, self.page_address.0, self.node_offset.0);
        return s;
    }
}

impl fmt::Display for PagePointer {

    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Write strictly the first element into the supplied output
        // stream: `f`. Returns `fmt::Result` which indicates whether the
        // operation succeeded or failed. Note that `write!` uses syntax which
        // is very similar to `println!`.
        write!(f, "{:?}|{:?}|{:?}", self.page_type, self.page_address.0, self.node_offset.0)
    }
}

impl Pager {
    pub fn new(path: &Path, create: bool) -> Result<Pager, Error> {

        match create {
            true => {
                let fd = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(true)
                    .open(path)?;

                return Ok(Pager {
                    file: fd,
                    cursor: PageAddress(0),
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
                    
                    return Ok(Pager {
                        file: fd,
                        cursor: PageAddress(value),
                    })

            }
        }

    }

    pub fn calc_offset(&self, offset: u64) -> u64 {

        return layout::FILE_DATA_START as u64 + offset;

    }

    pub fn get_record_page(&mut self, offset: &PageAddress) -> Result<LeafPage, Error> {
        let mut page: [u8; layout::PAGE_SIZE] = [0x00; layout::PAGE_SIZE];
        //let mut page: [u8; layout::PAGE_SIZE - 10] = [0x00; layout::PAGE_SIZE - 10];
        //dbg!(offset.0);
        self.file.seek(SeekFrom::Start(self.calc_offset(offset.as_actual_address())))?;
        self.file.read_exact(&mut page).unwrap();

        //dbg!(result);

        let page = LeafPage::from_arr(page);

        return Ok(page);
        //return Ok(PageFromDisk::Node(page_object));
        //Ok(Page::new(page))
    }

    pub fn get_node_page(&mut self, offset: &PageAddress) -> Result<NodePage, Error> {
        //dbg!(&offset);
        let mut page: [u8; layout::PAGE_SIZE] = [0x00; layout::PAGE_SIZE];

        let start = self.calc_offset(offset.as_actual_address());
        //dbg!(&start);
        self.file.seek(SeekFrom::Start(self.calc_offset(offset.as_actual_address())))?;
        self.file.read_exact(&mut page)?;
        //dbg!(&page[0..10]);

        let page = NodePage::from_arr(page);

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

    pub fn print_records(&mut self) {


        let mut records: Vec<(CompoundIdentifier, PageAddress)> = Vec::new();
        let mut curr_address = 0;

        loop {

            if curr_address > self.cursor.0 {
                break
            }

            let page: LeafPage = self.get_record_page(&PageAddress(curr_address)).unwrap();

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

    pub fn write_page(&mut self, page: &impl Pageable) -> Result<PageAddress, Error> {
        self.file.seek(SeekFrom::Start(self.calc_offset(self.cursor.as_actual_address())))?;
        let data = page.get_data();
        //dbg!(&data[0..10]);
        //self.file.write_all(data)?;
        self.file.write(data)?;

        let res = self.cursor.clone();
        self.cursor.0 += 1;
        //dbg!("WRITE PAGE RETURN ADDRESS");
        //dbg!(&res);
        
        
        
        //update cursor on disk
        let mut cursor_arr: [u8; layout::HEADER_CURSOR_SIZE] = [0x00; layout::HEADER_CURSOR_SIZE];

        BigEndian::write_u64(&mut cursor_arr, self.cursor.0 as u64);

        self.file.seek(SeekFrom::Start(layout::HEADER_CURSOR_START as u64))?;
        self.file.write(&cursor_arr)?;
        //println!("NEW CURSOR: {:?} -> {:?}", page.get_type(), self.cursor.0);

        Ok(res)
    }

    pub fn write_page_at_offset(&mut self, page: &impl Pageable, offset: &PageAddress) -> Result<PageAddress, Error> {
        self.file.seek(SeekFrom::Start(self.calc_offset(offset.as_actual_address())))?;
        let data = page.get_data();
        //dbg!(data.len());
        //dbg!("WRITE AT", offset);
        //self.file.write_all(data)?;
        self.file.write(data)?;
        let res = self.cursor.clone();
        //self.cursor.0 += 1;
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{PageAddress, InternalNode, NodeOffset, Descriptor, CompoundRecord, CompoundIdentifier};
    
    #[test]
    fn quick_nodepage_to_file_and_back_works() {
        let mut pager = Pager::new(Path::new("test_data/kdtree.nodes"), true).unwrap();

        let mut node1 = InternalNode::default();

        node1.parent_page_address = PageAddress(65536);
        node1.parent_node_offset = NodeOffset(2);
        node1.left_child_page_address = PageAddress(300);
        node1.left_child_node_offset = NodeOffset(4);
        node1.right_child_page_address = PageAddress(500);
        node1.right_child_node_offset = NodeOffset(6);
        node1.split_axis = 7;
        node1.split_value = 8.8888;

        let mut nodepage = NodePage::new();

        node1.parent_page_address = PageAddress(4096);
        for _ in 0..10 {
            nodepage.push_node(&node1).unwrap();
        }

        let mut node2 = InternalNode::default();

        node2.parent_page_address = PageAddress(4550);
        node2.parent_node_offset = NodeOffset(2);
        node2.left_child_page_address = PageAddress(300);
        node2.left_child_node_offset = NodeOffset(4);
        node2.right_child_page_address = PageAddress(500);
        node2.right_child_node_offset = NodeOffset(6);
        node2.split_axis = 2;
        node2.split_value = 32.9999;

        nodepage.push_node(&node2).unwrap();

        let page = &nodepage;
        pager.write_page(page).unwrap();
        pager.write_page(page).unwrap();

        //read first page
        let page = pager.get_node_page(&PageAddress(0)).unwrap();

        //let node = page.get_node_at(NodeOffset(9));

        let node_from_file = page.get_node_at(NodeOffset(0)).unwrap();
        assert_eq!(node1, node_from_file);

        let node_from_file = page.get_node_at(NodeOffset(8)).unwrap();
        assert_eq!(node1, node_from_file);


        let node_from_file = page.get_node_at(NodeOffset(10)).unwrap();
        assert_eq!(node2, node_from_file);

        //read second page
        let page = pager.get_node_page(&PageAddress(1)).unwrap();

        let node_from_file = page.get_node_at(NodeOffset(0)).unwrap();
        assert_eq!(node1, node_from_file);

        let node_from_file = page.get_node_at(NodeOffset(10)).unwrap();
        assert_eq!(node2, node_from_file);
    }

    #[test]
    fn quick_leafpage_to_file_and_back_works() {
        let mut pager = Pager::new(Path::new("test_data/kdtree.records.2"), true).unwrap();

        let descriptor_array: [f32; layout::DESCRIPTOR_LENGTH] = 
                                        [1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8];
        let cr = CompoundRecord {
            dataset_identifier: 0,
            compound_identifier: CompoundIdentifier::from_str("ZINC1234"),
            descriptor: Descriptor{data: descriptor_array},
        };

        let mut lp = LeafPage::new();

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


    #[test]
    fn quick_leafpage_writes_and_overwrites() {
        let mut pager = Pager::new(Path::new("test_data/kdtree.records.1"), true).unwrap();

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


        let mut lp1 = LeafPage::new();
        let mut lp2 = LeafPage::new();

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


}



#[derive(Debug)]
pub struct CachedPager {
    pub nodes: Vec<InternalNode>,
    pub address_map: HashMap<(usize, usize), usize>,
}

impl CachedPager {

    pub fn from_filename(filename: &Path) -> Result<CachedPager, Error> {

        dbg!(filename);
        let mut node_pager = Pager::new(filename, false)?;

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

    pub fn get_node_from_address(&self, address: &PageAddress, offset: &NodeOffset) -> Result<InternalNode, String> {
        let address = address.0 as usize;
        let offset = offset.0 as usize;

        return Ok(self.nodes[self.address_map[&(address, offset)]].clone());

    }

}


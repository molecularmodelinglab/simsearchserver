//! Reads and writes whole pages on disk.

use crate::error::Error;
use crate::node::{InternalNode, PagePointer};
use crate::page::{RecordPage, RecordPageRef};
use byteorder::{ByteOrder, BigEndian};
use crate::layout;
use memmap2::{Advice, Mmap, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::collections::HashMap;

#[derive(Debug)]
pub struct RecordPager {
    read_fd: File,
    /// Whole-file map, present when the pager was opened rather than created.
    /// `page_ref` serves pages out of it with no read and no copy.
    mmap: Option<Mmap>,
    path: String,
    pub next_free_index: usize, //this is the next available slot
    pub desc_length: usize,
    pub page_length: usize,
    cache: HashMap<usize, RecordPage>,
    cache_limit: Option<f32>,
    cache_check_counter: usize,
}

pub struct DiskNodePager {
    filename: String
}

impl DiskNodePager {

    pub fn from_file(filename: &String) -> Result<Self, Error> {
        return Ok(Self{filename: filename.clone()});
    }

    fn calc_offset(index: usize) -> usize {

        return layout::FILE_DATA_START + (index * layout::NODE_SIZE);

    }

    pub fn get_node(&self, index: &usize) -> Result<InternalNode, String> {

        let mut fd = OpenOptions::new()
                    .create(false)
                    .read(true)
                    .write(false)
                    .truncate(false)
                    .open(&self.filename).unwrap();

        let start = Self::calc_offset(*index);
        let mut node_arr: [u8; layout::NODE_SIZE] = [0x00; layout::NODE_SIZE];
        fd.seek(SeekFrom::Start(start as u64)).unwrap();
        fd.read_exact(&mut node_arr).unwrap();
        let node = InternalNode::from_slice(&node_arr).unwrap();

        Ok(node)

    }

}

pub struct ImmutNodePager {
    pub store: Vec<InternalNode>
}

pub trait GetNode {

    fn get_node(&self, index: &usize) -> Result<&InternalNode, String>;

}

impl ImmutNodePager {

    pub fn len(&self) -> usize {
        return self.store.len();
    }

    fn calc_offset(index: usize) -> usize {

        return layout::FILE_DATA_START + (index * layout::NODE_SIZE);

    }

    pub fn from_file(filename: &String) -> Result<Self, Error> {

        let path = Path::new(filename);

        let mut fd = OpenOptions::new()
                    .create(false)
                    .read(true)
                    .write(false)
                    .truncate(false)
                    .open(path)?;

        let mut next_free_index_arr: [u8; layout::HEADER_CURSOR_SIZE] = [0x00; layout::HEADER_CURSOR_SIZE];

        fd.seek(SeekFrom::Start(layout::HEADER_CURSOR_START as u64))?;
        fd.read_exact(&mut next_free_index_arr)?;

        let mut store: Vec<InternalNode> = Vec::new();

        let attempted_usize = layout::Value::try_from(next_free_index_arr);
        let layout::Value(value) = attempted_usize.unwrap();
        
        for i in 0..(value + 1) {
            let mut node_arr: [u8; layout::NODE_SIZE] = [0x00; layout::NODE_SIZE];

            let start = Self::calc_offset(i);
            fd.seek(SeekFrom::Start(start as u64))?;
            fd.read_exact(&mut node_arr)?;
            let node = InternalNode::from_slice(&node_arr).unwrap();
            store.push(node);

        }
        
        Ok(Self{store})

    }


}
impl GetNode for ImmutNodePager {

    fn get_node(&self, index: &usize) -> Result<&InternalNode, String> {

        let ret = match self.store.get(index.clone()) {
            Some(node) => Ok(node),
            None => Err(format!("Node not found at address: {:?}", index)),
        };

        return ret;
    }
}


#[derive(Debug)]
pub struct FastNodePager {
    pub store: Vec<InternalNode>,
}



impl GetNode for FastNodePager {

    fn get_node(&self, index: &usize) -> Result<&InternalNode, String> {

        let ret = match self.store.get(index.clone()) {
            Some(node) => Ok(node),
            None => Err(format!("Node not found at address: {:?}", index)),
        };

        return ret;
    }
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
        }

        let next_free_index = self.store.len() - 1;
        //update next_free_index on disk
        let mut next_free_index_arr: [u8; layout::HEADER_CURSOR_SIZE] = [0x00; layout::HEADER_CURSOR_SIZE];

        BigEndian::write_u64(&mut next_free_index_arr, next_free_index as u64);

        fd.seek(SeekFrom::Start(layout::HEADER_CURSOR_START as u64))?;
        fd.write(&next_free_index_arr)?;

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

        let mut next_free_index_arr: [u8; layout::HEADER_CURSOR_SIZE] = [0x00; layout::HEADER_CURSOR_SIZE];
        fd.seek(SeekFrom::Start(layout::HEADER_CURSOR_START as u64))?;
        fd.read_exact(&mut next_free_index_arr)?;

        let mut pager = Self::new();

        let attempted_usize = layout::Value::try_from(next_free_index_arr);
        let layout::Value(value) = attempted_usize.unwrap();
        
        for i in 0..(value + 1) {
            let mut node_arr: [u8; layout::NODE_SIZE] = [0x00; layout::NODE_SIZE];
            let start = Self::calc_offset(i);
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

    pub fn add_node(&mut self, node: &InternalNode) -> Result<PagePointer, Error> {

        self.store.push(node.clone());

        let added_index = self.store.len() - 1;

        Ok(PagePointer::Node(added_index))
    }

    pub fn update_node(&mut self, index: &usize, new_node: &InternalNode) -> Result<(), Error> {

        self.store[index.clone()] = new_node.clone();

        return Ok(())
    }
}


impl RecordPager {

    pub fn new(path: String, page_length: usize, desc_length: usize, create: bool, cache_limit: Option<f32>) -> Result<Self, Error> {

        match create {
            true => {

                    let mut file = OpenOptions::new()
                            .create(true)
                            .read(true)
                            .write(true)
                            .open(path.clone())?;

                    file.write("empty".as_bytes())?;

                    let read_fd = OpenOptions::new()
                            .read(true)
                            .open(path.clone())?;

                    return Ok(Self{
                    read_fd,
                    mmap: None,
                    path: path,
                    next_free_index: 0,
                    desc_length,
                    page_length,
                    cache: HashMap::new(),
                    cache_limit: cache_limit,
                    cache_check_counter: 0,
                })
            },
            false => {
                let mut fd = OpenOptions::new()
                    .read(true)
                    .open(path.clone())?;

                    let mut next_free_index_from_disk: [u8; layout::HEADER_CURSOR_SIZE] = [0x00; layout::HEADER_CURSOR_SIZE];
                    fd.seek(SeekFrom::Start(layout::HEADER_CURSOR_START as u64))?;
                    fd.read_exact(&mut next_free_index_from_disk)?;

                    let attempted_usize = layout::Value::try_from(next_free_index_from_disk);
                    let layout::Value(value) = attempted_usize.unwrap();

                    let mmap = unsafe { MmapOptions::new().map(&fd) }?;
                    let _ = mmap.advise(Advice::Random);

                    return Ok(Self {
                        read_fd: fd,
                        mmap: Some(mmap),
                        path: path,
                        next_free_index: value,
                        desc_length,
                        page_length,
                        cache: HashMap::new(),
                        cache_limit: cache_limit,
                        cache_check_counter: 0,
                })

            }
        }

    }


    pub fn calc_offset(&self, address: &usize) -> u64 {

        let offset = (address * self.page_length) as u64;
        return layout::FILE_DATA_START as u64 + offset;

    }

    pub fn get_record_page(&self, address: &usize) -> Result<RecordPage, Error> {

        let retval = match self.cache.get(address) {
            Some(x) => {
                Ok(x.clone())
            }
            None => {

                
                let page = self._read_record_page(address)?;
                Ok(page)
            }
        };

        return retval;
    }

    pub fn get_record_page_no_cache(&self, address: &usize) -> Result<RecordPage, Error> {

        let page = self._read_record_page(address)?;
        Ok(page)
    }

    pub fn _read_record_page(&self, address: &usize) -> Result<RecordPage, Error> {

        let mut page: Vec<u8>  = vec![0; self.page_length];

        let start = self.calc_offset(address);

        self.read_fd.read_exact_at(&mut page, start)?;

        return Ok(RecordPage::from_vec(page, self.page_length, self.desc_length));
    }

    /// Faults the whole record file into the page cache.
    pub fn warm(&self) -> Result<usize, Error> {

        let mmap = match &self.mmap {
            Some(m) => m,
            None => return Ok(0),
        };

        mmap.advise(Advice::Sequential)?;
        mmap.advise(Advice::WillNeed)?;
        mmap.advise(Advice::PopulateRead)?;
        mmap.advise(Advice::Random)?;

        return Ok(mmap.len());
    }

    /// Borrowed view of a leaf page, served from the memory map.
    pub fn page_ref(&self, address: &usize) -> Result<RecordPageRef<'_>, Error> {

        let mmap = match &self.mmap {
            Some(m) => m,
            None => return Err(Error::UnexpectedError),
        };

        let start = self.calc_offset(address) as usize;
        let end = start + self.page_length;

        if end > mmap.len() {
            return Err(Error::KeyNotFound);
        }

        return Ok(RecordPageRef::from_slice(&mmap[start..end], self.desc_length));
    }

    pub fn print_records(&mut self) {

        let mut records: Vec<(u64, usize)> = Vec::new();
        let mut curr_address = 0;

        loop {

            if curr_address > self.next_free_index {
                break
            }

            let page: RecordPage = self.get_record_page(&curr_address).unwrap();

            let this_records = page.get_records();

            let this_amended_records: Vec<_> = this_records.into_iter().map(|x| (x.index, curr_address)).collect();

            records.extend(this_amended_records);

            curr_address += 1;


        }

        for record in records.iter() {
            println!("DEBUG RECORD: {:?}", record);
        }

        println!("NUMBER OF RECORDS FOUND: {:?}", records.len());
        println!("LAST RECORD FOUND: {:?}", records[records.len() - 1]);

    }

    pub fn get_cache_len(&self) -> usize {
        return self.cache.len();
    }

    pub fn len(&self) -> usize {
        return self.next_free_index;
    }

    pub fn flush(&mut self) {

        for (key, value) in self.cache.clone().iter() {

            self._write_page_at_offset(value, &key)
                .expect("could not write a cached page during flush");

        }

    }

    pub fn flush_keys(&mut self, keys: Vec<usize>) {

        for key in keys.iter() {

            let value = self.cache.get(key).unwrap().clone();

            let res = self._write_page_at_offset(&value, &key);

            match res {
                Ok(_) => {},
                Err(e) => {panic!("{:?}", e)},
            }
            self.cache.remove(key);

        }

    }

    pub fn add_page(&mut self, page: &RecordPage) -> Result<PagePointer, Error> {


        self.cache.insert(self.next_free_index, page.clone());
        let ret_index = self.next_free_index.clone();
        self.next_free_index += 1;
        
        return Ok(PagePointer::Leaf(ret_index));
    }

    pub fn update_page(&mut self, page: &RecordPage, address: &usize) -> Result<(), Error> {

        self.cache.insert(address.clone(), page.clone());

        self.check_cache();

        Ok(())
    }

    pub fn get_cache_size_gb(&self) -> f32 {
        return self.get_cache_len() as f32 * self.page_length as f32 / 1000000000 as f32;
    }

    pub fn check_cache(&mut self) {

        if self.cache_check_counter > 1000 {
            self.cache_check_counter = 0;
        } else {
            self.cache_check_counter += 1;
            ()
        }

        match self.cache_limit {
            None => {},
            Some(limit) => {
                let current_cache_size_gb = self.get_cache_size_gb();

                if current_cache_size_gb > limit as f32 {
                    self._evict();
                }
            }
        }
    }

    fn _evict(&mut self) {

        let evict_prop = 0.1;
        let evict_num = self.cache.len() as f32 * evict_prop;
        let mut keys_to_flush: Vec<usize> = Vec::new();

        let mut key_iter = self.cache.keys();
        for _ in 0..evict_num as usize {
            let key = key_iter.next().unwrap().clone();
            keys_to_flush.push(key);
        }

        self.flush_keys(keys_to_flush);


    }

    pub fn _write_page_at_offset(&mut self, page: &RecordPage, address: &usize) -> Result<(), Error> {
        let start = self.calc_offset(address);

        let mut file = OpenOptions::new()
                    .write(true)
                    .open(self.path.clone())?;

        file.seek(SeekFrom::Start(start))?;


        let data = page.get_data();

        file.write(data)?;

        Ok(())
    }
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
                
                std::fs::create_dir_all("/tmp/kd_tree_test").unwrap();

                let filename = "/tmp/kd_tree_test/io_node".to_string();
                pager.to_file(&filename).unwrap();
                pager = FastNodePager::from_file(&filename).unwrap();

                assert_eq!(pager.store.len(), num_nodes);
            }
        }

    }
}



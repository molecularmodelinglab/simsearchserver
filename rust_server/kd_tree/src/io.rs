//! Reads and writes whole pages on disk.

use crate::error::Error;
use crate::node::{InternalNode, PagePointer};
use crate::page::{RecordPage, RecordPageRef};
use crate::layout;
use memmap2::{Advice, Mmap, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::collections::HashMap;

fn write_file_header(fd: &mut File, magic: &[u8; layout::MAGIC_SIZE], cursor: usize)
    -> Result<(), Error> {

    let mut header = [0u8; layout::FILE_DATA_START];
    header[layout::MAGIC_OFFSET..layout::MAGIC_OFFSET + layout::MAGIC_SIZE]
        .copy_from_slice(magic);
    header[layout::VERSION_OFFSET..layout::VERSION_OFFSET + layout::VERSION_SIZE]
        .copy_from_slice(&layout::FORMAT_VERSION.to_le_bytes());
    header[layout::HEADER_CURSOR_START..layout::HEADER_CURSOR_START + layout::HEADER_CURSOR_SIZE]
        .copy_from_slice(&(cursor as u64).to_le_bytes());

    fd.seek(SeekFrom::Start(0))?;
    fd.write_all(&header)?;

    Ok(())
}

/// Panics rather than returns: a mismatched file makes every answer wrong.
fn read_file_header(fd: &File, magic: &[u8; layout::MAGIC_SIZE], filename: &str) -> usize {

    let mut header = [0u8; layout::FILE_DATA_START];
    fd.read_exact_at(&mut header, 0)
        .unwrap_or_else(|e| panic!("could not read the header of {}: {}", filename, e));

    let found = &header[layout::MAGIC_OFFSET..layout::MAGIC_OFFSET + layout::MAGIC_SIZE];
    if found != magic {
        panic!(
            "{} starts with {:?}, not {:?}: this is not the file it was expected \
             to be, or it predates the magic number and has to be rebuilt.",
            filename, String::from_utf8_lossy(found), String::from_utf8_lossy(magic),
        );
    }

    let version = u32::from_le_bytes(
        header[layout::VERSION_OFFSET..layout::VERSION_OFFSET + layout::VERSION_SIZE]
            .try_into().expect("version is VERSION_SIZE bytes"));
    if version != layout::FORMAT_VERSION {
        panic!(
            "{} is format version {}, but this build reads version {}. Rebuild the index.",
            filename, version, layout::FORMAT_VERSION,
        );
    }

    return u64::from_le_bytes(
        header[layout::HEADER_CURSOR_START..layout::HEADER_CURSOR_START + layout::HEADER_CURSOR_SIZE]
            .try_into().expect("cursor is HEADER_CURSOR_SIZE bytes")) as usize;
}

#[derive(Debug)]
pub struct RecordPager {
    read_fd: File,
    /// Present when opened rather than created, letting `page_ref` avoid copying.
    mmap: Option<Mmap>,
    path: String,
    pub next_free_index: usize,
    pub desc_length: usize,
    pub page_length: usize,
    cache: HashMap<usize, RecordPage>,
    cache_limit: Option<f32>,
    cache_check_counter: usize,
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

        let node_count = read_file_header(&fd, &layout::NODE_FILE_MAGIC, filename);

        let mut store: Vec<InternalNode> = Vec::new();

        for i in 0..node_count {
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

        write_file_header(&mut fd, &layout::NODE_FILE_MAGIC, self.store.len())?;

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

        let node_count = read_file_header(&fd, &layout::NODE_FILE_MAGIC, filename);

        let mut pager = Self::new();

        for i in 0..node_count {
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

                    write_file_header(&mut file, &layout::RECORD_FILE_MAGIC, 0)?;

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
                let fd = OpenOptions::new()
                    .read(true)
                    .open(path.clone())?;

                    let next_free_index = read_file_header(&fd, &layout::RECORD_FILE_MAGIC, &path);

                    let mmap = unsafe { MmapOptions::new().map(&fd) }?;
                    let _ = mmap.advise(Advice::Random);

                    return Ok(Self {
                        read_fd: fd,
                        mmap: Some(mmap),
                        path: path,
                        next_free_index,
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

        let mut fd = OpenOptions::new()
                .write(true)
                .open(self.path.clone())
                .expect("could not reopen the record file to stamp its header");

        write_file_header(&mut fd, &layout::RECORD_FILE_MAGIC, self.next_free_index)
            .expect("could not stamp the record file header");

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



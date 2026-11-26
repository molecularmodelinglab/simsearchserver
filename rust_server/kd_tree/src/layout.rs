//! Sets constants for the disk layout


use std::mem::size_of;
use crate::error::Error;
use std::convert::TryFrom;



pub const PTR_SIZE: usize = size_of::<usize>(); 


pub struct Value (pub usize);

impl TryFrom<[u8; PTR_SIZE]> for Value {
    type Error = Error;

    fn try_from(arr: [u8; PTR_SIZE]) -> Result<Self, Self::Error> {
        Ok(Self(usize::from_le_bytes(arr)))
    }
}

impl TryFrom<[u8; 1]> for Value {
    type Error = Error;

    fn try_from(arr: [u8; 1]) -> Result<Self, Self::Error> {
        Ok(Self(usize::from(arr[0])))
    }
}

pub const LEFT_CHILD_INDEX_START: usize = 0;
pub const LEFT_CHILD_INDEX_SIZE: usize = PTR_SIZE;

pub const LEFT_CHILD_TYPE_START: usize = LEFT_CHILD_INDEX_START + LEFT_CHILD_INDEX_SIZE;
pub const LEFT_CHILD_TYPE_SIZE: usize = 1;

pub const RIGHT_CHILD_INDEX_START: usize = LEFT_CHILD_TYPE_START + LEFT_CHILD_TYPE_SIZE; 
pub const RIGHT_CHILD_INDEX_SIZE: usize = PTR_SIZE;

pub const RIGHT_CHILD_TYPE_START: usize = RIGHT_CHILD_INDEX_START + RIGHT_CHILD_INDEX_SIZE;
pub const RIGHT_CHILD_TYPE_SIZE: usize = 1;

pub const SPLIT_AXIS_OFFSET: usize = RIGHT_CHILD_TYPE_START + RIGHT_CHILD_TYPE_SIZE;
pub const SPLIT_AXIS_SIZE: usize = 1;

pub const SPLIT_VALUE_OFFSET: usize = SPLIT_AXIS_OFFSET + SPLIT_AXIS_SIZE;
pub const SPLIT_VALUE_SIZE: usize = 4;

pub const NODE_SIZE: usize = SPLIT_VALUE_OFFSET + SPLIT_VALUE_SIZE;


pub const INDEX_START: usize = 0;
pub const INDEX_SIZE: usize = 8;

pub const DESCRIPTOR_START: usize = INDEX_START + INDEX_SIZE;

//for generic Page
pub const PAGE_TYPE_OFFSET: usize = 0;
pub const PAGE_TYPE_SIZE: usize = 1;

pub const TAIL_OFFSET: usize = PAGE_TYPE_OFFSET + PAGE_TYPE_SIZE;
pub const TAIL_SIZE: usize = 4;

pub const IS_EMPTY_OFFSET: usize = TAIL_OFFSET + TAIL_SIZE;
pub const IS_EMPTY_SIZE: usize = 1;

/// Aligns records to an 8-byte boundary.
pub const PAGE_HEADER_PADDING: usize = 2;

/// Interleaved f32 pairs, so a page is skipped without scanning its records.
pub const BOUNDS_OFFSET: usize = IS_EMPTY_OFFSET + IS_EMPTY_SIZE + PAGE_HEADER_PADDING;

pub const fn bounds_size(desc_length: usize) -> usize {
    return desc_length * 8;
}

pub const fn page_data_start(desc_length: usize) -> usize {
    return BOUNDS_OFFSET + bounds_size(desc_length);
}

/// On-disk format revision, stamped into every file of an index.
pub const FORMAT_VERSION: u32 = 1;

/// Rejects a file passed in the wrong position.
pub const RECORD_FILE_MAGIC: [u8; MAGIC_SIZE] = *b"KDTRECRD";
pub const NODE_FILE_MAGIC: [u8; MAGIC_SIZE] = *b"KDTNODES";

pub const MAGIC_OFFSET: usize = 0;
pub const MAGIC_SIZE: usize = 8;

pub const VERSION_OFFSET: usize = MAGIC_OFFSET + MAGIC_SIZE;
pub const VERSION_SIZE: usize = 4;

/// Pads the file header so the cursor lands on an 8-byte boundary.
pub const FILE_HEADER_PADDING: usize = 4;

pub const HEADER_CURSOR_START: usize = VERSION_OFFSET + VERSION_SIZE + FILE_HEADER_PADDING;
pub const HEADER_CURSOR_SIZE: usize = PTR_SIZE;

pub const FILE_DATA_START: usize = HEADER_CURSOR_START + HEADER_CURSOR_SIZE;





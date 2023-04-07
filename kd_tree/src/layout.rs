//! Sets constants for the disk layout


use std::mem::size_of;
use crate::error::Error;
use std::convert::TryFrom;

//pub const PAGE_SIZE: usize = 4096;
//pub const PAGE_SIZE: usize = 8192;
//pub const PAGE_SIZE: usize = 8192;
pub const PTR_SIZE: usize = size_of::<usize>(); 

pub const NODE_PAGE_SIZE: usize = 8192;
//aub const NODE_PAGE_SIZE: usize = 4096;

//pub const RECORD_PAGE_SIZE: usize = 16384;
//jpub const RECORD_PAGE_SIZE: usize = 32768;
pub const RECORD_PAGE_SIZE: usize = 65536;
//pub const RECORD_PAGE_SIZE: usize = 8192;

//TODO: allow different values
pub const DESCRIPTOR_LENGTH: usize = 8;
//pub const DESCRIPTOR_LENGTH: usize = 16;

pub struct Value (pub usize);

/// Converts an array of length len(usize) to a usize as a BigEndian integer.
/// TODO: switch to using byteorder here as well
impl TryFrom<[u8; PTR_SIZE]> for Value {
    type Error = Error;

    fn try_from(arr: [u8; PTR_SIZE]) -> Result<Self, Self::Error> {
        Ok(Self(usize::from_be_bytes(arr)))
    }
}

/// Converts an array of length len(usize) to a usize as a BigEndian integer.
impl TryFrom<[u8; 1]> for Value {
    type Error = Error;

    fn try_from(arr: [u8; 1]) -> Result<Self, Self::Error> {
        Ok(Self(usize::from(arr[0])))
    }
}

//for InternalNode
//pub const NODE_TYPE_OFFSET: usize = 0;
//pub const NODE_TYPE_SIZE: usize = 1;

pub const PARENT_PAGE_START: usize = 0;
pub const PARENT_PAGE_SIZE: usize = PTR_SIZE;

pub const PARENT_NODE_OFFSET_START: usize = PARENT_PAGE_START + PARENT_PAGE_SIZE;
pub const PARENT_NODE_OFFSET_SIZE: usize = 1;

pub const LEFT_CHILD_PAGE_START: usize = PARENT_NODE_OFFSET_START + PARENT_NODE_OFFSET_SIZE;
pub const LEFT_CHILD_PAGE_SIZE: usize = PTR_SIZE;

pub const LEFT_CHILD_NODE_OFFSET_START: usize = LEFT_CHILD_PAGE_START + LEFT_CHILD_PAGE_SIZE; 
pub const LEFT_CHILD_NODE_OFFSET_SIZE: usize = 1;

pub const LEFT_CHILD_TYPE_START: usize = LEFT_CHILD_NODE_OFFSET_START + LEFT_CHILD_NODE_OFFSET_SIZE;
pub const LEFT_CHILD_TYPE_SIZE: usize = 1;

pub const RIGHT_CHILD_PAGE_START: usize = LEFT_CHILD_TYPE_START + LEFT_CHILD_TYPE_SIZE; 
pub const RIGHT_CHILD_PAGE_SIZE: usize = PTR_SIZE;

pub const RIGHT_CHILD_NODE_OFFSET_START: usize =  RIGHT_CHILD_PAGE_START + RIGHT_CHILD_PAGE_SIZE;
pub const RIGHT_CHILD_NODE_OFFSET_SIZE: usize = 1;

pub const RIGHT_CHILD_TYPE_START: usize = RIGHT_CHILD_NODE_OFFSET_START + RIGHT_CHILD_NODE_OFFSET_SIZE;
pub const RIGHT_CHILD_TYPE_SIZE: usize = 1;

pub const SPLIT_AXIS_OFFSET: usize = RIGHT_CHILD_TYPE_START + RIGHT_CHILD_TYPE_SIZE;
pub const SPLIT_AXIS_SIZE: usize = 1;

pub const SPLIT_VALUE_OFFSET: usize = SPLIT_AXIS_OFFSET + SPLIT_AXIS_SIZE;
pub const SPLIT_VALUE_SIZE: usize = 4;

pub const NODE_SIZE: usize = SPLIT_VALUE_OFFSET + SPLIT_VALUE_SIZE;


//for CompoundRecord
//TODO: make generic over different descriptor lengths
//
pub const DATASET_IDENTIFIER_START: usize = 0;
pub const DATASET_IDENTIFIER_SIZE: usize = 1;

pub const COMPOUND_IDENTIFIER_START: usize = DATASET_IDENTIFIER_START + DATASET_IDENTIFIER_SIZE;
pub const COMPOUND_IDENTIFIER_SIZE: usize = 16;

pub const DESCRIPTOR_START: usize = COMPOUND_IDENTIFIER_START + COMPOUND_IDENTIFIER_SIZE;
//pub const DESCRIPTOR_SIZE: usize = 32;

//pub const COMPOUND_RECORD_SIZE: usize = DESCRIPTOR_START + DESCRIPTOR_SIZE;

//for generic Page
pub const PAGE_TYPE_OFFSET: usize = 0;
pub const PAGE_TYPE_SIZE: usize = 1;

pub const TAIL_OFFSET: usize = PAGE_TYPE_OFFSET + PAGE_TYPE_SIZE;
//pub const TAIL_SIZE: usize = 2; //2 bytes supports up to 65535 records
pub const TAIL_SIZE: usize = 4; //2 bytes supports up to 65535 records, but 4 is easier to implement lol

pub const IS_EMPTY_OFFSET: usize = TAIL_OFFSET + TAIL_SIZE;
pub const IS_EMPTY_SIZE: usize = 1;

pub const PAGE_DATA_START: usize = IS_EMPTY_OFFSET + IS_EMPTY_SIZE;

//for whole file
pub const HEADER_CURSOR_START: usize = 0;
pub const HEADER_CURSOR_SIZE: usize = PTR_SIZE;

pub const FILE_DATA_START: usize = HEADER_CURSOR_START + HEADER_CURSOR_SIZE;





//! Sets constants for the disk layout


use std::mem::size_of;
use crate::error::Error;
use std::convert::TryFrom;

use crate::data::{IDENTIFIER_SIZE};


pub const PTR_SIZE: usize = size_of::<usize>(); 


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


//for TreeRecord
//TODO: make generic over different descriptor lengths
//
pub const INDEX_START: usize = 0;
pub const INDEX_SIZE: usize = 8;

pub const DESCRIPTOR_START: usize = INDEX_START + INDEX_SIZE;

//for generic Page
pub const PAGE_TYPE_OFFSET: usize = 0;
pub const PAGE_TYPE_SIZE: usize = 1;

pub const TAIL_OFFSET: usize = PAGE_TYPE_OFFSET + PAGE_TYPE_SIZE;
pub const TAIL_SIZE: usize = 4; //2 bytes supports up to 65535 records, but 4 is easier to implement lol

pub const IS_EMPTY_OFFSET: usize = TAIL_OFFSET + TAIL_SIZE;
pub const IS_EMPTY_SIZE: usize = 1;

pub const PAGE_DATA_START: usize = IS_EMPTY_OFFSET + IS_EMPTY_SIZE;

//for whole file
pub const HEADER_CURSOR_START: usize = 0;
pub const HEADER_CURSOR_SIZE: usize = PTR_SIZE;

pub const FILE_DATA_START: usize = HEADER_CURSOR_START + HEADER_CURSOR_SIZE;





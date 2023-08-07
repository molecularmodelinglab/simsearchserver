//! Holds structs and methods for representing nodes (both internal and leaf) of kd tree.
//!
//! Structs can be sent to and from fixed-length byte arrays to be swapped back and forth from disk
//!

use rand::prelude::*;

use crate::error::Error;
use crate::data::{Parser};
use crate::page::PageType;
use crate::layout;

use byteorder::{ByteOrder, BigEndian};
use std::fmt;

use std::convert::TryInto;

#[derive(Debug, Clone, PartialEq)]
pub enum PagePointer {
    Node(usize),
    Leaf(usize),
}

impl fmt::Display for PagePointer {

    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Write strictly the first element into the supplied output
        // stream: `f`. Returns `fmt::Result` which indicates whether the
        // operation succeeded or failed. Note that `write!` uses syntax which
        // is very similar to `println!`.
        match self {
            PagePointer::Node(node_offset) => {
                write!(f, "NODE {:?}|{:?}", PageType::Node, node_offset)
            },
            PagePointer::Leaf(page_address) => {
                write!(f, "LEAF {:?}|{:?}", PageType::Leaf, page_address)
            }
        }
    }
}


#[derive(Debug, PartialEq)]
pub enum NodeType{
    Internal,
    Leaf,
}

#[derive(Debug, PartialEq, Clone)]
pub struct InternalNode {

    pub left_child_pointer: PagePointer,
    pub right_child_pointer: PagePointer,
    pub split_axis: usize,
    pub split_value: f32,
}

impl InternalNode {

    pub fn default() -> Self {

        let node = Self {
            left_child_pointer: PagePointer::Node(0),
            right_child_pointer: PagePointer::Node(0),
            split_axis: 0,
            split_value: 0.0,

        };

        return node
    }

    pub fn pretty(&self) -> String {

        return format!("SA: {:?} SV: {:?}
                            LC: {}
                            RC: {}",
                        self.split_axis,
                        self.split_value,
                        self.left_child_pointer,
                        self.right_child_pointer)
    }

    pub fn from_slice(node_slice: &[u8]) -> Result<InternalNode, String> {

        let left_child_index = Parser::get_usize_from_array(node_slice, layout::LEFT_CHILD_INDEX_START, layout::LEFT_CHILD_INDEX_SIZE);
        let left_child_type = Parser::get_usize_from_array(node_slice, layout::LEFT_CHILD_TYPE_START, layout::LEFT_CHILD_TYPE_SIZE);

        let right_child_index = Parser::get_usize_from_array(node_slice, layout::RIGHT_CHILD_INDEX_START, layout::RIGHT_CHILD_INDEX_SIZE);
        let right_child_type = Parser::get_usize_from_array(node_slice, layout::RIGHT_CHILD_TYPE_START, layout::RIGHT_CHILD_TYPE_SIZE);

        let split_axis = Parser::get_usize_from_array(node_slice, layout::SPLIT_AXIS_OFFSET, layout::SPLIT_AXIS_SIZE);
        let split_value = Parser::get_f32_from_array(node_slice, layout::SPLIT_VALUE_OFFSET);

        let mut node = InternalNode::default();

        node.left_child_pointer = match left_child_type.unwrap() {
            1 => {PagePointer::Node(left_child_index.unwrap())},
            2 => {PagePointer::Leaf(left_child_index.unwrap())},
            _ => {panic!()},
        };

        node.right_child_pointer = match right_child_type.unwrap() {
            1 => {PagePointer::Node(right_child_index.unwrap())},
            2 => {PagePointer::Leaf(right_child_index.unwrap())},
            _ => {panic!()},
        };

        node.split_axis = split_axis.unwrap();
        node.split_value = split_value.unwrap();

        Ok(node)
    }

    pub fn to_arr(&self) -> [u8; layout::NODE_SIZE] {

        let mut arr: [u8; layout::NODE_SIZE] = [0; layout::NODE_SIZE];

        let slice = &mut arr[layout::LEFT_CHILD_INDEX_START..layout::LEFT_CHILD_INDEX_START + layout::LEFT_CHILD_INDEX_SIZE];

        let (node_type, value) = match self.left_child_pointer {
            PagePointer::Node(x) => (1, x.try_into().unwrap()),
            PagePointer::Leaf(x) => (2, x.try_into().unwrap()),
        };
        BigEndian::write_u64(slice, value);


        arr[layout::LEFT_CHILD_TYPE_START] = node_type as u8;

        let slice = &mut arr[layout::RIGHT_CHILD_INDEX_START..layout::RIGHT_CHILD_INDEX_START + layout::RIGHT_CHILD_INDEX_SIZE];

        let (node_type, value) = match self.right_child_pointer {
            PagePointer::Node(x) => (1, x.try_into().unwrap()),
            PagePointer::Leaf(x) => (2, x.try_into().unwrap()),
        };
        BigEndian::write_u64(slice, value);

        arr[layout::RIGHT_CHILD_TYPE_START] = node_type as u8;


        arr[layout::SPLIT_AXIS_OFFSET] = self.split_axis as u8;

        let slice = &mut arr[layout::SPLIT_VALUE_OFFSET..layout::SPLIT_VALUE_OFFSET + layout::SPLIT_VALUE_SIZE];
        BigEndian::write_f32(slice, self.split_value.try_into().unwrap());

        return arr
    }
}


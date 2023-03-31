//! Holds structs and methods for representing nodes (both internal and leaf) of kd tree.
//!
//! Structs can be sent to and from fixed-length byte arrays to be swapped back and forth from disk
//!

use rand::prelude::*;

use crate::error::Error;
use crate::page::PageType;
use crate::layout;

use byteorder::{ByteOrder, BigEndian};
use ascii::{AsAsciiStr, AsciiString};
use std::cmp;

#[derive(Debug, PartialEq, Clone)]
pub struct PageAddress(pub usize);

impl PageAddress {
    
    pub fn as_actual_address(&self) -> u64 {
        return (self.0 * layout::PAGE_SIZE) as u64;
    }
}



#[derive(Debug, PartialEq, Clone)]
pub struct NodeOffset(pub usize);

#[derive(Debug, PartialEq)]
pub enum NodeType{
    Internal,
    Leaf,
}


#[derive(Debug, PartialEq, Clone)]
pub struct Descriptor {
    pub data: [f32; layout::DESCRIPTOR_LENGTH],
}
impl Descriptor {

    pub fn distance(&self, other: &Descriptor) -> f32 {

        let mut sum: f32 = 0.0;
        for i in 0..layout::DESCRIPTOR_LENGTH {
            sum += f32::powf(self.data[i] - other.data[i], 2.0);

        }

        let result = f32::powf(sum, 0.5);
        //println!("DISTANCE: {:?}", result);
        return result;
    
    }

    pub fn from_vec(v: Vec<f32>) -> Self {

        return Self {
            data: v.try_into().unwrap(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct InternalNode {

    pub parent_page_address: PageAddress,
    pub parent_node_offset: NodeOffset,
    pub left_child_page_address: PageAddress,
    pub left_child_node_offset: NodeOffset,
    pub left_child_type: PageType,
    pub right_child_page_address: PageAddress,
    pub right_child_node_offset: NodeOffset,
    pub right_child_type: PageType,
    pub split_axis: usize,
    pub split_value: f32,
}


pub const IDENTIFIER_SIZE: usize = 16;
pub const DESCRIPTOR_SIZE: usize = 32;

//pub struct CompoundIdentifier(pub String);

#[derive(Debug, PartialEq, Clone)]
pub struct CompoundIdentifier(pub [u8; IDENTIFIER_SIZE]);


impl CompoundIdentifier {

    pub fn from_string(s: String) -> Self {

        assert!(s.len() <= 16);

        return Self::from_str(&s);
    }

    pub fn from_str(data: &str) -> Self {

        let mut fill_arr = [0u8; IDENTIFIER_SIZE];

        let bytes = data.as_bytes();

        
        fill_arr[..bytes.len()].copy_from_slice(bytes);
        //dbg!(&bytes.len());
        let s: [u8;IDENTIFIER_SIZE] = fill_arr.try_into().expect("slice with incorrect length");

        return Self(s);
    }

    pub fn from_ascii_array(data: &[u8], offset: usize, length: usize) -> Self {

        let bytes = &data[offset..offset + length];
        let s: [u8;IDENTIFIER_SIZE] = bytes.try_into().expect("slice with incorrect length");
        return Self(s);

    }

    pub fn random() -> Self {

        let mut rng = rand::thread_rng();
        let mut bytes = [0u8; IDENTIFIER_SIZE];


        for x in &mut bytes {
            *x = rng.gen_range(60..80);
        }

        //rng.fill(&mut bytes[..]);

        return Self(bytes);
    }

}

#[derive(Debug, PartialEq, Clone)]
pub struct CompoundRecord {
    pub dataset_identifier: u8,
    pub compound_identifier: CompoundIdentifier,
    pub descriptor: Descriptor,
}

pub fn coerce_(value: &[u8]) -> [u8; 1] {
    value.try_into().expect("slice with incorrect length")
}

pub fn coerce_byte(value: &[u8]) -> [u8; 1] {
    value.try_into().expect("slice with incorrect length")
}

pub fn coerce_pointer(value: &[u8]) -> [u8; layout::PTR_SIZE] {
    value.try_into().expect("slice with incorrect length")
}

pub fn coerce_f32(value: &[u8]) -> [u8; 4] {
    value.try_into().expect("slice with incorrect length")
}


pub fn get_usize_from_array(data: &[u8], offset: usize, length: usize) -> Result<usize, Error> {

        match length {
            1 => {
                let bytes = &data[offset..offset + length];
                let known_size_array = coerce_byte(bytes);
                let attempted_usize = layout::Value::try_from(known_size_array);
                let layout::Value(value) = attempted_usize.unwrap();
                Ok(value)
            },
            8 => {
                let bytes = &data[offset..offset + length];
                let known_size_array = coerce_pointer(bytes);
                let attempted_usize = layout::Value::try_from(known_size_array);
                let layout::Value(value) = attempted_usize.unwrap();
                Ok(value)
            },
            _ => panic!(),
        }
    }

 pub fn get_f32_from_array(data: &[u8], offset: usize) -> Result<f32, Error> {
        let bytes = &data[offset..offset + 4];
        let known_size_array = coerce_f32(bytes);
        let attempted_f32 = BigEndian::read_f32(&known_size_array);
        Ok(attempted_f32)
    }

 pub fn get_descriptor_from_array(data: &[u8], offset: usize, _length: usize) -> Result<Descriptor, Error> {

        let num_iters = layout::DESCRIPTOR_LENGTH;

        let mut curr_offset: usize = offset;
        //let mut values: Vec<f32> = Vec::new();
        let mut arr: [f32; layout::DESCRIPTOR_LENGTH] = [0.0; layout::DESCRIPTOR_LENGTH];
        //let mut values: Vec<f32> = Vec::with_capacity(layout::DESCRIPTOR_LENGTH);
        for i in 0..num_iters {
            let bytes = &data[curr_offset..curr_offset + 4];
            let known_size_array = coerce_f32(bytes);
            let attempted_f32 = BigEndian::read_f32(&known_size_array);
            //values.push(attempted_f32);
            arr[i] = attempted_f32;
            curr_offset += 4;
        }

        //let arr: [f32; layout::DESCRIPTOR_LENGTH] = vec_to_array(values);

        let desc = Descriptor { data: arr };
        Ok(desc)
    }

use std::convert::TryInto;

fn vec_to_array<T, const N: usize>(v: Vec<T>) -> [T; N] {
    v.try_into()
        .unwrap_or_else(|v: Vec<T>| panic!("Expected a Vec of length {} but it was {}", N, v.len()))
}

impl CompoundRecord {


    pub fn default() -> Self {

        return Self {
            dataset_identifier: 0,
            compound_identifier: CompoundIdentifier::from_str("defaultname"),
            descriptor: Descriptor {data: [0.0; layout::DESCRIPTOR_LENGTH]},
        }
    }

    //TODO: handle trailing whitespace
    pub fn from_slice(record_slice: &[u8]) -> Result<Self, String> {

        let dataset_identifier = get_usize_from_array(record_slice, layout::DATASET_IDENTIFIER_START, layout::DATASET_IDENTIFIER_SIZE).unwrap();
        //let compound_identifier = get_usize_from_array(record_slice, layout::COMPOUND_IDENTIFIER_SIZE, layout::COMPOUND_IDENTIFIER_SIZE).unwrap();
        let compound_identifier = CompoundIdentifier::from_ascii_array(record_slice, layout::COMPOUND_IDENTIFIER_START, layout::COMPOUND_IDENTIFIER_SIZE);
        //let compound_identifier = CompoundIdentifier("ayy".to_string());
        let descriptor = get_descriptor_from_array(record_slice, layout::DESCRIPTOR_START, layout::DESCRIPTOR_SIZE).unwrap();

        return Ok (Self {
            dataset_identifier: dataset_identifier as u8,
            compound_identifier,
            descriptor,
        })
    }


    pub fn to_arr(&self) -> [u8; layout::COMPOUND_RECORD_SIZE] {

        let mut arr: [u8; layout::COMPOUND_RECORD_SIZE] = [0; layout::COMPOUND_RECORD_SIZE];

        arr[0] = self.dataset_identifier;


        let s: AsciiString = AsciiString::from_ascii(self.compound_identifier.0.clone()).unwrap();
        let b = s.as_bytes();

        //if the string we're copying is shorter than the max, only copy that many bytes
        let length = cmp::min(b.len(), layout::COMPOUND_IDENTIFIER_SIZE);
        let slice = &mut arr[layout::COMPOUND_IDENTIFIER_START..layout::COMPOUND_IDENTIFIER_START + length];

        slice.copy_from_slice(b);

        let mut curr_offset = layout::DESCRIPTOR_START;
        for i in 0..self.descriptor.data.len() {

            let slice = &mut arr[curr_offset..curr_offset + 4];
            BigEndian::write_f32(slice, self.descriptor.data[i]);

            curr_offset += 4;
        }
        
        return arr

    }




}

impl InternalNode {

    pub fn default() -> Self {

        let node = Self {
            //node_type: NodeType::Internal,
            parent_page_address: PageAddress(0),
            parent_node_offset: NodeOffset(0),
            left_child_page_address: PageAddress(0),
            left_child_node_offset: NodeOffset(0),
            left_child_type: PageType::Node,
            right_child_page_address: PageAddress(0),
            right_child_node_offset: NodeOffset(0),
            right_child_type: PageType::Node,
            split_axis: 0,
            split_value: 0.0,

        };

        return node
    }

    pub fn pretty(&self) -> String {

        return format!("LC: {:?}|{:?}|{:?} RC: {:?}|{:?}|{:?}| SA: {:?} SV: {:?}",
                        self.left_child_type,
                        self.left_child_page_address.0,
                        self.left_child_node_offset.0,
                        self.right_child_type,
                        self.right_child_page_address.0,
                        self.right_child_node_offset.0,
                        self.split_axis,
                        self.split_value);
    }

    pub fn from_slice(node_slice: &[u8]) -> Result<InternalNode, String> {

        //let node_type = get_usize_from_array(node_slice, layout::NODE_TYPE_OFFSET, layout::NODE_TYPE_SIZE);
        let parent_page_address = get_usize_from_array(node_slice, layout::PARENT_PAGE_START, layout::PARENT_PAGE_SIZE);
        let parent_node_offset = get_usize_from_array(node_slice, layout::PARENT_NODE_OFFSET_START, layout::PARENT_NODE_OFFSET_SIZE);

        let left_child_page_address = get_usize_from_array(node_slice, layout::LEFT_CHILD_PAGE_START, layout::LEFT_CHILD_PAGE_SIZE);
        let left_child_node_offset = get_usize_from_array(node_slice, layout::LEFT_CHILD_NODE_OFFSET_START, layout::LEFT_CHILD_NODE_OFFSET_SIZE);
        let left_child_type = get_usize_from_array(node_slice, layout::LEFT_CHILD_TYPE_START, layout::LEFT_CHILD_TYPE_SIZE);

        let right_child_page_address = get_usize_from_array(node_slice, layout::RIGHT_CHILD_PAGE_START, layout::RIGHT_CHILD_PAGE_SIZE);
        let right_child_node_offset = get_usize_from_array(node_slice, layout::RIGHT_CHILD_NODE_OFFSET_START, layout::RIGHT_CHILD_NODE_OFFSET_SIZE);
        let right_child_type = get_usize_from_array(node_slice, layout::RIGHT_CHILD_TYPE_START, layout::RIGHT_CHILD_TYPE_SIZE);

        let split_axis = get_usize_from_array(node_slice, layout::SPLIT_AXIS_OFFSET, layout::SPLIT_AXIS_SIZE);
        let split_value = get_f32_from_array(node_slice, layout::SPLIT_VALUE_OFFSET);

        let mut node = InternalNode::default();

        /*
        node.node_type = match node_type.unwrap() {
            0 => NodeType::Internal,
            1 => NodeType::Leaf,
            _ => panic!(),
        };
        */

        node.parent_page_address = PageAddress(parent_page_address.unwrap());
        node.parent_node_offset = NodeOffset(parent_node_offset.unwrap());

        node.left_child_page_address = PageAddress(left_child_page_address.unwrap());
        node.left_child_node_offset = NodeOffset(left_child_node_offset.unwrap());
        //dbg!(&left_child_type);
        node.left_child_type = match left_child_type.unwrap() {
            1 => {PageType::Node},
            2 => {PageType::Leaf},
            _ => {
                panic!()},

        };

        node.right_child_page_address = PageAddress(right_child_page_address.unwrap());
        node.right_child_node_offset = NodeOffset(right_child_node_offset.unwrap());
        node.right_child_type = match right_child_type.unwrap() {
            1 => {PageType::Node},
            2 => {PageType::Leaf},
            _ => {panic!()},

        };

        node.split_axis = split_axis.unwrap();
        node.split_value = split_value.unwrap();

        Ok(node)
    }

    pub fn to_arr(&self) -> [u8; layout::NODE_SIZE] {

        let mut arr: [u8; layout::NODE_SIZE] = [0; layout::NODE_SIZE];

        /*
        arr[0] = match self.node_type {
            NodeType::Internal => 0,
            NodeType::Leaf => 1,
        };
        */

        let slice = &mut arr[layout::PARENT_PAGE_START..layout::PARENT_PAGE_START + layout::PARENT_PAGE_SIZE];
        BigEndian::write_u64(slice, self.parent_page_address.0.try_into().unwrap());

        arr[layout::PARENT_NODE_OFFSET_START] = self.parent_node_offset.0 as u8;

        let slice = &mut arr[layout::LEFT_CHILD_PAGE_START..layout::LEFT_CHILD_PAGE_START + layout::LEFT_CHILD_PAGE_SIZE];
        let value: u64 = self.left_child_page_address.0.try_into().unwrap();
        BigEndian::write_u64(slice, value);

        let value = self.left_child_node_offset.0 as u8;
        arr[layout::LEFT_CHILD_NODE_OFFSET_START] = value;

        arr[layout::LEFT_CHILD_TYPE_START] = self.left_child_type.clone() as u8;

        let slice = &mut arr[layout::RIGHT_CHILD_PAGE_START..layout::RIGHT_CHILD_PAGE_START + layout::RIGHT_CHILD_PAGE_SIZE];
        BigEndian::write_u64(slice, self.right_child_page_address.0.try_into().unwrap());

        arr[layout::RIGHT_CHILD_NODE_OFFSET_START] = self.right_child_node_offset.0 as u8;
        arr[layout::RIGHT_CHILD_TYPE_START] = self.right_child_type.clone() as u8;

        arr[layout::SPLIT_AXIS_OFFSET] = self.split_axis as u8;

        let slice = &mut arr[layout::SPLIT_VALUE_OFFSET..layout::SPLIT_VALUE_OFFSET + layout::SPLIT_VALUE_SIZE];
        BigEndian::write_f32(slice, self.split_value.try_into().unwrap());

        return arr

    }

}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quick_node_from_slice_works() {

        let page_data: [u8; layout::NODE_SIZE] = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, // PARENT_OFFSET
            0x02,                                           // PARENT_INTERNAL_OFFSET
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, // LEFT_CHILD_OFFSET
            0x03,                                           // LEFT_CHILD_INTERNAL_OFFSET
            0x01,                                           // LEFT_CHILD_TYPE
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, // RIGHT_CHILD_OFFSET
            0x04,                                           // RIGHT_CHILD_INTERNAL_OFFSET
            0x01,                                           // RIGHT_CHILD_TYPE
            0x10,                                           // SPLIT_AXIS
            0x40, 0xdc, 0xcc, 0xcd,                         // SPLIT_VALUE
        ];

        let node = InternalNode::from_slice(&page_data).unwrap();

        let true_node = InternalNode {
            parent_page_address: PageAddress(256),
            parent_node_offset: NodeOffset(2),
            left_child_page_address: PageAddress(512),
            left_child_node_offset: NodeOffset(3),
            left_child_type: PageType::Node,
            right_child_page_address: PageAddress(1024),
            right_child_node_offset: NodeOffset(4),
            right_child_type: PageType::Node,
            split_axis: 16,
            split_value: 6.9,
        };

        assert_eq!(node, true_node);
    }

    #[test]
    fn quick_node_to_array_and_back_works() {

        let mut node = InternalNode::default();

        node.parent_page_address = PageAddress(100);
        node.parent_node_offset = NodeOffset(2);
        node.left_child_page_address = PageAddress(300);
        node.left_child_node_offset = NodeOffset(4);
        node.right_child_page_address = PageAddress(500);
        node.right_child_node_offset = NodeOffset(6);
        node.split_axis = 7;
        node.split_value = 8.8888;

        let arr = node.to_arr();

        let new_node = InternalNode::from_slice(&arr);

        assert_eq!(new_node.unwrap(), node);
    }

    #[test]
    fn quick_compound_record_from_array_works() {

        let page_data: [u8; layout::COMPOUND_RECORD_SIZE] = [
            0x01,                                           // DATASET_IDENTIFIER
            0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, // COMPOUND_IDENTIFIER
            0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50, // COMPOUND_IDENTIFIER
            0x40, 0xdc, 0xcc, 0xcd,                         // DESCRIPTOR 1
            0x40, 0xdc, 0xcc, 0xcd,                         // DESCRIPTOR 2
            0x40, 0xdc, 0xcc, 0xcd,                         // DESCRIPTOR 3
            0x40, 0xdc, 0xcc, 0xcd,                         // DESCRIPTOR 4
            0x40, 0xdc, 0xcc, 0xcd,                         // DESCRIPTOR 5
            0x40, 0xdc, 0xcc, 0xcd,                         // DESCRIPTOR 6
            0x40, 0xdc, 0xcc, 0xcd,                         // DESCRIPTOR 7
            0x40, 0xdc, 0xcc, 0xcd,                         // DESCRIPTOR 8
        ];

        let cr = CompoundRecord::from_slice(&page_data).unwrap();

        let true_cr = CompoundRecord {
            dataset_identifier: 1,
            compound_identifier: CompoundIdentifier::from_str("ABCDEFGHIJKLMNOP"),
            descriptor: Descriptor {
                data:  [6.9,6.9,6.9,6.9,
                        6.9,6.9,6.9,6.9],
            },

        };

        assert_eq!(cr, true_cr);


    }
 
    #[test]
    fn quick_compound_record_to_array_and_back_works() {

        let descriptor_array: [f32; layout::DESCRIPTOR_LENGTH] = 
                                        [1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8];
        let cr = CompoundRecord {
            dataset_identifier: 0,
            compound_identifier: CompoundIdentifier::from_str("ayy"),
            descriptor: Descriptor{data: descriptor_array},
        };

        let arr = cr.to_arr();

        let new_cr = CompoundRecord::from_slice(&arr).unwrap();

        assert_eq!(cr, new_cr);

    }

    #[test]
    fn get_sizes()
    {
        dbg![layout::NODE_SIZE];
        dbg![layout::COMPOUND_RECORD_SIZE];
    }
    }
    




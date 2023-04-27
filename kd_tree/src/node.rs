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
use std::fmt;

use std::convert::TryInto;

#[derive(Debug, PartialEq, Clone)]
pub struct PageAddress(pub usize);

impl PageAddress {
    pub fn default() -> Self {
        return Self(0);
    }
}

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



/*
impl PageAddress {
    
    pub fn as_actual_address(&self) -> u64 {
        return (self.0 * layout::PAGE_SIZE) as u64;
    }
}
*/



#[derive(Debug, PartialEq)]
pub enum NodeType{
    Internal,
    Leaf,
}


#[derive(Debug, PartialEq, Clone)]
pub struct Descriptor {
    pub data: Vec<f32>,
    pub length: usize,
    //pub data: [f32; N],
}
impl Descriptor {

    pub fn distance(&self, other: &Descriptor) -> f32 {

        let mut sum: f32 = 0.0;
        for i in 0..self.length {
            sum += f32::powf(self.data[i] - other.data[i], 2.0);

        }

        let result = f32::powf(sum, 0.5);
        //println!("DISTANCE: {:?}", result);
        return result;
    
    }

    pub fn random(length: usize) -> Self {

        let random_vec: Vec::<f32> = (0..length).map(|_| rand::random::<f32>()).collect();
        return Self { data: random_vec, length: length };


    }

    pub fn from_vec(v: Vec<f32>, length: usize) -> Self {

        assert!(v.len() == length);
        return Self {
            data: v.try_into().unwrap(),
            length,
        }
    }

    pub fn yaml(&self) -> String {

        let mut s = "[".to_string();
        for (i, item) in self.data.iter().enumerate() {
            if i != 0 { s += ","; }
            s += &format!("{:.3}", item);
        }
        s += "]";

        return s;
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct InternalNode {

    pub left_child_pointer: PagePointer,
    pub right_child_pointer: PagePointer,
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
            *x = rng.gen_range(65..91);
        }

        //rng.fill(&mut bytes[..]);

        return Self(bytes);
    }

    pub fn to_string(&self) -> String {

        let identifier_str: AsciiString = AsciiString::from_ascii(self.0.clone()).unwrap();
        let identifier_string = String::from(identifier_str);
        
        return identifier_string;
    }

}

#[derive(Debug, PartialEq, Clone)]
pub struct CompoundRecord {
    pub dataset_identifier: u8,
    pub compound_identifier: CompoundIdentifier,
    pub descriptor: Descriptor,
    pub length: usize,
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

struct Parser {
}

impl Parser{

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

     pub fn get_descriptor_from_array(data: &[u8], offset: usize, length: usize) -> Result<Descriptor, Error> {


            let mut curr_offset: usize = offset;
            //let mut values: Vec<f32> = Vec::new();
            //let mut arr: [f32; length] = [0.0; length];
            let mut vec: Vec<f32> = Vec::with_capacity(length);
            //let mut values: Vec<f32> = Vec::with_capacity(layout::DESCRIPTOR_LENGTH);
            for i in 0..length {
                let bytes = &data[curr_offset..curr_offset + 4];
                let known_size_array = coerce_f32(bytes);
                let attempted_f32 = BigEndian::read_f32(&known_size_array);
                //values.push(attempted_f32);
                //arr[i] = attempted_f32;
                vec.push(attempted_f32);
                curr_offset += 4;
            }

            let desc = Descriptor { data: vec, length: length};
            Ok(desc)
        }


    fn vec_to_array<T, const M: usize>(v: Vec<T>) -> [T; M] {
        v.try_into()
            .unwrap_or_else(|v: Vec<T>| panic!("Expected a Vec of length {} but it was {}", M, v.len()))
    }

}

impl CompoundRecord {


    pub fn default(length: usize) -> Self {

        return Self {
            dataset_identifier: 0,
            compound_identifier: CompoundIdentifier::from_str("defaultname"),
            descriptor: Descriptor {data: vec![0.0; length], length},
            length: length
        };
    }
    
    pub fn random(length: usize) -> CompoundRecord {

        let descriptor = Descriptor::random(length);
        let identifier = CompoundIdentifier::random();

        let cr = CompoundRecord {
            dataset_identifier: 0,
            compound_identifier: identifier,
            descriptor,
            length,
        };

        return cr;
    }

    pub fn get_descriptor_size(&self) -> usize {
        let descriptor_size: usize = self.length * 4;
        return descriptor_size;
    }

    pub fn get_record_size(&self) -> usize {
        let record_size = layout::DATASET_IDENTIFIER_SIZE + layout::COMPOUND_IDENTIFIER_SIZE + self.get_descriptor_size();
        return record_size;

    }

    pub fn compute_record_size(length: usize) -> usize {
        let record_size = layout::DATASET_IDENTIFIER_SIZE + layout::COMPOUND_IDENTIFIER_SIZE + (length * 4);
        return record_size;

    }


    //TODO: handle trailing whitespace
    pub fn from_slice(record_slice: &[u8], length: usize) -> Result<Self, String> {

        /*
        const DATASET_IDENTIFIER_START: usize = 0;
        const DATASET_IDENTIFIER_SIZE: usize = 1;

        const COMPOUND_IDENTIFIER_START: usize = DATASET_IDENTIFIER_START + DATASET_IDENTIFIER_SIZE;
        const COMPOUND_IDENTIFIER_SIZE: usize = 16;

        const DESCRIPTOR_START: usize = COMPOUND_IDENTIFIER_START + COMPOUND_IDENTIFIER_SIZE;

        const COMPOUND_RECORD_SIZE: usize = DESCRIPTOR_START + DESCRIPTOR_SIZE;
        */

        let dataset_identifier = Parser::get_usize_from_array(record_slice, layout::DATASET_IDENTIFIER_START, layout::DATASET_IDENTIFIER_SIZE).unwrap();
        let compound_identifier = CompoundIdentifier::from_ascii_array(record_slice, layout::COMPOUND_IDENTIFIER_START, layout::COMPOUND_IDENTIFIER_SIZE);
        //let compound_identifier = CompoundIdentifier("ayy".to_string());
        let descriptor = Parser::get_descriptor_from_array(record_slice, layout::DESCRIPTOR_START, length).unwrap();

        return Ok (Self {
            dataset_identifier: dataset_identifier as u8,
            compound_identifier,
            descriptor,
            length,
        })
    }


    pub fn to_vec(&self) -> Vec<u8> {

        let mut vec: Vec<u8> = Vec::with_capacity(self.get_record_size());

        vec.push(self.dataset_identifier as u8);

        let s: AsciiString = AsciiString::from_ascii(self.compound_identifier.0.clone()).unwrap();
        let b = s.as_bytes();

        let mut arr = [0u8; layout::COMPOUND_IDENTIFIER_SIZE];
        arr.copy_from_slice(b);

        vec.extend_from_slice(&arr);

        let mut curr_offset = layout::DESCRIPTOR_START;
        for i in 0..self.length {

            let mut slice = [0u8; 4];
            BigEndian::write_f32(&mut slice, self.descriptor.data[i]);
            vec.extend_from_slice(&slice);

        }

        assert!(vec.len() == self.get_record_size());
        
        return vec;
    }
    /*
    pub fn to_arr(&self) -> [u8; N + 17] {

        const SIZE: usize = Self::get_record_size();

        let mut arr: [u8; Self::get_record_size()] = [0; Self::get_record_size()];

        arr[0] = self.dataset_identifier;


        let s: AsciiString = AsciiString::from_ascii(self.compound_identifier.0.clone()).unwrap();
        let b = s.as_bytes();

        //if the string we're copying is shorter than the max, only copy that many bytes
        let length = cmp::min(b.len(), layout::COMPOUND_IDENTIFIER_SIZE);
        let slice = &mut arr[layout::COMPOUND_IDENTIFIER_START..layout::COMPOUND_IDENTIFIER_START + length];

        slice.copy_from_slice(b);

        let mut curr_offset = layout::DESCRIPTOR_START;
        for i in 0..N {

            let slice = &mut arr[curr_offset..curr_offset + 4];
            BigEndian::write_f32(slice, self.descriptor.data[i]);

            curr_offset += 4;
        }
        
        return arr

    }
    */




}

impl InternalNode {

    pub fn default() -> Self {

        let node = Self {
            //node_type: NodeType::Internal,
            //parent_page_address: PageAddress(0),
            //parent_node_offset: ItemOffset(0),
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

        //let node_type = get_usize_from_array(node_slice, layout::NODE_TYPE_OFFSET, layout::NODE_TYPE_SIZE);
        //let parent_page_address = Parser::get_usize_from_array(node_slice, layout::PARENT_PAGE_START, layout::PARENT_PAGE_SIZE);
        //let parent_node_offset = Parser::get_usize_from_array(node_slice, layout::PARENT_NODE_OFFSET_START, layout::PARENT_NODE_OFFSET_SIZE);

        let left_child_index = Parser::get_usize_from_array(node_slice, layout::LEFT_CHILD_INDEX_START, layout::LEFT_CHILD_INDEX_SIZE);
        let left_child_type = Parser::get_usize_from_array(node_slice, layout::LEFT_CHILD_TYPE_START, layout::LEFT_CHILD_TYPE_SIZE);

        let right_child_index = Parser::get_usize_from_array(node_slice, layout::RIGHT_CHILD_INDEX_START, layout::RIGHT_CHILD_INDEX_SIZE);
        let right_child_type = Parser::get_usize_from_array(node_slice, layout::RIGHT_CHILD_TYPE_START, layout::RIGHT_CHILD_TYPE_SIZE);

        let split_axis = Parser::get_usize_from_array(node_slice, layout::SPLIT_AXIS_OFFSET, layout::SPLIT_AXIS_SIZE);
        let split_value = Parser::get_f32_from_array(node_slice, layout::SPLIT_VALUE_OFFSET);

        let mut node = InternalNode::default();

        /*
        node.node_type = match node_type.unwrap() {
            0 => NodeType::Internal,
            1 => NodeType::Leaf,
            _ => panic!(),
        };
        */

        //node.parent_page_address = PageAddress(parent_page_address.unwrap());
        //node.parent_node_offset = ItemOffset(parent_node_offset.unwrap() as u32);
        //

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

        /*
        arr[0] = match self.node_type {
            NodeType::Internal => 0,
            NodeType::Leaf => 1,
        };
        */

        //let slice = &mut arr[layout::PARENT_PAGE_START..layout::PARENT_PAGE_START + layout::PARENT_PAGE_SIZE];
        //BigEndian::write_u64(slice, self.parent_page_address.0.try_into().unwrap());

        //arr[layout::PARENT_NODE_OFFSET_START] = self.parent_node_offset.0 as u8;

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
#[cfg(test)]
mod tests {
    use super::*;

    /*
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
            parent_node_offset: ItemOffset(2),
            left_child_page_address: PageAddress(512),
            left_child_node_offset: ItemOffset(3),
            left_child_type: PageType::Node,
            right_child_page_address: PageAddress(1024),
            right_child_node_offset: ItemOffset(4),
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
        node.parent_node_offset = ItemOffset(2);
        node.left_child_page_address = PageAddress(300);
        node.left_child_node_offset = ItemOffset(4);
        node.right_child_page_address = PageAddress(500);
        node.right_child_node_offset = ItemOffset(6);
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
                //data:  [6.9,6.9,6.9,6.9,
                //        6.9,6.9,6.9,6.9],
                data: [6.9; layout::DESCRIPTOR_LENGTH],
            },

        };

        assert_eq!(cr, true_cr);


    }
 
    #[test]
    fn quick_compound_record_to_array_and_back_works() {

        let descriptor_array: [f32; layout::DESCRIPTOR_LENGTH] = 
                                        [1.10101; layout::DESCRIPTOR_LENGTH];
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
    

    */
    #[test]
    fn generic_descriptor() {

    const n: usize = 8;
    let d = Descriptor {
                //data:  [6.9,6.9,6.9,6.9,
                //        6.9,6.9,6.9,6.9],
                data: vec![6.9; n], 
                length: n
            };
    }

}





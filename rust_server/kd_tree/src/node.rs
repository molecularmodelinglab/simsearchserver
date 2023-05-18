//! Holds structs and methods for representing nodes (both internal and leaf) of kd tree.
//!
//! Structs can be sent to and from fixed-length byte arrays to be swapped back and forth from disk
//!

use rand::prelude::*;

use crate::error::Error;
use crate::page::PageType;
use crate::layout;

use byteorder::{ByteOrder, BigEndian};
use ascii::AsciiString;
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
        return result;
    
    }

    pub fn random(length: usize) -> Self {

        let random_vec: Vec::<f32> = (0..length).map(|_| rand::random::<f32>()).collect();
        return Self { data: random_vec, length};


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

#[derive(PartialEq, Clone)]
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

        return Self(bytes);
    }

    pub fn to_string(&self) -> String {

        let identifier_str: AsciiString = AsciiString::from_ascii(self.0.clone()).unwrap();
        let identifier_string = String::from(identifier_str);
        
        return identifier_string;
    }
}

// To use the `{}` marker, the trait `fmt::Display` must be implemented
// manually for the type.
impl fmt::Debug for CompoundIdentifier {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Write strictly the first element into the supplied output
        // stream: `f`. Returns `fmt::Result` which indicates whether the
        // operation succeeded or failed. Note that `write!` uses syntax which
        // is very similar to `println!`.
        write!(f, "{}", self.to_string())
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

struct Parser {}

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

            let mut vec: Vec<f32> = Vec::with_capacity(length);

            for _ in 0..length {
                let bytes = &data[curr_offset..curr_offset + 4];
                let known_size_array = coerce_f32(bytes);
                let attempted_f32 = BigEndian::read_f32(&known_size_array);
                vec.push(attempted_f32);
                curr_offset += 4;
            }

            let desc = Descriptor { data: vec, length};
            Ok(desc)
        }
}

impl CompoundRecord {


    pub fn default(length: usize) -> Self {

        return Self {
            dataset_identifier: 0,
            compound_identifier: CompoundIdentifier::from_str("defaultname"),
            descriptor: Descriptor {data: vec![0.0; length], length},
            length,
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

        let dataset_identifier = Parser::get_usize_from_array(record_slice, layout::DATASET_IDENTIFIER_START, layout::DATASET_IDENTIFIER_SIZE).unwrap();
        let compound_identifier = CompoundIdentifier::from_ascii_array(record_slice, layout::COMPOUND_IDENTIFIER_START, layout::COMPOUND_IDENTIFIER_SIZE);

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

        for i in 0..self.length {

            let mut slice = [0u8; 4];
            BigEndian::write_f32(&mut slice, self.descriptor.data[i]);
            vec.extend_from_slice(&slice);

        }

        assert!(vec.len() == self.get_record_size());
        
        return vec;
    }
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


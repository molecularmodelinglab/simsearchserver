use byteorder::{ByteOrder, BigEndian};
use std::fmt;
use ascii::AsciiString;
use rand::{distributions::Alphanumeric, Rng};
use crate::layout;
use crate::tree::TreeRecord;
use crate::error::{Error};

pub type CompoundIndex = u64;

pub const IDENTIFIER_SIZE: usize = 30;

pub const MAX_SMILES_LENGTH: usize = 100;
pub const MAX_IDENTIFIER_LENGTH: usize = 30;

#[derive(Debug, PartialEq, Clone)]
pub struct CompoundRecord {
    pub smiles: String,
    pub compound_identifier: CompoundIdentifier,
    pub descriptor: Descriptor,
    pub length: usize,
}

impl CompoundRecord {


    pub fn new(dataset_identifier: u8, smiles: String, compound_identifier: CompoundIdentifier, descriptor: Descriptor, length: usize) -> Self {

        return Self {
            smiles,
            compound_identifier,
            descriptor,
            length,
        }
    }

    pub fn get_tree_record(&self, idx: &CompoundIndex) -> TreeRecord {

        let mut tree_record = TreeRecord::default(self.length);
        tree_record.index = *idx;
        tree_record.descriptor = self.descriptor.clone();
        return tree_record;
    }

    pub fn random(length: usize) -> Self {

        let smiles: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(20)
            .map(char::from)
            .collect();

        let compound_identifier = CompoundIdentifier::random();
        let descriptor = Descriptor::random(length);

        return Self {
            smiles,
            compound_identifier,
            descriptor,
            length,
        }
    }
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

#[derive(PartialEq, Clone)]
pub struct CompoundIdentifier(pub [u8; IDENTIFIER_SIZE]);

impl CompoundIdentifier {

    pub fn from_string(s: String) -> Self {

        assert!(s.len() <= IDENTIFIER_SIZE);

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


pub struct Parser {}

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

pub fn coerce_byte(value: &[u8]) -> [u8; 1] {
    value.try_into().expect("slice with incorrect length")
}

pub fn coerce_pointer(value: &[u8]) -> [u8; layout::PTR_SIZE] {
    value.try_into().expect("slice with incorrect length")
}

pub fn coerce_f32(value: &[u8]) -> [u8; 4] {
    value.try_into().expect("slice with incorrect length")
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn make_compound_record() {

        let smiles = "C1=CC=C(C=C1)C(=O)O".to_string();
        let identifier = CompoundIdentifier::from_string("it's a molecule".to_string());
        let dataset_identifier = 1;
        let length = 8;
        let descriptor = Descriptor::from_vec(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], length);

        let cr = CompoundRecord::new(dataset_identifier, smiles, identifier, descriptor, length);

        dbg!(&cr);

        let tr = cr.get_tree_record(&3);

        dbg!(&tr);


    }



}


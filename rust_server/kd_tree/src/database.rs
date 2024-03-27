use std::fs::{File, OpenOptions};
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use kdam::tqdm;

use rand::Rng;

use crate::data::{MAX_SMILES_LENGTH, MAX_IDENTIFIER_LENGTH, CompoundRecord, CompoundIdentifier};


pub const ENTRIES_START: usize = 0;
pub const ENTRIES_SIZE: usize = 8;

pub const SMILES_START: usize = ENTRIES_SIZE;
pub const SMILES_SIZE: usize = MAX_SMILES_LENGTH;

pub const ID_START: usize = SMILES_START + SMILES_SIZE;
pub const ID_SIZE: usize = MAX_IDENTIFIER_LENGTH;

pub const DATABASE_ENTRY_SIZE: usize = ENTRIES_SIZE + SMILES_SIZE + ID_SIZE;

#[derive(Debug, PartialEq, Clone)]
pub struct DatabaseRecord {
    pub smiles: String,
    pub identifier: CompoundIdentifier,
}

impl From<CompoundRecord> for DatabaseRecord {

    fn from(record: CompoundRecord) -> Self {

        DatabaseRecord {
            smiles: record.smiles,
            identifier: record.compound_identifier,
        }
    }
}

impl DatabaseRecord {

    fn from_line(line: &str) -> Result<Self, String> {

        let mut s = line.split(",");

        let smiles = match s.next() {

            Some(s) => s,
            None => panic!("No smiles string found"),
        };
    
        if smiles.len() > SMILES_SIZE {
            return Err("Smiles string too long".to_string());
        }

        let identifier = match s.next() {

            Some(s) => s,
            None => panic!("No identifier found"),
        };

        if identifier.len() > ID_SIZE {
            return Err("Identifier string too long".to_string());
        }

        return Ok(DatabaseRecord {
            smiles: smiles.to_string(),
            identifier: CompoundIdentifier::from_str(identifier),
        });
    }


    fn to_arr(&self) -> Result<[u8; DATABASE_ENTRY_SIZE], String> {

        let mut arr = [0u8; DATABASE_ENTRY_SIZE];

        let mut fill_arr = [0u8; SMILES_SIZE];

        let bytes = self.smiles.as_bytes();

        fill_arr[..bytes.len()].copy_from_slice(bytes);


        let smiles_arr: [u8;SMILES_SIZE] = fill_arr.try_into().expect("slice with incorrect length");


        let mut fill_arr = [0u8; ID_SIZE];

        let identifier = self.identifier.to_string();
        let bytes = identifier.as_bytes();

        fill_arr[..bytes.len()].copy_from_slice(bytes);

        let identifier_arr: [u8;ID_SIZE] = fill_arr.try_into().expect("slice with incorrect length");


        arr[SMILES_START..SMILES_START + SMILES_SIZE].copy_from_slice(&smiles_arr);
        arr[ID_START..ID_START + ID_SIZE].copy_from_slice(&identifier_arr);

        Ok(arr)
    }

    fn from_arr(arr: [u8; DATABASE_ENTRY_SIZE]) -> Self {

        let smiles_arr = &arr[SMILES_START..SMILES_START + SMILES_SIZE];
        let identifier_arr = &arr[ID_START..ID_START + ID_SIZE];

        let mut smiles = String::from_utf8(smiles_arr.to_vec()).unwrap();
        let smiles = smiles.trim_matches(char::from(0));
        let identifier = String::from_utf8(identifier_arr.to_vec()).unwrap();
        let identifier = identifier.trim_matches(char::from(0));

        DatabaseRecord {
            smiles: smiles.to_string(),
            identifier: CompoundIdentifier::from_str(identifier),
        }
    }

    fn random() -> Self {

        use rand::{distributions::Alphanumeric, Rng};

        let mut rng = rand::thread_rng();

        let smiles: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(20)
            .map(char::from)
            .collect();

        let identifier = CompoundIdentifier::random();

        DatabaseRecord {
            smiles: smiles,
            identifier: identifier,
        }
    }

}

#[derive(Debug)]

pub struct ImmutDatabase {
    filename: String,
    num_entries: u64,
}

impl ImmutDatabase {

    pub fn open(filename: &str) -> Self {

        let path = Path::new(filename);

        let mut fd = OpenOptions::new()
                    .create(false)
                    .read(true)
                    .write(false)
                    .truncate(false)
                    .open(path).unwrap();

        let mut buf = [0u8; ENTRIES_SIZE];
        fd.seek(SeekFrom::Start(ENTRIES_START as u64)).unwrap();
        fd.read(&mut buf).unwrap();

        let num_entries = u64::from_le_bytes(buf);

        Self {
            filename: filename.to_string(),
            num_entries: num_entries,
        }
    }

    pub fn query(&self, id: &u64) -> Option<DatabaseRecord> {

        let mut buf = [0u8; DATABASE_ENTRY_SIZE];

        let mut fd = OpenOptions::new()
                    .create(false)
                    .read(true)
                    .write(false)
                    .truncate(false)
                    .open(&self.filename).unwrap();

            
        fd.seek(SeekFrom::Start((id) * (DATABASE_ENTRY_SIZE as u64))).unwrap();
        fd.read(&mut buf).unwrap();

        let entry = DatabaseRecord::from_arr(buf);

        return Some(entry);
    }
}



#[derive(Debug)]
pub struct Database {
    filename: String,
    fd: File,
    num_entries: u64,
}

impl Database {

    pub fn num_entries(&self) -> usize {
        self.num_entries.try_into().unwrap()
    }

    pub fn new(filename: &str) -> Self {

        let path = Path::new(filename);

        let mut fd = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(true)
                    .open(path).unwrap();

        Database {
            filename: filename.to_string(),
            fd: fd,
            num_entries: 0,
        }
    }

    pub fn open(filename: &str) -> Self {

        let path = Path::new(filename);

        let mut fd = OpenOptions::new()
                    .create(false)
                    .read(true)
                    .write(false)
                    .truncate(false)
                    .open(path).unwrap();

        let mut buf = [0u8; ENTRIES_SIZE];
        fd.seek(SeekFrom::Start(ENTRIES_START as u64)).unwrap();
        fd.read(&mut buf).unwrap();

        let num_entries = u64::from_le_bytes(buf);

        Database {
            filename: filename.to_string(),
            fd: fd,
            num_entries: num_entries,
        }
    }
    pub fn add_compound_record(&mut self, entry: &CompoundRecord) -> Result<u64, String> {

        let database_record = DatabaseRecord::from(entry.clone());

        return self.add_entry(&database_record);
    }

    fn add_entry(&mut self, entry: &DatabaseRecord) -> Result<u64, String> {

        let arr = entry.to_arr();

        let arr = match arr {
            Ok(arr) => arr,
            Err(e) => return Err(e),
        };

        self.fd.seek(SeekFrom::Start((self.num_entries as u64) * (DATABASE_ENTRY_SIZE as u64))).unwrap();
        self.fd.write(&arr).unwrap();

        let return_idx = self.num_entries;

        self.num_entries += 1;

        self.fd.seek(SeekFrom::Start(ENTRIES_START as u64)).unwrap();
        self.fd.write(&(self.num_entries as u64).to_le_bytes()).unwrap();

        return Ok(return_idx);
    }

    pub fn query(&mut self, id: &u64) -> Option<DatabaseRecord> {

        let mut buf = [0u8; DATABASE_ENTRY_SIZE];
            
        self.fd.seek(SeekFrom::Start((id) * (DATABASE_ENTRY_SIZE as u64))).unwrap();
        self.fd.read(&mut buf).unwrap();

        let entry = DatabaseRecord::from_arr(buf);

        return Some(entry);
    }
}

fn main() {

    let filename = "/pool/test_file.db";

    let mut database = Database::open(filename);

    let mut rng = rand::thread_rng();
    let mut indices: Vec<u64> = Vec::new();
    for i in (0..1000000) {

        let idx: u64 = rng.gen_range(0..1e9 as u64);
        indices.push(idx);
    }

    println!("Running queries");
    for idx in tqdm!(indices.into_iter()) {
        database.query(&idx);
    }
}


fn build_from_file(filename: &str) {


    use std::io::prelude::*;

    let mut file = File::open(filename).unwrap();
    let mut contents = String::new();

    match file.read_to_string(&mut contents) {
        Ok(_) => println!("File read successfully"),
        Err(e) => println!("Error reading file: {}", e),
    }

    let out_filename = "/pool/test_file.db";

    let path = Path::new(out_filename);

    let mut fd = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(path).unwrap();

    for (i, line) in contents.lines().enumerate() {
        if i == 0 {
            continue;
        }

        let entry: DatabaseRecord = DatabaseRecord::from_line(line).unwrap();
        let arr = entry.to_arr();

        let arr = match arr {
            Ok(arr) => arr,
            Err(e) => continue,
        };

        if i == 2 {
            for i in tqdm!(0..1e9 as u64) {
                fd.write(&arr).unwrap();
            }
        }

    }
}
#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn small_random_db() {
        use rand::{distributions::Alphanumeric, Rng};

        let filename = "/tmp/small_random.db";

        let mut database = Database::new(filename);


        let mut entries: Vec<DatabaseRecord> = Vec::new();

        for _ in 0..10000 {
            let entry = DatabaseRecord::random();
            entries.push(entry);
        }

        let mut reported_entries: Vec<(u64, DatabaseRecord)> = Vec::new();

        for entry in entries.iter() {
            let idx = database.add_entry(entry);
            reported_entries.push((idx.unwrap() as u64, entry.clone()));
        }

        for (idx, entry) in reported_entries.into_iter() {
            let queried_entry = database.query(&idx).unwrap();
            assert_eq!(queried_entry, entry);
        }

        let mut database = Database::open(filename);
    }


}

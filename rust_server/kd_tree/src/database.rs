//! SMILES and identifier storage, keyed by the record index the tree stores.
//!
//! Two files:
//!
//! - `db.db`     magic, entry count, then packed entries
//! - `db.db.idx` magic, entry count, then `n + 1` byte offsets into `db.db`
//!
//! An entry is `[u16 id_len][u16 smiles_len][id][smiles]`. The offset table
//! gives random access by index; the lengths keep each entry self-describing.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

use memmap2::{Advice, Mmap, MmapOptions};

use crate::data::{CompoundRecord, CompoundIdentifier};

/// `b"KDTDB003"`. Bumped from the fixed-width layout so an index built by an
/// older version fails loudly instead of being read as garbage.
pub const DB_MAGIC: [u8; 8] = *b"KDTDB003";
pub const IDX_MAGIC: [u8; 8] = *b"KDTIX003";

pub const HEADER_SIZE: usize = 16; // magic + u64 count
pub const ENTRY_HEADER_SIZE: usize = 4; // two u16 lengths

fn index_path(db_path: &str) -> String {
    return format!("{}.idx", db_path);
}

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

    /// Appends this entry to `buf`. Does not allocate when `buf` is reused.
    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), String> {

        let identifier = self.identifier.to_string();
        let id_bytes = identifier.as_bytes();
        let smiles_bytes = self.smiles.as_bytes();

        if id_bytes.len() > u16::MAX as usize {
            return Err(format!("identifier is {} bytes, over 65535", id_bytes.len()));
        }

        if smiles_bytes.len() > u16::MAX as usize {
            return Err(format!("SMILES is {} bytes, over 65535", smiles_bytes.len()));
        }

        buf.clear();
        buf.extend_from_slice(&(id_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(&(smiles_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(id_bytes);
        buf.extend_from_slice(smiles_bytes);

        return Ok(());
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {

        if bytes.len() < ENTRY_HEADER_SIZE {
            return Err(format!("entry is {} bytes, too short for a header", bytes.len()));
        }

        let id_len = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
        let smiles_len = u16::from_le_bytes([bytes[2], bytes[3]]) as usize;

        if ENTRY_HEADER_SIZE + id_len + smiles_len > bytes.len() {
            return Err(format!(
                "entry claims {} + {} bytes but only {} are available",
                id_len, smiles_len, bytes.len() - ENTRY_HEADER_SIZE,
            ));
        }

        let id_at = ENTRY_HEADER_SIZE;
        let smiles_at = id_at + id_len;

        let identifier = std::str::from_utf8(&bytes[id_at..id_at + id_len])
            .map_err(|e| format!("identifier is not utf8: {}", e))?;

        let smiles = std::str::from_utf8(&bytes[smiles_at..smiles_at + smiles_len])
            .map_err(|e| format!("SMILES is not utf8: {}", e))?;

        return Ok(DatabaseRecord {
            smiles: smiles.to_string(),
            identifier: CompoundIdentifier::from_str(identifier),
        });
    }

    #[cfg(test)]
    fn random() -> Self {

        use rand::{distributions::Alphanumeric, Rng};

        let smiles: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(20)
            .map(char::from)
            .collect();

        return DatabaseRecord {
            smiles,
            identifier: CompoundIdentifier::random(),
        };
    }
}


fn read_header(fd: &File, expected_magic: &[u8; 8], filename: &str) -> u64 {

    let mut header = [0u8; HEADER_SIZE];

    fd.read_exact_at(&mut header, 0)
        .unwrap_or_else(|e| panic!("could not read header of {}: {}", filename, e));

    if &header[0..8] != expected_magic {
        panic!(
            "{} has magic {:?}, expected {:?}. An index built before the \
             variable-length database format has to be rebuilt.",
            filename, &header[0..8], expected_magic,
        );
    }

    return u64::from_le_bytes(header[8..16].try_into().expect("count is 8 bytes"));
}

/// Read-only database, served from memory maps.
#[derive(Debug)]
pub struct ImmutDatabase {
    filename: String,
    entries: Mmap,
    offsets: Mmap,
    num_entries: u64,
}

impl ImmutDatabase {

    pub fn open(filename: &str) -> Self {

        let open_read = |path: &str| -> File {
            OpenOptions::new()
                .read(true)
                .open(Path::new(path))
                .unwrap_or_else(|e| panic!("could not open {}: {}", path, e))
        };

        let idx_filename = index_path(filename);

        let entries_fd = open_read(filename);
        let offsets_fd = open_read(&idx_filename);

        let num_entries = read_header(&entries_fd, &DB_MAGIC, filename);
        let offsets_entries = read_header(&offsets_fd, &IDX_MAGIC, &idx_filename);

        if num_entries != offsets_entries {
            panic!(
                "{} says {} entries but {} says {}; the index is inconsistent",
                filename, num_entries, idx_filename, offsets_entries,
            );
        }

        let entries = unsafe { MmapOptions::new().map(&entries_fd) }
            .unwrap_or_else(|e| panic!("could not map {}: {}", filename, e));
        let offsets = unsafe { MmapOptions::new().map(&offsets_fd) }
            .unwrap_or_else(|e| panic!("could not map {}: {}", idx_filename, e));

        // Lookups are scattered: one per returned hit, at unrelated indices.
        let _ = entries.advise(Advice::Random);
        let _ = offsets.advise(Advice::Random);

        return Self {
            filename: filename.to_string(),
            entries,
            offsets,
            num_entries,
        };
    }

    pub fn num_entries(&self) -> u64 {
        return self.num_entries;
    }

    #[inline]
    fn offset_at(&self, i: u64) -> usize {

        let at = HEADER_SIZE + (i as usize) * 8;
        let bytes = self.offsets[at..at + 8].try_into().expect("offset is 8 bytes");

        return u64::from_le_bytes(bytes) as usize;
    }

    /// Looks up the entry for a compound index.
    pub fn query(&self, id: &u64) -> Option<DatabaseRecord> {

        if *id >= self.num_entries {
            return None;
        }

        let start = self.offset_at(*id);
        let end = self.offset_at(*id + 1);

        if end > self.entries.len() || start > end {
            eprintln!("{}: entry {} has offsets {}..{}, outside the file",
                      self.filename, id, start, end);
            return None;
        }

        match DatabaseRecord::decode(&self.entries[start..end]) {
            Ok(record) => Some(record),
            Err(e) => {
                eprintln!("{}: could not decode entry {}: {}", self.filename, id, e);
                None
            },
        }
    }
}

/// Append-only database used while building a tree.
#[derive(Debug)]
pub struct Database {
    entries: BufWriter<File>,
    offsets: BufWriter<File>,
    /// Byte offset in `db.db` where the next entry starts.
    cursor: u64,
    num_entries: u64,
    scratch: Vec<u8>,
}

impl Database {

    pub fn new(filename: &str) -> Self {

        let create = |path: &str| -> File {
            OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(Path::new(path))
                .unwrap_or_else(|e| panic!("could not create {}: {}", path, e))
        };

        let mut entries = BufWriter::with_capacity(1 << 20, create(filename));
        let mut offsets = BufWriter::with_capacity(1 << 20, create(&index_path(filename)));

        // Counts are placeholders until `flush`.
        entries.write_all(&DB_MAGIC).unwrap();
        entries.write_all(&0u64.to_le_bytes()).unwrap();

        offsets.write_all(&IDX_MAGIC).unwrap();
        offsets.write_all(&0u64.to_le_bytes()).unwrap();

        // Offset table holds n+1 entries; the first is where entry 0 begins.
        offsets.write_all(&(HEADER_SIZE as u64).to_le_bytes()).unwrap();

        return Database {
            entries,
            offsets,
            cursor: HEADER_SIZE as u64,
            num_entries: 0,
            scratch: Vec::with_capacity(256),
        };
    }

    /// Reopens an existing database for reading and appending.
    pub fn open(filename: &str) -> Self {

        let idx_filename = index_path(filename);

        let reopen = |path: &str| -> File {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(Path::new(path))
                .unwrap_or_else(|e| panic!("could not open {}: {}", path, e))
        };

        let mut entries_fd = reopen(filename);
        let mut offsets_fd = reopen(&idx_filename);

        let num_entries = read_header(&entries_fd, &DB_MAGIC, filename);
        let offsets_entries = read_header(&offsets_fd, &IDX_MAGIC, &idx_filename);

        if num_entries != offsets_entries {
            panic!(
                "{} says {} entries but {} says {}; the index is inconsistent",
                filename, num_entries, idx_filename, offsets_entries,
            );
        }

        // offset[num_entries] is where the next entry would start
        let mut cursor_bytes = [0u8; 8];
        let at = (HEADER_SIZE + (num_entries as usize) * 8) as u64;
        offsets_fd
            .read_exact_at(&mut cursor_bytes, at)
            .unwrap_or_else(|e| panic!("could not read write cursor from {}: {}", idx_filename, e));

        let cursor = u64::from_le_bytes(cursor_bytes);

        // BufWriter appends from the file's current position
        entries_fd.seek(SeekFrom::Start(cursor)).unwrap();
        offsets_fd.seek(SeekFrom::Start(at + 8)).unwrap();

        return Database {
            entries: BufWriter::with_capacity(1 << 20, entries_fd),
            offsets: BufWriter::with_capacity(1 << 20, offsets_fd),
            cursor,
            num_entries,
            scratch: Vec::with_capacity(256),
        };
    }

    pub fn num_entries(&self) -> u64 {
        return self.num_entries;
    }

    pub fn add_compound_record(&mut self, entry: &CompoundRecord) -> Result<u64, String> {

        let database_record = DatabaseRecord::from(entry.clone());

        return self.add_entry(&database_record);
    }

    /// Appends an entry and returns its index.
    fn add_entry(&mut self, entry: &DatabaseRecord) -> Result<u64, String> {

        let mut scratch = std::mem::take(&mut self.scratch);
        let encoded = entry.encode(&mut scratch);
        self.scratch = scratch;

        encoded?;

        self.entries.write_all(&self.scratch)
            .map_err(|e| format!("could not write entry: {}", e))?;

        self.cursor += self.scratch.len() as u64;

        self.offsets.write_all(&self.cursor.to_le_bytes())
            .map_err(|e| format!("could not write offset: {}", e))?;

        let return_idx = self.num_entries;
        self.num_entries += 1;

        return Ok(return_idx);
    }

    /// Flushes buffers and stamps the final entry count into both headers.
    pub fn flush(&mut self) {

        self.entries.flush().expect("could not flush database entries");
        self.offsets.flush().expect("could not flush database offsets");

        let count = self.num_entries.to_le_bytes();

        for writer in [self.entries.get_mut(), self.offsets.get_mut()] {
            writer.seek(SeekFrom::Start(8)).expect("could not seek to count");
            writer.write_all(&count).expect("could not write count");
            writer.flush().expect("could not flush count");
        }
    }

    /// Reads back an entry
    pub fn query(&mut self, id: &u64) -> Option<DatabaseRecord> {

        if *id >= self.num_entries {
            return None;
        }

        self.flush();

        let mut offsets = [0u8; 16];
        let at = (HEADER_SIZE + (*id as usize) * 8) as u64;
        self.offsets.get_ref().read_exact_at(&mut offsets, at).ok()?;

        let start = u64::from_le_bytes(offsets[0..8].try_into().unwrap());
        let end = u64::from_le_bytes(offsets[8..16].try_into().unwrap());

        let mut buf = vec![0u8; (end - start) as usize];
        self.entries.get_ref().read_exact_at(&mut buf, start).ok()?;

        return DatabaseRecord::decode(&buf).ok();
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn small_random_db() {

        let filename = "/tmp/kd_tree_test/small_random.db";
        std::fs::create_dir_all("/tmp/kd_tree_test").unwrap();

        let mut database = Database::new(filename);

        let mut entries: Vec<DatabaseRecord> = Vec::new();

        for _ in 0..10000 {
            entries.push(DatabaseRecord::random());
        }

        let mut reported_entries: Vec<(u64, DatabaseRecord)> = Vec::new();

        for entry in entries.iter() {
            let idx = database.add_entry(entry);
            reported_entries.push((idx.unwrap() as u64, entry.clone()));
        }

        database.flush();

        for (idx, entry) in reported_entries.iter() {
            let queried_entry = database.query(idx).unwrap();
            assert_eq!(&queried_entry, entry);
        }

        // and the same entries through the read-only, memory-mapped path
        let immut = ImmutDatabase::open(filename);
        assert_eq!(immut.num_entries(), 10000);

        for (idx, entry) in reported_entries.iter() {
            assert_eq!(&immut.query(idx).unwrap(), entry);
        }

        assert!(immut.query(&10000).is_none());
    }

    #[test]
    fn variable_length_entries_round_trip() {

        let filename = "/tmp/kd_tree_test/varlen.db";
        std::fs::create_dir_all("/tmp/kd_tree_test").unwrap();

        let cases = vec![
            DatabaseRecord { smiles: "C".to_string(), identifier: CompoundIdentifier::from_str("A") },
            DatabaseRecord { smiles: "CC(=O)OCC[N+](C)(C)C".to_string(), identifier: CompoundIdentifier::from_str("Z1741968337") },
            DatabaseRecord { smiles: "C".repeat(148), identifier: CompoundIdentifier::from_str("x".repeat(50).as_str()) },
        ];

        let mut database = Database::new(filename);
        for case in cases.iter() {
            database.add_entry(case).unwrap();
        }
        database.flush();

        let immut = ImmutDatabase::open(filename);
        for (i, case) in cases.iter().enumerate() {
            assert_eq!(&immut.query(&(i as u64)).unwrap(), case);
        }
    }
}

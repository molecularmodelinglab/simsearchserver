//! Fingerprints stored by record index, for re-ranking without RDKit.

use crate::error::Error;
use crate::layout;
use memmap2::{Advice, Mmap, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::io::{Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::Path;

pub const FINGERPRINT_MAGIC: [u8; layout::MAGIC_SIZE] = *b"KDTFNGPR";

/// magic, version, padding, record count, bytes per record.
pub const HEADER_SIZE: usize = 32;
const COUNT_AT: usize = 16;
const WIDTH_AT: usize = 24;

/// The file sits beside the index's other files under this name.
pub const FILENAME: &str = "fingerprints";

pub fn path_in(directory: &str) -> String {
    return format!("{}/{}", directory, FILENAME);
}

/// Tanimoto over two equal-length bit vectors.
pub fn tanimoto(left: &[u8], right: &[u8]) -> f32 {
    let mut shared = 0u32;
    let mut total = 0u32;

    for (a, b) in left.chunks_exact(8).zip(right.chunks_exact(8)) {
        let a = u64::from_le_bytes(a.try_into().expect("chunks_exact(8) yields 8 bytes"));
        let b = u64::from_le_bytes(b.try_into().expect("chunks_exact(8) yields 8 bytes"));

        shared += (a & b).count_ones();
        total += a.count_ones() + b.count_ones();
    }

    let union = total - shared;
    if union == 0 {
        return 0.0;
    }

    return shared as f32 / union as f32;
}

/// Appends fingerprints in the order records are added to the tree.
pub struct FingerprintWriter {
    out: BufWriter<File>,
    bytes_per_record: usize,
    written: u64,
}

impl FingerprintWriter {

    pub fn create(path: &str, bytes_per_record: usize) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true).read(true).write(true).truncate(true)
            .open(Path::new(path))?;

        let mut out = BufWriter::with_capacity(1 << 20, file);

        let mut header = [0u8; HEADER_SIZE];
        header[..layout::MAGIC_SIZE].copy_from_slice(&FINGERPRINT_MAGIC);
        header[layout::VERSION_OFFSET..layout::VERSION_OFFSET + layout::VERSION_SIZE]
            .copy_from_slice(&layout::FORMAT_VERSION.to_le_bytes());
        header[WIDTH_AT..WIDTH_AT + 8].copy_from_slice(&(bytes_per_record as u64).to_le_bytes());
        out.write_all(&header)?;

        return Ok(Self { out, bytes_per_record, written: 0 });
    }

    pub fn append(&mut self, fingerprint: &[u8]) -> Result<(), Error> {
        if fingerprint.len() != self.bytes_per_record {
            return Err(Error::UnexpectedError);
        }

        self.out.write_all(fingerprint)?;
        self.written += 1;

        return Ok(());
    }

    pub fn written(&self) -> u64 {
        return self.written;
    }

    /// Stamps the count, which is what makes the file readable.
    pub fn finish(mut self) -> Result<u64, Error> {
        self.out.flush()?;

        let file = self.out.get_mut();
        file.seek(SeekFrom::Start(COUNT_AT as u64))?;
        file.write_all(&self.written.to_le_bytes())?;
        file.flush()?;

        return Ok(self.written);
    }
}

#[derive(Debug)]
pub struct FingerprintStore {
    map: Mmap,
    count: u64,
    bytes_per_record: usize,
}

impl FingerprintStore {

    /// Panics on a mismatch: it would re-rank against another index's molecules.
    pub fn open(path: &str, expected_records: u64) -> Result<Self, Error> {
        let file = File::open(Path::new(path))?;

        let mut header = [0u8; HEADER_SIZE];
        file.read_exact_at(&mut header, 0)?;

        if header[..layout::MAGIC_SIZE] != FINGERPRINT_MAGIC {
            panic!("{} is not a fingerprint file", path);
        }

        let version = u32::from_le_bytes(
            header[layout::VERSION_OFFSET..layout::VERSION_OFFSET + layout::VERSION_SIZE]
                .try_into().expect("version is VERSION_SIZE bytes"));
        if version != layout::FORMAT_VERSION {
            panic!("{} is format version {}, expected {}", path, version, layout::FORMAT_VERSION);
        }

        let count = u64::from_le_bytes(
            header[COUNT_AT..COUNT_AT + 8].try_into().expect("count is 8 bytes"));
        let bytes_per_record = u64::from_le_bytes(
            header[WIDTH_AT..WIDTH_AT + 8].try_into().expect("width is 8 bytes")) as usize;

        if count != expected_records {
            panic!("{} holds {} fingerprints but the index holds {} records",
                   path, count, expected_records);
        }

        if bytes_per_record == 0 {
            panic!("{} reports a zero-width fingerprint", path);
        }

        let map = unsafe { MmapOptions::new().map(&file) }?;
        // Lookups are scattered: one per returned hit, at unrelated indices.
        let _ = map.advise(Advice::Random);
        let _ = map.advise(Advice::HugePage);

        return Ok(Self { map, count, bytes_per_record });
    }

    pub fn bytes_per_record(&self) -> usize {
        return self.bytes_per_record;
    }

    pub fn len(&self) -> u64 {
        return self.count;
    }

    pub fn get(&self, index: u64) -> Option<&[u8]> {
        if index >= self.count {
            return None;
        }

        let start = HEADER_SIZE + (index as usize * self.bytes_per_record);

        return Some(&self.map[start..start + self.bytes_per_record]);
    }

    /// Prefaults the whole file so the first queries do not pay for it.
    pub fn warm(&self) -> Result<usize, Error> {
        let _ = self.map.advise(Advice::Sequential);
        let _ = self.map.advise(Advice::WillNeed);
        let _ = self.map.advise(Advice::PopulateRead);
        let _ = self.map.advise(Advice::Random);

        return Ok(self.map.len());
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn tanimoto_matches_the_definition() {
        let a = [0b0000_1111u8, 0, 0, 0, 0, 0, 0, 0];
        let b = [0b0011_1100u8, 0, 0, 0, 0, 0, 0, 0];

        // Two bits shared, six in the union.
        assert!((tanimoto(&a, &b) - 2.0 / 6.0).abs() < 1e-6);
        assert_eq!(tanimoto(&a, &a), 1.0);
        assert_eq!(tanimoto(&a, &[0u8; 8]), 0.0);
        assert_eq!(tanimoto(&[0u8; 8], &[0u8; 8]), 0.0);
    }

    #[test]
    fn fingerprints_round_trip_by_index() {
        let directory = std::env::temp_dir().join(format!("kdfp_{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = path_in(directory.to_str().unwrap());

        let mut writer = FingerprintWriter::create(&path, 8).unwrap();
        for i in 0..50u8 {
            writer.append(&[i; 8]).unwrap();
        }
        assert_eq!(writer.finish().unwrap(), 50);

        let store = FingerprintStore::open(&path, 50).unwrap();
        assert_eq!(store.len(), 50);
        assert_eq!(store.bytes_per_record(), 8);

        for i in 0..50u8 {
            assert_eq!(store.get(i as u64).unwrap(), &[i; 8]);
        }
        assert!(store.get(50).is_none());

        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn a_wrong_width_is_refused() {
        let directory = std::env::temp_dir().join(format!("kdfpw_{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = path_in(directory.to_str().unwrap());

        let mut writer = FingerprintWriter::create(&path, 8).unwrap();
        assert!(writer.append(&[0u8; 4]).is_err());

        std::fs::remove_dir_all(&directory).unwrap();
    }
}

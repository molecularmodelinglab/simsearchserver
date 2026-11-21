use kd_tree::data::{CompoundIdentifier, Descriptor, CompoundRecord, IDENTIFIER_SIZE, MAX_SMILES_LENGTH};

use kdam::tqdm;
use kd_tree::tree;
use std::io::prelude::*;
use std::io::{self, BufRead};
use std::path::Path;
use std::fs::{File, OpenOptions};
use std::time::Instant;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use arrow_array::{Array, Float32Array, StringArray, LargeStringArray};
use arrow_schema::DataType;

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser, Clone)]
#[clap(name = "builder", version)]
pub struct App {
    #[clap(flatten)]
    global_opts: GlobalOpts,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand, Clone)]
enum Command {
    BuildFromFiles(BuildFromFileArgs),
}

#[derive(Debug, Args, Clone)]
struct BuildFromFileArgs {

    ///Filenames of source data (.parquet or .csv)
    #[arg(long, num_args(1..))]
    filenames: Vec<String>,

    ///Config filename
    #[clap(long)]
    config_filename: String,

    ///Cache size for build, in GB
    #[clap(long, default_value_t = 1.0)]
    cache_size: f32,

    ///Stop after this many records (for timing runs on a subset)
    #[clap(long)]
    limit: Option<usize>,
}

#[derive(Debug, Args, Clone)]
struct GlobalOpts {
    /// Color
    #[clap(long, global = true)]
    color: bool,
}

fn main() {

    let args = App::parse();

    match &args.command {
        Command::BuildFromFiles(bargs) => {build_from_files(&bargs);},
    }
}

/// Running totals shared across all input files.
struct BuildStats {
    success: usize,
    errors: usize,
    start: Instant,
}

/// Rejects records the fixed-width on-disk format cannot hold.
fn validate(smiles: &str, identifier: &str) -> Result<(), String> {

    if identifier.len() > IDENTIFIER_SIZE {
        return Err(format!("identifier is {} bytes, limit is {}", identifier.len(), IDENTIFIER_SIZE));
    }

    if !identifier.is_ascii() {
        return Err("identifier is not ASCII".to_string());
    }

    if smiles.len() > MAX_SMILES_LENGTH {
        return Err(format!("SMILES is {} bytes, limit is {}", smiles.len(), MAX_SMILES_LENGTH));
    }

    if !smiles.is_ascii() {
        return Err("SMILES is not ASCII".to_string());
    }

    return Ok(());
}

fn add_record(
    tree: &mut tree::Tree,
    log_file: &mut File,
    stats: &mut BuildStats,
    desc_length: usize,
    stem: &str,
    smiles: &str,
    id_val: &str,
    descriptor_values: Vec<f32>,
) {

    if descriptor_values.len() != desc_length {
        log_error(log_file, stats, &format!(
            "descriptor has {} values, tree expects {}: {} {}",
            descriptor_values.len(), desc_length, smiles, id_val));
        return;
    }

    let id_string = format!("{}{}", stem, id_val);

    match validate(smiles, &id_string) {
        Ok(_) => {},
        Err(e) => {
            log_error(log_file, stats, &format!("{}: {} {}", e, smiles, id_string));
            return;
        },
    }

    let mut descriptor = Descriptor{ data: descriptor_values, length: desc_length };

    descriptor.add_small_noise();

    let record = CompoundRecord{
        compound_identifier: CompoundIdentifier::from_string(id_string.clone()),
        smiles: smiles.to_string(),
        descriptor,
        length: desc_length,
    };

    match tree.add_record(&record) {
        Ok(_) => {},
        Err(e) => {
            log_error(log_file, stats, &format!("could not add to tree: {}: {} {}", e, smiles, id_string));
            return;
        },
    }

    stats.success += 1;

    if (stats.success % 1000000 == 0) && (stats.success != 0) {
        let elapsed = stats.start.elapsed().as_secs_f64();
        let line = format!("Total records added: {} in {:.1} seconds ({:.0}/s)\n",
                           stats.success, elapsed, stats.success as f64 / elapsed);
        print!("{}", &line);
        let _ = log_file.write_all(line.as_bytes());
    }
}

fn log_error(log_file: &mut File, stats: &mut BuildStats, message: &str) {
    stats.errors += 1;
    let _ = log_file.write_all(format!("{}\n", message).as_bytes());
}

/// Dataset prefix, taken from the leading underscore-delimited part of the
/// filename, so records from different source files stay distinguishable.
fn stem_of(filename: &str) -> String {
    return filename
        .split('/').last().unwrap_or(filename)
        .split('_').next().unwrap_or("")
        .to_string();
}

fn build_from_files(args: &BuildFromFileArgs) {

    let config = tree::TreeConfig::from_file(args.config_filename.clone());
    let desc_length = config.desc_length;

    match args.filenames.len() {
        0 => panic!("No filenames supplied"),
        _ => {},
    }

    let mut tree = tree::Tree::create_with_config(config.clone());

    let log_file_path = config.directory.clone() + "/build_log.txt";

    let mut log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(log_file_path.clone())
            .expect("could not open build log");

    let mut stats = BuildStats { success: 0, errors: 0, start: Instant::now() };

    'files: for filename in tqdm!(args.filenames.iter()) {

        let stem = stem_of(filename);

        println!("{} (prefix {:?})", &filename, &stem);

        if filename.ends_with(".parquet") {
            ingest_parquet(filename, &stem, &mut tree, &mut log_file, &mut stats, desc_length, args.limit);
        } else {
            ingest_csv(filename, &stem, &mut tree, &mut log_file, &mut stats, desc_length, args.limit);
        }

        if let Some(limit) = args.limit {
            if stats.success >= limit {
                println!("reached limit of {} records", limit);
                break 'files;
            }
        }
    }

    tree.flush();

    let elapsed = stats.start.elapsed().as_secs_f64();

    println!(
        "{} records added, {} skipped, in {:.1}s ({:.0}/s). Skipped records logged in {}",
        stats.success, stats.errors, elapsed, stats.success as f64 / elapsed, log_file_path,
    );
}

/// Reads `ID`, `SMILES` and `dim_0..dim_{n-1}` columns from a parquet file.
fn ingest_parquet(
    filename: &str,
    stem: &str,
    tree: &mut tree::Tree,
    log_file: &mut File,
    stats: &mut BuildStats,
    desc_length: usize,
    limit: Option<usize>,
) {

    let file = File::open(filename).expect(&format!("could not open {}", filename));

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect(&format!("could not read parquet metadata from {}", filename));

    let schema = builder.schema().clone();

    let mut wanted: Vec<String> = vec!["ID".to_string(), "SMILES".to_string()];
    for i in 0..desc_length {
        wanted.push(format!("dim_{}", i));
    }

    let mut indices: Vec<usize> = Vec::new();
    for name in wanted.iter() {
        match schema.index_of(name) {
            Ok(i) => indices.push(i),
            Err(_) => panic!(
                "{} has no column {:?}; expected ID, SMILES and dim_0..dim_{}",
                filename, name, desc_length - 1,
            ),
        }
    }
    
    let file_dims = schema.fields().iter()
        .filter(|f| f.name().starts_with("dim_"))
        .count();

    if file_dims != desc_length {
        panic!(
            "{} has {} dim_* columns but the tree config says desc_length {}; \
             refusing to build an index from truncated vectors",
            filename, file_dims, desc_length,
        );
    }

    let mask = ProjectionMask::roots(builder.parquet_schema(), indices);

    let reader = builder
        .with_projection(mask)
        .with_batch_size(8192)
        .build()
        .expect("could not build parquet reader");

    for batch in reader {

        let batch = batch.expect("could not read parquet batch");

        let ids = string_column(&batch, "ID", filename);
        let smiles_col = string_column(&batch, "SMILES", filename);

        let dims: Vec<&Float32Array> = (0..desc_length)
            .map(|i| {
                let name = format!("dim_{}", i);
                batch.column_by_name(&name)
                    .expect(&format!("{} missing in batch", name))
                    .as_any().downcast_ref::<Float32Array>()
                    .expect(&format!("{} in {} is not a float32 column", name, filename))
            })
            .collect();

        for row in 0..batch.num_rows() {

            if let Some(limit) = limit {
                if stats.success >= limit { return; }
            }

            if ids[row].is_none() || smiles_col[row].is_none() {
                log_error(log_file, stats, "null ID or SMILES");
                continue;
            }

            let mut values: Vec<f32> = Vec::with_capacity(desc_length);
            let mut null_dim = false;

            for d in dims.iter() {
                if d.is_null(row) { null_dim = true; break; }
                values.push(d.value(row));
            }

            if null_dim {
                log_error(log_file, stats, &format!("null descriptor value for {:?}", ids[row]));
                continue;
            }

            add_record(
                tree, log_file, stats, desc_length, stem,
                smiles_col[row].unwrap(), ids[row].unwrap(), values,
            );
        }
    }
}

/// Reads a string column as `Option<&str>` per row, accepting either
/// `Utf8` or `LargeUtf8` (pyarrow writes `large_string` by default).
fn string_column<'a>(
    batch: &'a arrow_array::RecordBatch,
    name: &str,
    filename: &str,
) -> Vec<Option<&'a str>> {

    let column = batch.column_by_name(name)
        .expect(&format!("{} has no column {}", filename, name));

    let values: Vec<Option<&str>> = match column.data_type() {
        DataType::Utf8 => {
            let a = column.as_any().downcast_ref::<StringArray>().unwrap();
            (0..a.len()).map(|i| if a.is_null(i) { None } else { Some(a.value(i)) }).collect()
        },
        DataType::LargeUtf8 => {
            let a = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
            (0..a.len()).map(|i| if a.is_null(i) { None } else { Some(a.value(i)) }).collect()
        },
        other => panic!("column {} in {} is {:?}, expected a string type", name, filename, other),
    };

    return values;
}

/// Reads `smiles,id,dim_0..dim_{n-1}` rows, skipping one header line.
fn ingest_csv(
    filename: &str,
    stem: &str,
    tree: &mut tree::Tree,
    log_file: &mut File,
    stats: &mut BuildStats,
    desc_length: usize,
    limit: Option<usize>,
) {

    let lines = read_lines(filename).expect(&format!("Could not read file: {:?}", filename));

    for (i, line) in lines.enumerate() {

        if i == 0 { continue; }

        if let Some(limit) = limit {
            if stats.success >= limit { return; }
        }

        let mut good_line = match line {
            Ok(l) => l,
            Err(e) => { log_error(log_file, stats, &format!("could not read line: {}", e)); continue; },
        };

        if good_line.trim().is_empty() { continue; }

        //strip out smiles extension stuff
        if good_line.contains("|") {

            let mut keep_string: Vec<char> = Vec::new();
            let mut keep = true;
            for char in good_line.chars() {
                if char == '|' { keep = !keep; continue }
                if keep {
                    keep_string.push(char.clone());
                }
            }

            good_line = keep_string.into_iter().collect();
        }

        let fields: Vec<&str> = good_line.split(",").collect();

        let mut field_iter = fields.into_iter();

        let smiles = match field_iter.next() {
            Some(s) => s,
            None => { log_error(log_file, stats, "no smiles field"); continue; },
        };

        let id_val = match field_iter.next() {
            Some(s) => s,
            None => { log_error(log_file, stats, "no identifier field"); continue; },
        };

        let descriptor_fields: Vec<&str> = field_iter.collect();

        let descriptor_values = match parse_descriptor_vec(descriptor_fields) {
            Ok(a) => a,
            Err(e) => {
                log_error(log_file, stats, &format!("could not parse descriptor: {}: {}", e, &good_line));
                continue
            },
        };

        add_record(tree, log_file, stats, desc_length, stem, smiles, id_val, descriptor_values);
    }
}

fn parse_descriptor_vec(v: Vec<&str>) -> Result<Vec<f32>, std::num::ParseFloatError> {

    let descriptor_values: Result<Vec<f32>,_> = v.iter()
        .map(|x| x
                  .replace("[","")
                  .replace("]","")
                  .replace(" ","")
                  .parse::<f32>()
            )
        .collect();
    return descriptor_values;
}

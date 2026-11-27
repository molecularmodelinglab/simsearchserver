//! End-to-end behavior through the public API.

use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use rand::SeedableRng;
use rand::rngs::StdRng;

use kd_tree::data::{CompoundIdentifier, CompoundRecord, Descriptor};
use kd_tree::tree::{ImmutTree, NearestNeighbors, Tree, TreeConfig};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Seeded per test, so a failure can be reproduced from the test name alone.
fn seeded(name: &str) -> StdRng {
    return StdRng::seed_from_u64(name.bytes().map(u64::from).sum());
}

/// Cargo runs tests in parallel, so a shared path would make them collide.
fn scratch_directory(name: &str) -> String {
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut path: PathBuf = std::env::temp_dir();
    path.push(format!("kd_tree_test_{}_{}_{}", name, std::process::id(), unique));

    let _ = std::fs::remove_dir_all(&path);

    return path.to_string_lossy().into_owned();
}

fn build_tree(records: &[CompoundRecord], desc_length: usize, name: &str) -> ImmutTree {

    let directory = scratch_directory(name);

    let mut config = TreeConfig::default();
    config.desc_length = desc_length;
    config.directory = directory.clone();

    let mut tree = Tree::force_create_with_config(config);

    for record in records {
        tree.add_record(record).unwrap();
    }
    tree.flush();

    return ImmutTree::read_from_directory(directory);
}

fn brute_force_top_k(
    records: &[CompoundRecord],
    query: &Descriptor,
    k: usize,
) -> (Vec<String>, Vec<f32>) {

    let mut scored: Vec<(f32, String)> = records
        .iter()
        .map(|r| (query.distance(&r.descriptor), r.compound_identifier.to_string()))
        .collect();

    scored.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .expect("descriptor distances are finite")
            .then_with(|| a.1.cmp(&b.1))
    });

    scored.truncate(k);

    let identifiers = scored.iter().map(|(_, id)| id.clone()).collect();
    let distances = scored.iter().map(|(d, _)| *d).collect();

    return (identifiers, distances);
}

fn assert_is_true_knn(
    tree: &ImmutTree,
    nn: &NearestNeighbors,
    records: &[CompoundRecord],
    query: &Descriptor,
    k: usize,
) {

    let (expected_ids, expected_dists) = brute_force_top_k(records, query, k);

    let mut got_ids: Vec<String> = tree
        .records_at(&nn.indexes)
        .iter()
        .map(|r| {
            r.as_ref()
                .expect("every hit should resolve in the database")
                .identifier
                .to_string()
        })
        .collect();

    assert_eq!(got_ids.len(), expected_ids.len(),
               "tree returned {} hits, expected {}", got_ids.len(), expected_ids.len());

    for i in 0..expected_dists.len() {
        let delta = (nn.distances[i] - expected_dists[i]).abs();
        assert!(delta < 1e-5,
                "hit {} is at distance {} but the true {}th nearest is at {}",
                i, nn.distances[i], i, expected_dists[i]);
    }

    let mut expected_ids = expected_ids;
    got_ids.sort();
    expected_ids.sort();

    assert_eq!(got_ids, expected_ids, "tree returned a different set of records than brute force");
}

fn random_records(rng: &mut StdRng, n: usize, desc_length: usize) -> Vec<CompoundRecord> {
    return (0..n).map(|_| CompoundRecord::random(rng, desc_length)).collect();
}

#[test]
fn builds_a_tree_across_page_splits() {

    let mut rng = seeded("builds_a_tree_across_page_splits");

    let directory = scratch_directory("build");

    let mut config = TreeConfig::default();
    config.directory = directory;

    let mut tree = Tree::force_create_with_config(config);

    for _ in 0..2000 {
        tree.add_record(&CompoundRecord::random(&mut rng, 8)).unwrap();
    }
}

#[test]
fn finds_records_that_were_added_and_not_ones_that_were_not() {

    let mut rng = seeded("finds_records_that_were_added_and_not_ones_that_were_not");

    for desc_length in [8, 12, 16] {

        let directory = scratch_directory("find");

        let mut config = TreeConfig::default();
        config.desc_length = desc_length;
        config.directory = directory;

        let mut tree = Tree::force_create_with_config(config);

        let target = CompoundRecord::random(&mut rng, desc_length);

        for _ in 0..2000 {
            tree.add_record(&CompoundRecord::random(&mut rng, desc_length)).unwrap();
        }

        assert!(!tree.record_in_tree(&target).unwrap(),
                "dim {}: found a record that was never added", desc_length);

        tree.add_record(&target).unwrap();
        assert!(tree.record_in_tree(&target).unwrap(),
                "dim {}: could not find a record that was added", desc_length);

        // Still findable once later inserts have split its page.
        for _ in 0..2000 {
            tree.add_record(&CompoundRecord::random(&mut rng, desc_length)).unwrap();
        }

        assert!(tree.record_in_tree(&target).unwrap(),
                "dim {}: lost a record across page splits", desc_length);

        tree.flush();
    }
}

#[test]
fn returns_the_true_nearest_neighbors() {

    let mut rng = seeded("returns_the_true_nearest_neighbors");

    // Catches a bound that prunes too hard, which loses a true neighbor.
    for desc_length in [8, 16] {

        let records = random_records(&mut rng, 4000, desc_length);
        let tree = build_tree(&records, desc_length, "knn");

        for _ in 0..20 {
            let query = Descriptor::random(&mut rng, desc_length);
            let nn = tree.get_nearest_neighbors(&query, 10);

            assert_is_true_knn(&tree, &nn, &records, &query, 10);
        }
    }
}

/// Catches the opposite failure: bounds that are correct but do no work.
#[test]
fn the_bounds_avoid_most_of_the_records() {

    let mut rng = seeded("the_bounds_avoid_most_of_the_records");

    // Uniform data is the worst case; 4,000 records at dim 16 prunes nothing.
    let desc_length = 8;
    let records = random_records(&mut rng, 50_000, desc_length);
    let tree = build_tree(&records, desc_length, "pruning");

    for _ in 0..20 {
        let query = Descriptor::random(&mut rng, desc_length);
        let nn = tree.get_nearest_neighbors(&query, 10);

        assert_is_true_knn(&tree, &nn, &records, &query, 10);

        // Which bound rejected varies with the data; that work was avoided does not.
        assert!(nn.records_scanned * 2 < records.len(),
                "scanned {} of {} records, so the bounds are barely working",
                nn.records_scanned, records.len());
    }
}

#[test]
fn a_record_is_its_own_nearest_neighbor() {

    let mut rng = seeded("a_record_is_its_own_nearest_neighbor");

    let desc_length = 8;
    let records = random_records(&mut rng, 3000, desc_length);
    let tree = build_tree(&records, desc_length, "self");

    for record in records.iter().take(25) {
        let nn = tree.get_nearest_neighbors(&record.descriptor, 1);

        assert_eq!(nn.distances[0], 0.0);
        assert_eq!(
            tree.records_at(&nn.indexes)[0].as_ref().unwrap().identifier.to_string(),
            record.compound_identifier.to_string(),
        );
    }
}

#[test]
fn returns_every_record_when_k_exceeds_the_tree() {

    let mut rng = seeded("returns_every_record_when_k_exceeds_the_tree");

    let desc_length = 8;
    let records = random_records(&mut rng, 40, desc_length);
    let tree = build_tree(&records, desc_length, "small");

    let query = Descriptor::random(&mut rng, desc_length);
    let nn = tree.get_nearest_neighbors(&query, 40);

    assert_eq!(nn.distances.len(), 40);
    assert_is_true_knn(&tree, &nn, &records, &query, 40);
}

#[test]
fn returns_a_short_list_when_k_exceeds_the_record_count() {

    let mut rng = seeded("returns_a_short_list_when_k_exceeds_the_record_count");

    let desc_length = 8;
    let records = random_records(&mut rng, 30, desc_length);
    let tree = build_tree(&records, desc_length, "short");

    let query = Descriptor::random(&mut rng, desc_length);
    let nn = tree.get_nearest_neighbors(&query, 200);

    assert_eq!(nn.distances.len(), 30, "asked for 200 of 30 records");
    assert_is_true_knn(&tree, &nn, &records, &query, 30);
}

#[test]
fn an_index_still_opens_after_it_is_moved() {

    let mut rng = seeded("moved");
    let desc_length = 8;
    let records = random_records(&mut rng, 400, desc_length);

    let built_at = scratch_directory("moved_from");
    let opened_at = scratch_directory("moved_to");

    let mut config = TreeConfig::default();
    config.desc_length = desc_length;
    config.directory = built_at.clone();

    let mut tree = Tree::force_create_with_config(config);
    for record in &records {
        tree.add_record(record).unwrap();
    }
    tree.flush();
    drop(tree);

    // The path written into config.yaml no longer exists.
    std::fs::rename(&built_at, &opened_at).expect("could not move the index");

    let moved = ImmutTree::read_from_directory(opened_at);
    assert!(!moved.config.built_at.is_empty(), "a built index should record its date");

    let query = Descriptor::random(&mut rng, desc_length);
    let nn = moved.get_nearest_neighbors(&query, 5);

    assert_eq!(nn.distances.len(), 5, "a moved index should still answer");
    assert_is_true_knn(&moved, &nn, &records, &query, 5);
}

/// Build a tree, then overwrite the first byte of one of its files.
fn corrupt_magic(name: &str, filename: &str) -> String {

    let mut rng = seeded("corrupt_magic");

    let directory = scratch_directory(name);

    let mut config = TreeConfig::default();
    config.directory = directory.clone();

    let mut tree = Tree::force_create_with_config(config);
    for _ in 0..200 {
        tree.add_record(&CompoundRecord::random(&mut rng, 8)).unwrap();
    }
    tree.flush();

    let path = format!("{}/{}", directory, filename);
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.write_all(b"X").unwrap();

    return directory;
}

#[test]
#[should_panic(expected = "not the file it was expected to be")]
fn rejects_a_record_file_with_the_wrong_magic() {
    let directory = corrupt_magic("badrecord", "record");
    ImmutTree::read_from_directory(directory);
}

#[test]
#[should_panic(expected = "not the file it was expected to be")]
fn rejects_a_node_file_with_the_wrong_magic() {
    let directory = corrupt_magic("badnode", "node");
    ImmutTree::read_from_directory(directory);
}

#[test]
#[should_panic(expected = "not the file it was expected to be")]
fn rejects_a_database_with_the_wrong_magic() {
    let directory = corrupt_magic("baddb", "db.db");
    ImmutTree::read_from_directory(directory);
}

/// A fixed dataset rather than random ones
#[test]
fn matches_brute_force_on_the_stored_fixture() {

    let desc_length = 8;
    let path = Path::new("test_data/random_descriptors.txt");

    if !path.exists() {
        println!("skipping: {} not present; regenerate with \
                  rust_server/scripts/generate_test_fixtures.py", path.display());
        return;
    }

    let mut contents = String::new();
    File::open(path).unwrap().read_to_string(&mut contents).unwrap();

    let mut records: Vec<CompoundRecord> = Vec::new();

    for line in contents.split("\n").skip(1) {

        if line.is_empty() {
            break;
        }

        let mut fields = line.split(",");
        let identifier = CompoundIdentifier::from_string(fields.next().unwrap().to_string());
        let data: Vec<f32> = fields.map(|x| x.parse::<f32>().unwrap()).collect();

        assert_eq!(data.len(), desc_length);

        records.push(CompoundRecord {
            compound_identifier: identifier,
            descriptor: Descriptor { data, length: desc_length },
            smiles: "no smiles".to_string(),
            length: desc_length,
        });
    }

    let tree = build_tree(&records, desc_length, "fixture");

    for record in records.iter().take(20) {
        let nn = tree.get_nearest_neighbors(&record.descriptor, 10);
        assert_is_true_knn(&tree, &nn, &records, &record.descriptor, 10);
    }
}

#[test]
fn builds_and_searches_a_coarsely_quantized_space() {

    let mut rng = seeded("builds_and_searches_a_coarsely_quantized_space");

    // Few distinct values per axis, which is what breaks a depth-cycling split.
    let desc_length = 8;

    let mut records: Vec<CompoundRecord> = Vec::new();
    for _ in 0..6000 {
        let mut record = CompoundRecord::random(&mut rng, desc_length);
        for value in record.descriptor.data.iter_mut() {
            *value = value.round();
        }
        records.push(record);
    }

    let tree = build_tree(&records, desc_length, "quantized");

    for _ in 0..10 {
        let query = Descriptor::random(&mut rng, desc_length);
        let nn = tree.get_nearest_neighbors(&query, 10);

        let (_, expected) = brute_force_top_k(&records, &query, 10);
        for i in 0..10 {
            assert!((nn.distances[i] - expected[i]).abs() < 1e-5,
                    "rank {}: tree {} vs brute force {}", i, nn.distances[i], expected[i]);
        }
    }
}

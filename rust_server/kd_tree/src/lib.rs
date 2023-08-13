//! Implementation of a disk-based kd tree with a focus on fast chemical nearest neighbor search.
//!
//! Intended to be used for extremely large chemical dataset like the 3.7 billion compound Enamine
//! REAL dataset. Any low-dimensional ( < 20 ) representation of chemicals can be used for a
//! speedup over brute force nearest neighbor search, but the initial purpose of this library is for
//! a SALSA descriptor to approximate graph edit distance. 
//!
//! Internal nodes of the tree do not contain points, only pointers to neighbors. Leaf nodes
//! contain records with dataset identifiers, compound identifiers, and descriptor values.
//! Collections of internal nodes and collections of compound records are organized into 4kb disk
//! pages, which can be parsed into Rust structs, updated, and written back to disk.
//!
//! TODO
//! - [x] prototype tree construction and querying with tests
//! - [ ] explore alternate construction algorithms
//! - [ ] implement parallel querying
//! - [x] implement server with whole tree in memory
//! - [ ] make descriptor size generic
//!
//!
//!
#![feature(test)]
pub mod node;
pub mod error;
pub mod layout;
pub mod io;
pub mod page;
pub mod tree;
pub mod decision_tree;
pub mod database;
pub mod data;
//! Disk-paged kd-tree for nearest-neighbor search over low-dimensional
//! molecular embeddings. Internal nodes hold only split planes and child
//! pointers; leaf pages hold the records.
pub mod node;
pub mod error;
pub mod layout;
pub mod io;
pub mod page;
pub mod tree;
pub mod database;
pub mod data;
use ndarray::prelude::*;
use ndarray::{Array, OwnedRepr};

use onnxruntime::{environment::Environment, LoggingLevel, GraphOptimizationLevel, tensor::OrtOwnedTensor};

pub fn main() {
    dbg!("AYY");

    let a = arr2(&[[1., 2., 3.], [4., 5., 6.]]);

    dbg!(&a);

    let batch_size = 96;
    let seq_len = 122;
    let num_chars = 39;

    let seq = Array::<u64, _>::ones((batch_size, seq_len));
    let pad_mask = Array::<f32, _>::ones((batch_size, seq_len));
    let avg_mask = Array::<f32, _>::ones((batch_size, seq_len));

    let bigmat = Array::<f32, _>::ones((3, batch_size, seq_len));

    dbg!(&seq);


    let environment = Environment::builder()
    .with_name("test")
    .with_log_level(LoggingLevel::Verbose)
    .build().unwrap();

    let mut session = environment
    .new_session_builder().unwrap()
    .with_optimization_level(GraphOptimizationLevel::Basic).unwrap()
    .with_number_threads(30).unwrap()
    .with_model_from_file("/home/josh/git/Salsa/smallsa_latents/salsa_bigmat.onnx").unwrap();

    println!("START RUN");

    dbg!(&session.inputs);
    //let inputs: Vec::<ArrayBase<OwnedRepr<f32>, _>> = vec![seq, pad_mask, avg_mask, out_mask];
    let inputs = vec![bigmat];

    //let outputs: Vec<OrtOwnedTensor<f32,_>> = session.run((seq, pad_mask, avg_mask, out_mask)).unwrap();
    let outputs: Vec<OrtOwnedTensor<f32,_>> = session.run(inputs).unwrap();

    dbg!(&outputs);

}

pub fn tokenize() {
    todo!();
}

pub fn canonicalize() {
    todo!();
}

pub fn pad() {
    todo!();
}

pub fn expand() {
    todo!();
}

pub fn get_dummy_input() {
    todo!();

}

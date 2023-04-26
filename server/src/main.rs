use kd_tree::{tree, layout};
use kd_tree::node::{Descriptor, CompoundIdentifier};

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use rand::prelude::*;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response};
use hyper::server::Server;

async fn get_nn(req: Request<Body>, tree: Arc<Mutex<tree::Tree>>) -> Result<Response<Body>, Infallible> {
    let smiles = req.uri().path().to_string();
    let num_nn = 100;
    println!("{:?}", smiles);

    let descriptor = get_smiles_embedding(&smiles);

    let mut mg = tree.lock().unwrap();

    dbg!(&descriptor);
    let nn = mg.get_nearest_neighbors(&descriptor, num_nn);
    let s = nn.to_yaml();

    let mut data = "".to_string();
    data += &format!("query: {:?}\n", smiles);
    data += &format!("embedding: {}\n", descriptor.yaml());
    data += &format!("num nn: {:?}\n", num_nn);
    data += "hits: \n";
    data += &s;
    data += "}";

    Ok(Response::new(Body::from(data.as_bytes().to_vec())))
}
fn preprocess_smiles(smiles: &String) -> Result<(), String> {

    //check max length
    
    //check valid characters
    
    //canonicalize

    return Ok(());

}

fn get_smiles_embedding(smiles: &String) -> Descriptor {

    let res = preprocess_smiles(smiles);
    match res {
        Ok(_) => {},
        Err(e) => { panic!();}
    }

    return Descriptor::random(8);
}
#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    
    let directory = "/pool/server_tree".to_string();
    let mut tree = Arc::new(Mutex::new(tree::Tree::read_from_directory(directory)));
    //pretty_env_logger::init();

    // For every connection, we must make a `Service` to handle all
    // incoming HTTP requests on said connection.
    let make_svc = make_service_fn(move |_conn| {
        let tree = tree.clone();
        // This is the `Service` that will handle the connection.
        // `service_fn` is a helper to convert a function that
        // returns a Response into a `Service`.
        async move { Ok::<_, Infallible>(service_fn( move |req| {
            let tree = tree.clone();
            get_nn(req, tree)
        }
            ))}
    });

    let addr = ([127, 0, 0, 1], 3000).into();
    //let addr = ([127, 0, 0, 1], layout::DESCRIPTOR_LENGTH0).into();

    let server = Server::bind(&addr).serve(make_svc);

     //   .serve(move || {
     //       let counter = counter.clone();
     //       service_fn_ok(move |_req| use_counter(counter.clone()))

    println!("Listening on http://{}", addr);

    server.await?;

    Ok(())
}

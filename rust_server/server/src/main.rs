use kd_tree::tree;
use kd_tree::data::Descriptor;

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

//use rand::prelude::*;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response};
use hyper::server::Server;

async fn get_nn(req: Request<Body>, tree: Arc<Mutex<tree::Tree>>) -> Result<Response<Body>, Infallible> {

    let path = req.uri().path().to_string();

    let mut items = path.split("/");

    let method = items.nth(1).unwrap();
    dbg!(method);
    let num_nn = items.nth(0).unwrap();
    dbg!(num_nn);
    let num_nn = num_nn.parse::<usize>().unwrap();

    dbg!(num_nn);

    let data_string = items.next().unwrap();
    dbg!(data_string);

    let retval = match method {

        "descriptor" => query_descriptor(&data_string.to_string(), num_nn, tree),
        "smiles" => query_smiles(&data_string.to_string(), tree),
        _ => Ok(Response::new(Body::from("method not recognized".to_string().as_bytes().to_vec()))),
    };

    return retval;

    /*
    let mut mg = tree.lock().unwrap();
    let descriptor = get_smiles_embedding(&smiles, mg.config.desc_length);


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

    */
    Ok(Response::new(Body::from("ayy".to_string().as_bytes().to_vec())))
}

fn query_descriptor(data_string: &String, num_nn: usize, tree: Arc<Mutex<tree::Tree>>) -> Result<Response<Body>, Infallible> {

    let values = data_string.split(",").map(|x| x.parse::<f32>()).collect::<Vec<_>>();

    let mut parsed_values: Vec<f32> = Vec::new();
    for value in values.into_iter() {
        match value {
            Ok(v) => parsed_values.push(v),
            Err(e) => return Ok(Response::new(Body::from("invalid descriptor".to_string().as_bytes().to_vec()))),
        }
    }

    let mut mg = tree.lock().unwrap();

    match parsed_values.len() == mg.config.desc_length {
        true => (),
        false => return Ok(Response::new(Body::from("invalid descriptor".to_string().as_bytes().to_vec()))),
    }

    let descriptor = Descriptor{ data: parsed_values.clone(), length: parsed_values.len()};

    let nn = mg.get_nearest_neighbors(&descriptor, num_nn);
    let s = nn.to_yaml();

    let mut data = "".to_string();
    data += &format!("query: {:?}\n", "none");
    data += &format!("embedding: {}\n", descriptor.yaml());
    data += &format!("num nn: {:?}\n", num_nn);
    data += "hits: \n";
    data += &s;
    data += "}";

    //let data = "direct descriptor query not implemented".to_string();

    Ok(Response::new(Body::from(data.as_bytes().to_vec())))

}

fn query_smiles(data_string: &String, tree: Arc<Mutex<tree::Tree>>) -> Result<Response<Body>, Infallible> {

    let data = "direct smiles query not implemented".to_string();


    Ok(Response::new(Body::from(data.as_bytes().to_vec())))

}


fn preprocess_smiles(smiles: &String) -> Result<(), String> {

    //check max length
    
    //check valid characters
    
    //canonicalize

    return Ok(());

}

fn get_smiles_embedding(smiles: &String, len: usize) -> Descriptor {

    let res = preprocess_smiles(smiles);
    match res {
        Ok(_) => {},
        Err(_) => { panic!();}
    }

    return Descriptor::random(len);
}
#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let directory = std::env::args().nth(1).expect("No directory specified");
    
    //let directory = "/pool/1_bil_16".to_string();
    let tree = Arc::new(Mutex::new(tree::Tree::read_from_directory(directory)));
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

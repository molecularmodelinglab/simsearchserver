use kd_tree::tree;
use kd_tree::data::Descriptor;

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

//use rand::prelude::*;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response};
use hyper::server::Server;

use serde_yaml::Result;

use reqwest::Error;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    //Dirname containing tree
    #[arg(short, long)]
    dirname: String,

    //Port to listen on
    #[arg(short, long)]
    port: Option<u16>,
}
async fn handle_request(req: Request<Body>, dirname: String) -> Result<Response<Body>> {

    let path = req.uri().path().to_string();
    dbg!(&path);

    let mut items = path.split("/");

    dbg!(&items);
    let method = items.nth(1).unwrap();
    dbg!(&path);
    let retval = match method {

        "nn" => dispatch_nn(req, &dirname).await,
        //"range" => dispatch_range(req, dirname).await,
        "test" => dispatch_test(&dirname).await,
        _ => Ok(Response::new(Body::from("method not recognized".to_string().as_bytes().to_vec()))),
    };
    
    dbg!(&retval);
    return retval;
}

async fn dispatch_test(dirname: &String) -> Result<Response<Body>> {

    let tree = tree::ImmutTree::read_from_directory(dirname.clone());
    let length = tree.config.desc_length;
    let descriptor = Descriptor::random(length);

    dbg!(&descriptor);

    let nn = tree.get_nearest_neighbors(&descriptor, 10);
    let s = serde_yaml::to_string(&nn).unwrap();
    //let s = nn.to_yaml();

    Ok(Response::new(Body::from(s.as_bytes().to_vec())))
}
async fn dispatch_nn(req: Request<Body>, dirname: &String) -> Result<Response<Body>> {

    let tree = tree::ImmutTree::read_from_directory(dirname.clone());
    dbg!("in dispatch_nn");
    let path = req.uri().path().to_string();

    let mut items: Vec<String> = path.split("/").map(|x| x.to_string()).collect();

    dbg!(&items);
    let num_nn = items[2].clone();

    let num_nn = num_nn.parse::<usize>().unwrap();
    let smiles = items[3].clone();
    match smiles.len() {
        0 => return Ok(Response::new(Body::from("No SMILES supplied".to_string().as_bytes().to_vec()))),
        _ => (),
    };

    let smiles_request = format!("http://localhost:5000/smiles/{}", smiles);
    dbg!(&smiles_request);

    let response = reqwest::get(&smiles_request).await.unwrap();
    dbg!(&response);

    match response.status() {
        reqwest::StatusCode::OK => (),
        _ => {
            let message = response.text().await.unwrap();
                return Ok(Response::new(Body::from(message.to_string().as_bytes().to_vec())));
            }

    }
    let embedding = response.text().await.unwrap();
    dbg!(&embedding);

    let embedding: Vec<f32> = serde_json::from_str(&embedding).unwrap();
    dbg!(&embedding);

    let descriptor = Descriptor{ data: embedding.clone(), length: embedding.len()};

    dbg!(&descriptor);

    /*
    let mut mg = tree.lock().unwrap();
    let nn = mg.get_nearest_neighbors(&descriptor, num_nn);
    */
    let nn = tree.get_nearest_neighbors(&descriptor, num_nn);
    let s = nn.to_yaml();

    Ok(Response::new(Body::from(s.as_bytes().to_vec())))
}

async fn dispatch_range(req: Request<Body>, tree: Arc<Mutex<tree::ImmutTree>>) -> Result<Response<Body>> {

    let path = req.uri().path().to_string();

    let mut items = path.split("/");

    Ok(Response::new(Body::from(path.as_bytes().to_vec())))

}


/*
    let values = data_string.split(",").map(|x| x.parse::<f32>()).collect::<Vec<_>>();

    let mut parsed_values: Vec<f32> = Vec::new();
    for value in values.into_iter() {
        matct p value {
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
*/

fn query_smiles(data_string: &String, tree: Arc<Mutex<tree::ImmutTree>>) -> Result<Response<Body>> {

    let data = "direct smiles query not implemented".to_string();


    Ok(Response::new(Body::from(data.as_bytes().to_vec())))

}


fn preprocess_smiles(smiles: &String) -> Result<()> {

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
pub async fn main() -> Result<()> {

    let args = Args::parse();
    dbg!(&args);

    let port = match args.port {
        Some(p) => p,
        None => 3000,
    };


    // For every connection, we must make a `Service` to handle all
    // incoming HTTP requests on said connection.
    let make_svc = make_service_fn(move |_conn| {
        let dirname = args.dirname.clone();

        // This is the `Service` that will handle the connection.
        // `service_fn` is a helper to convert a function that
        // returns a Response into a `Service`.
        async move { Ok::<_, Infallible>(service_fn( move |req| {
            //let tree = tree.clone();
            handle_request(req, dirname.clone())
        }
            ))}
    });

    let addr = ([127, 0, 0, 1], port).into();

    let server = Server::bind(&addr).serve(make_svc);

     //   .serve(move || {
     //       let counter = counter.clone();
     //       service_fn_ok(move |_req| use_counter(counter.clone()))

    println!("Listening on http://{}", addr);

    server.await.unwrap();

    Ok(())
}

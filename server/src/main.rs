use kd_tree::tree;
use kd_tree::node::Descriptor;

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use rand::prelude::*;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response};
use hyper::server::Server;

async fn hello(req: Request<Body>, tree: Arc<Mutex<tree::Tree>>) -> Result<Response<Body>, Infallible> {
    println!("{:?}", req.uri().path());

    let random_arr: [f32; 8] = rand::random();
    let descriptor = Descriptor { data: random_arr };
    dbg!(&descriptor);


    //let descriptor = Descriptor::from_vec(vec![0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]);
    println!("HERE");
    let mut mg = tree.lock().unwrap();
    println!("HERE2");
    let nn = mg.get_nearest_neighbors(&descriptor, 10);
    //let s = format!("{:?}", nn.records[0]);
    //let s = format!("{:?}", nn.to_json());
    let s = nn.to_json();
    //Ok(Response::new(Body::from("Hello World!")))
    Ok(Response::new(Body::from(s)))
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    
    //let db_filename = "/home/josh/db/1_bil_test/".to_string();
    //let node_filename = "/home/josh/db/1_bil_test/node".to_string();
    let node_filename = "/home/josh/tmpfs_mount_point/node".to_string();
    //let record_filename = "/home/josh/db/1_bil_test/record".to_string();
    let record_filename = "/home/josh/big_tmpfs/record".to_string();
    let mut tree = Arc::new(Mutex::new(tree::Tree::from_filenames(node_filename.clone(), record_filename.clone())));
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
            hello(req, tree)
        }
            ))}
    });

    let addr = ([127, 0, 0, 1], 3000).into();
    //let addr = ([127, 0, 0, 1], 80).into();

    let server = Server::bind(&addr).serve(make_svc);

     //   .serve(move || {
     //       let counter = counter.clone();
     //       service_fn_ok(move |_req| use_counter(counter.clone()))

    println!("Listening on http://{}", addr);

    server.await?;

    Ok(())
}

use kd_tree::{tree, layout};
use kd_tree::node::Descriptor;

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use rand::prelude::*;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response};
use hyper::server::Server;

async fn hello(req: Request<Body>, tree: Arc<Mutex<tree::Tree>>) -> Result<Response<Body>, Infallible> {
    println!("{:?}", req.uri().path());



    //let descriptor = Descriptor::from_vec(vec![0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]);
    println!("HERE");
    let mut mg = tree.lock().unwrap();
    let descriptor = Descriptor::random(mg.config.desc_length);
    dbg!(&descriptor);
    println!("HERE2");
    let nn = mg.get_nearest_neighbors(&descriptor, 100);
    //let s = format!("{:?}", nn.records[0]);
    //let s = format!("{:?}", nn.to_json());
    let s = nn.to_json();
    //Ok(Response::new(Body::from("Hello World!")))
    Ok(Response::new(Body::from(s)))
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    
    let directory = "/home/josh/db/config_test/".to_string();
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
            hello(req, tree)
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

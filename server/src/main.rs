use kd_tree::tree;
use kd_tree::node::Descriptor;

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response};
use hyper::server::Server;

async fn hello(req: Request<Body>, tree: Arc<Mutex<tree::Tree>>) -> Result<Response<Body>, Infallible> {
    println!("{:?}", req.uri().path());

    let descriptor = Descriptor::from_vec(vec![1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0]);
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
    
    let db_filename = "/home/josh/db/chembl_8/".to_string();
    let mut tree = Arc::new(Mutex::new(tree::Tree::new(db_filename.clone(), false)));
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

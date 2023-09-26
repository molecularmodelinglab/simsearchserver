use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response};
use hyper::server::Server;

use serde_json::Result;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {

    //Port to listen on
    #[arg(short, long)]
    port: Option<u16>,
}
async fn handle_request(req: Request<Body>) -> Result<Response<Body>> {

    let path = req.uri().path().to_string();

    let mut items = path.split("/");

    let method = items.nth(1).unwrap();

    dbg!(&path);
    let retval = match method {

        "embed" => dispatch_embed(req),
        _ => Ok(Response::new(Body::from("method not recognized".to_string().as_bytes().to_vec()))),
    };
    
    return retval;
}

fn dispatch_embed(req: Request<Body>) -> Result<Response<Body>> {

    dbg!("in dispatch_smiles");
    let path = req.uri().path().to_string();

    let mut items = path.split("/");

    let target = items.nth(2).unwrap();
    dbg!(&target);
    match target {
        "" => return Ok(Response::new(Body::from("No target supplied".to_string().as_bytes().to_vec()))),
        _ => (),
    }

    let response_vec = [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5].to_vec();

    let response_string = serde_json::to_string(&response_vec).unwrap();
    Ok(Response::new(Body::from(response_string.as_bytes().to_vec())))

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

        // This is the `Service` that will handle the connection.
        // `service_fn` is a helper to convert a function that
        // returns a Response into a `Service`.
        async move { Ok::<_, Infallible>(service_fn( move |req| {
            handle_request(req)
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

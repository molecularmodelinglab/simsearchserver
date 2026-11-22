//! Vector nearest-neighbor search service over a kd-tree built by `builder`.
//!
//! Knows nothing about SMILES or embedding models: callers embed their query
//! and send the resulting vector.
//!
//! - `GET  /health`            - liveness, plus the descriptor length this index expects
//! - `GET  /nn/{k}/{v0,v1,..}` - k nearest neighbors of a comma-separated vector
//! - `POST /nn/{k}`            - same, with body `[v0, v1, ...]` or `{"descriptor": [...]}`
//!
//! Prefer `POST` from client code; a high-dimensional descriptor in a URL runs
//! into path-length limits.

use kd_tree::tree::{ImmutTree, NearestNeighbors};
use kd_tree::data::Descriptor;

use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, StatusCode};
use hyper::server::Server;

use serde::{Deserialize, Serialize};

use clap::Parser;

const MAX_NEIGHBORS: usize = 1000;
const MAX_BODY_BYTES: usize = 1024 * 1024;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory containing the tree (config.yaml, node, record, db.db)
    #[arg(short, long)]
    dirname: String,

    /// Port to listen on
    #[arg(short, long, default_value_t = 3000)]
    port: u16,

    /// Address to bind. Defaults to loopback; set to 0.0.0.0 to accept
    /// off-host connections.
    #[arg(long, default_value = "127.0.0.1")]
    host: IpAddr,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum QueryBody {
    Bare(Vec<f32>),
    Wrapped { descriptor: Vec<f32> },
}

impl QueryBody {
    fn into_vec(self) -> Vec<f32> {
        match self {
            QueryBody::Bare(v) => v,
            QueryBody::Wrapped { descriptor } => descriptor,
        }
    }
}

#[derive(Serialize)]
struct Hit {
    identifier: String,
    smiles: String,
    distance: f32,
    embedding: Vec<f32>,
}

#[derive(Serialize)]
struct QueryResponse {
    num_hits: usize,
    hits: Vec<Hit>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    desc_length: usize,
    num_records: u64,
    num_internal_nodes: usize,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_REQUEST, message: message.into() }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self { status: StatusCode::NOT_FOUND, message: message.into() }
    }

    fn into_response(self) -> Response<Body> {
        let body = serde_json::to_string(&ErrorResponse { error: self.message })
            .unwrap_or_else(|_| r#"{"error":"internal error"}"#.to_string());

        return json_response(self.status, body);
    }
}

fn json_response(status: StatusCode, body: String) -> Response<Body> {
    return Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(body))
        .expect("response with valid status and headers");
}

async fn handle_request(req: Request<Body>, tree: Arc<ImmutTree>) -> Result<Response<Body>, Infallible> {
    let result = route(req, tree).await;

    let response = match result {
        Ok(response) => response,
        Err(e) => e.into_response(),
    };

    return Ok(response);
}

async fn route(req: Request<Body>, tree: Arc<ImmutTree>) -> Result<Response<Body>, ApiError> {
    let path = req.uri().path().to_string();

    // A path starts with '/', so the first split segment is always empty.
    let segments: Vec<&str> = path.split('/').skip(1).collect();

    let head = match segments.first() {
        Some(s) => *s,
        None => return Err(ApiError::not_found("no path supplied")),
    };

    match (req.method(), head) {
        (&Method::GET, "health") => handle_health(&tree),
        (&Method::GET, "nn") => handle_nn_get(&segments, &tree),
        (&Method::POST, "nn") => handle_nn_post(req, &segments, &tree).await,
        (_, "health") | (_, "nn") => Err(ApiError {
            status: StatusCode::METHOD_NOT_ALLOWED,
            message: format!("method {} not allowed for /{}", req.method(), head),
        }),
        _ => Err(ApiError::not_found(format!(
            "unknown endpoint {:?}; available: /health, /nn/{{k}}/{{vector}}, POST /nn/{{k}}",
            path
        ))),
    }
}

fn handle_health(tree: &ImmutTree) -> Result<Response<Body>, ApiError> {
    let body = HealthResponse {
        status: "ok",
        desc_length: tree.config.desc_length,
        num_records: tree.database.num_entries(),
        num_internal_nodes: tree.node_handler.len(),
    };

    let body = serde_json::to_string(&body)
        .map_err(|e| ApiError { status: StatusCode::INTERNAL_SERVER_ERROR, message: e.to_string() })?;

    return Ok(json_response(StatusCode::OK, body));
}

fn handle_nn_get(segments: &[&str], tree: &ImmutTree) -> Result<Response<Body>, ApiError> {
    let k = match segments.get(1) {
        Some(s) => parse_k(s)?,
        None => return Err(ApiError::bad_request(
            "missing neighbor count; expected /nn/{k}/{comma-separated vector}",
        )),
    };

    let vector_str = match segments.get(2) {
        Some(s) if !s.is_empty() => *s,
        _ => return Err(ApiError::bad_request(
            "missing query vector; expected /nn/{k}/{comma-separated vector}",
        )),
    };

    let values = parse_descriptor_csv(vector_str)?;

    return run_query(values, k, tree);
}

async fn handle_nn_post(
    req: Request<Body>,
    segments: &[&str],
    tree: &ImmutTree,
) -> Result<Response<Body>, ApiError> {
    let k = match segments.get(1) {
        Some(s) => parse_k(s)?,
        None => return Err(ApiError::bad_request("missing neighbor count; expected POST /nn/{k}")),
    };

    let body = hyper::body::to_bytes(req.into_body())
        .await
        .map_err(|e| ApiError::bad_request(format!("could not read request body: {}", e)))?;

    if body.len() > MAX_BODY_BYTES {
        return Err(ApiError {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: format!("request body exceeds {} bytes", MAX_BODY_BYTES),
        });
    }

    let parsed: QueryBody = serde_json::from_slice(&body).map_err(|e| {
        ApiError::bad_request(format!(
            "could not parse JSON body as a float array or {{\"descriptor\": [...]}}: {}",
            e
        ))
    })?;

    return run_query(parsed.into_vec(), k, tree);
}

fn parse_k(s: &str) -> Result<usize, ApiError> {
    let k = s
        .parse::<usize>()
        .map_err(|_| ApiError::bad_request(format!("neighbor count {:?} is not a positive integer", s)))?;

    if k == 0 {
        return Err(ApiError::bad_request("neighbor count must be at least 1"));
    }

    if k > MAX_NEIGHBORS {
        return Err(ApiError::bad_request(format!(
            "neighbor count {} exceeds the maximum of {}",
            k, MAX_NEIGHBORS
        )));
    }

    return Ok(k);
}

fn parse_descriptor_csv(s: &str) -> Result<Vec<f32>, ApiError> {
    let mut values: Vec<f32> = Vec::new();

    for (i, field) in s.split(',').enumerate() {
        let value = field.trim().parse::<f32>().map_err(|_| {
            ApiError::bad_request(format!("descriptor component {} ({:?}) is not a number", i, field))
        })?;

        values.push(value);
    }

    return Ok(values);
}

fn run_query(values: Vec<f32>, k: usize, tree: &ImmutTree) -> Result<Response<Body>, ApiError> {
    let expected = tree.config.desc_length;

    if values.len() != expected {
        return Err(ApiError::bad_request(format!(
            "query descriptor has {} dimensions but this index was built for {}",
            values.len(),
            expected
        )));
    }

    if let Some(i) = values.iter().position(|x| !x.is_finite()) {
        return Err(ApiError::bad_request(format!(
            "descriptor component {} is not a finite number",
            i
        )));
    }

    let descriptor = Descriptor { data: values, length: expected };

    let neighbors = tree.get_nearest_neighbors(&descriptor, k);

    let body = serde_json::to_string(&to_response(neighbors))
        .map_err(|e| ApiError { status: StatusCode::INTERNAL_SERVER_ERROR, message: e.to_string() })?;

    return Ok(json_response(StatusCode::OK, body));
}

fn to_response(neighbors: NearestNeighbors) -> QueryResponse {
    let mut hits: Vec<Hit> = Vec::new();

    for (distance, record) in neighbors.distances.iter().zip(neighbors.records.iter()) {
        let record = match record {
            Some(r) => r,
            // Index and SMILES database disagree; skip rather than fail the query.
            None => continue,
        };

        hits.push(Hit {
            identifier: record.compound_identifier.to_string(),
            smiles: record.smiles.clone(),
            distance: *distance,
            embedding: record.descriptor.data.clone(),
        });
    }

    return QueryResponse { num_hits: hits.len(), hits };
}

#[tokio::main]
pub async fn main() {
    let args = Args::parse();

    println!("Loading tree from {} ...", &args.dirname);
    let tree = Arc::new(ImmutTree::read_from_directory(args.dirname.clone()));
    println!(
        "Loaded {} records, {} internal nodes, descriptor length {}",
        tree.database.num_entries(),
        tree.node_handler.len(),
        tree.config.desc_length,
    );

    let make_svc = make_service_fn(move |_conn| {
        let tree = Arc::clone(&tree);

        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                handle_request(req, Arc::clone(&tree))
            }))
        }
    });

    let addr = SocketAddr::new(args.host, args.port);

    let server = Server::bind(&addr).serve(make_svc);

    println!("Listening on http://{}", addr);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
        std::process::exit(1);
    }
}

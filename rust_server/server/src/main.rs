//! Nearest-neighbor service over a kd-tree; callers send a vector, not SMILES.

use kd_tree::tree::{ImmutTree, NearestNeighbors};
use kd_tree::data::Descriptor;
use kd_tree::fingerprints::{self, FingerprintStore};

use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, StatusCode};
use hyper::server::Server;

use serde::{Deserialize, Serialize};

use clap::Parser;

const MAX_NEIGHBORS: usize = 1024;
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

    /// Defaults to loopback; set 0.0.0.0 to accept off-host connections.
    #[arg(long, default_value = "127.0.0.1")]
    host: IpAddr,

    /// Skip faulting the record file into the page cache before serving
    #[arg(long)]
    no_warm: bool,
}

/// What a request is answered from.
struct State {
    tree: ImmutTree,
    fingerprints: Option<FingerprintStore>,
    /// Served verbatim at `/transform`; the server does not interpret it.
    transform: Option<String>,
}

/// The subset of `config.resolved.yaml` the server can check for itself.
#[derive(Deserialize)]
struct Resolved {
    format_version: Option<u32>,
    descriptor_length: Option<usize>,
    record_count: Option<u64>,
    fingerprint: Option<ResolvedFingerprint>,
}

#[derive(Deserialize)]
struct ResolvedFingerprint {
    bytes_per_record: Option<usize>,
}

/// Refuses rather than answers, because a mismatch is wrong on every query.
fn check_resolved(directory: &str, tree: &ImmutTree, store: Option<&FingerprintStore>) {
    let path = format!("{}/config.resolved.yaml", directory);

    let text = match std::fs::read_to_string(&path) {
        Err(_) => {
            println!("No config.resolved.yaml; nothing to validate against");
            return;
        },
        Ok(text) => text,
    };

    let resolved: Resolved = serde_yaml::from_str(&text)
        .unwrap_or_else(|e| panic!("could not parse {}: {}", path, e));

    let mut wrong: Vec<String> = Vec::new();

    if let Some(version) = resolved.format_version {
        if version != kd_tree::layout::FORMAT_VERSION {
            wrong.push(format!("format_version {} against {}",
                               version, kd_tree::layout::FORMAT_VERSION));
        }
    }

    if let Some(length) = resolved.descriptor_length {
        if length != tree.config.desc_length {
            wrong.push(format!("descriptor_length {} against {}",
                               length, tree.config.desc_length));
        }
    }

    if let Some(count) = resolved.record_count {
        if count != tree.database.num_entries() {
            wrong.push(format!("record_count {} against {}",
                               count, tree.database.num_entries()));
        }
    }

    let declared = resolved.fingerprint.as_ref().and_then(|f| f.bytes_per_record);
    match (declared, store.map(|s| s.bytes_per_record())) {
        (Some(a), Some(b)) if a != b =>
            wrong.push(format!("fingerprint bytes_per_record {} against {}", a, b)),
        (Some(a), None) =>
            wrong.push(format!("fingerprint bytes_per_record {} but no fingerprint file", a)),
        _ => {},
    }

    if !wrong.is_empty() {
        panic!("{} does not describe this index: {}", path, wrong.join("; "));
    }

    println!("Validated against {}", path);
}

#[derive(Deserialize)]
#[serde(untagged)]
enum QueryBody {
    Bare(Vec<f32>),
    Wrapped {
        descriptor: Vec<f32>,
        /// The query's fingerprint; without it hits stay in embedding order.
        #[serde(default)]
        fingerprint: Option<Vec<u8>>,
        #[serde(default)]
        num_neighbors: Option<usize>,
    },
}

impl QueryBody {
    fn into_parts(self) -> (Vec<f32>, Option<Vec<u8>>, Option<usize>) {
        match self {
            QueryBody::Bare(v) => (v, None, None),
            QueryBody::Wrapped { descriptor, fingerprint, num_neighbors } =>
                (descriptor, fingerprint, num_neighbors),
        }
    }
}

#[derive(Serialize)]
struct Hit {
    identifier: String,
    smiles: String,
    distance: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    similarity: Option<f32>,
}

/// Microseconds; `insert` is counted inside `scan`, the remainder of `total` is the heap.
#[derive(Serialize)]
struct Timing {
    page_fetch: f64,
    scan: f64,
    insert: f64,
    node: f64,
    rerank: f64,
    total: f64,
}

#[derive(Serialize)]
struct QueryResponse {
    num_hits: usize,
    pages_visited: usize,
    pages_pruned: usize,
    nodes_visited: usize,
    records_scanned: usize,
    timing: Timing,
    hits: Vec<Hit>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    desc_length: usize,
    num_records: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    fingerprint_bytes: Option<usize>,
    has_transform: bool,
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

async fn handle_request(req: Request<Body>, state: Arc<State>) -> Result<Response<Body>, Infallible> {
    let result = route(req, state).await;

    let response = match result {
        Ok(response) => response,
        Err(e) => e.into_response(),
    };

    return Ok(response);
}

async fn route(req: Request<Body>, state: Arc<State>) -> Result<Response<Body>, ApiError> {
    let path = req.uri().path().to_string();

    // A path starts with '/', so the first split segment is always empty.
    let segments: Vec<&str> = path.split('/').skip(1).collect();

    let head = match segments.first() {
        Some(s) => *s,
        None => return Err(ApiError::not_found("no path supplied")),
    };

    match (req.method(), head) {
        (&Method::GET, "health") => handle_health(&state),
        (&Method::GET, "transform") => handle_transform(&state),
        (&Method::GET, "nn") => handle_nn_get(&segments, &state),
        (&Method::POST, "nn") => handle_nn_post(req, &segments, &state).await,
        (_, "health") | (_, "nn") | (_, "transform") => Err(ApiError {
            status: StatusCode::METHOD_NOT_ALLOWED,
            message: format!("method {} not allowed for /{}", req.method(), head),
        }),
        _ => Err(ApiError::not_found(format!(
            "unknown endpoint {:?}; available: /health, /transform, /nn/{{k}}/{{vector}}, POST /nn/{{k}}",
            path
        ))),
    }
}

fn handle_health(state: &State) -> Result<Response<Body>, ApiError> {
    let body = HealthResponse {
        status: "ok",
        desc_length: state.tree.config.desc_length,
        num_records: state.tree.database.num_entries(),
        num_internal_nodes: state.tree.node_handler.len(),
        fingerprint_bytes: state.fingerprints.as_ref().map(|s| s.bytes_per_record()),
        has_transform: state.transform.is_some(),
    };

    let body = serde_json::to_string(&body)
        .map_err(|e| ApiError { status: StatusCode::INTERNAL_SERVER_ERROR, message: e.to_string() })?;

    return Ok(json_response(StatusCode::OK, body));
}

fn handle_transform(state: &State) -> Result<Response<Body>, ApiError> {
    return match state.transform.as_ref() {
        Some(body) => Ok(json_response(StatusCode::OK, body.clone())),
        None => Err(ApiError::not_found(
            "this index ships no transform; it was built without a projection")),
    };
}

fn handle_nn_get(segments: &[&str], state: &State) -> Result<Response<Body>, ApiError> {
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

    return run_query(values, k, None, None, state);
}

async fn handle_nn_post(
    req: Request<Body>,
    segments: &[&str],
    state: &State,
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

    let (values, fingerprint, num_neighbors) = parsed.into_parts();

    return run_query(values, k, fingerprint, num_neighbors, state);
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

fn run_query(
    values: Vec<f32>,
    k: usize,
    fingerprint: Option<Vec<u8>>,
    num_neighbors: Option<usize>,
    state: &State,
) -> Result<Response<Body>, ApiError> {
    let expected = state.tree.config.desc_length;

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

    if let Some(bytes) = fingerprint.as_ref() {
        match state.fingerprints.as_ref() {
            None => return Err(ApiError::bad_request(
                "a query fingerprint was sent but this index has no fingerprint file")),
            Some(store) if store.bytes_per_record() != bytes.len() =>
                return Err(ApiError::bad_request(format!(
                    "query fingerprint is {} bytes but this index stores {}",
                    bytes.len(), store.bytes_per_record()))),
            Some(_) => {},
        }
    }

    let descriptor = Descriptor { data: values, length: expected };

    let neighbors = state.tree.get_nearest_neighbors(&descriptor, k);

    let body = serde_json::to_string(
        &to_response(neighbors, fingerprint.as_deref(), num_neighbors, state))
        .map_err(|e| ApiError { status: StatusCode::INTERNAL_SERVER_ERROR, message: e.to_string() })?;

    return Ok(json_response(StatusCode::OK, body));
}

fn to_response(
    neighbors: NearestNeighbors,
    query: Option<&[u8]>,
    num_neighbors: Option<usize>,
    state: &State,
) -> QueryResponse {
    let started = std::time::Instant::now();

    // Ranked first, so the database is only asked for the hits served.
    let mut order: Vec<usize> = (0..neighbors.indexes.len()).collect();
    let mut similarities: Vec<Option<f32>> = vec![None; neighbors.indexes.len()];

    if let (Some(query), Some(store)) = (query, state.fingerprints.as_ref()) {
        for i in &order {
            similarities[*i] = store.get(neighbors.indexes[*i])
                .map(|stored| fingerprints::tanimoto(query, stored));
        }

        // Descending similarity, with embedding distance breaking ties.
        order.sort_by(|a, b| {
            similarities[*b].unwrap_or(0.0).total_cmp(&similarities[*a].unwrap_or(0.0))
                .then(neighbors.distances[*a].total_cmp(&neighbors.distances[*b]))
        });
    }

    if let Some(n) = num_neighbors {
        order.truncate(n);
    }

    let rerank_micros = started.elapsed().as_nanos() as f64 / 1000.0;

    let chosen: Vec<u64> = order.iter().map(|i| neighbors.indexes[*i]).collect();
    let records = state.tree.records_at(&chosen);

    let mut hits: Vec<Hit> = Vec::new();

    for (slot, i) in order.iter().enumerate() {
        let record = match &records[slot] {
            Some(r) => r,
            // Index and SMILES database disagree; skip rather than fail the query.
            None => continue,
        };

        hits.push(Hit {
            identifier: record.identifier.to_string(),
            smiles: record.smiles.clone(),
            distance: neighbors.distances[*i],
            similarity: similarities[*i],
        });
    }

    return QueryResponse {
        num_hits: hits.len(),
        pages_visited: neighbors.record_pages_visited,
        pages_pruned: neighbors.record_pages_pruned,
        nodes_visited: neighbors.nodes_visited,
        records_scanned: neighbors.records_scanned,
        timing: Timing {
            page_fetch: neighbors.page_fetch_nanos as f64 / 1000.0,
            scan: neighbors.scan_nanos as f64 / 1000.0,
            insert: neighbors.insert_nanos as f64 / 1000.0,
            node: neighbors.node_nanos as f64 / 1000.0,
            rerank: rerank_micros,
            total: neighbors.total_nanos as f64 / 1000.0,
        },
        hits,
    };
}

#[tokio::main]
pub async fn main() {
    let args = Args::parse();

    println!("Loading tree from {} ...", &args.dirname);
    let tree = ImmutTree::read_from_directory(args.dirname.clone());
    println!(
        "Loaded {} records, {} internal nodes, descriptor length {}",
        tree.database.num_entries(),
        tree.node_handler.len(),
        tree.config.desc_length,
    );

    let fingerprint_path = fingerprints::path_in(&args.dirname);
    let store = match std::path::Path::new(&fingerprint_path).exists() {
        false => {
            println!("No fingerprint file; hits will be served in embedding order");
            None
        },
        true => {
            let store = FingerprintStore::open(&fingerprint_path, tree.database.num_entries())
                .expect("could not open the fingerprint file");
            println!("Loaded {} fingerprints of {} bytes", store.len(), store.bytes_per_record());
            Some(store)
        },
    };

    if !args.no_warm {
        let started = std::time::Instant::now();
        let mut faulted = 0usize;

        if let Some(store) = store.as_ref() {
            faulted += store.warm().unwrap_or(0);
        }

        faulted += tree.database.warm();

        match tree.record_handler.warm() {
            Ok(bytes) => {
                faulted += bytes;
                let gb = faulted as f64 / (1 << 30) as f64;
                let seconds = started.elapsed().as_secs_f64();
                println!("Warmed {:.1} GB in {:.1}s ({:.0} MB/s)",
                         gb, seconds, gb * 1024.0 / seconds);
            },
            Err(e) => println!("Could not warm the record file: {:?}", e),
        }
    }

    check_resolved(&args.dirname, &tree, store.as_ref());

    let transform = std::fs::read_to_string(format!("{}/transform.json", args.dirname)).ok();
    if transform.is_some() {
        println!("Serving the projection at /transform");
    }

    let state = Arc::new(State { tree, fingerprints: store, transform });

    let make_svc = make_service_fn(move |_conn| {
        let state = Arc::clone(&state);

        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                handle_request(req, Arc::clone(&state))
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

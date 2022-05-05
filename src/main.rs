mod daemon;
mod error;
mod prometheus;
mod snapshot;
mod token;
mod validator_info_utils;

use std::{
    io,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::SystemTime,
};

use clap::Parser;
use daemon::Daemon;
use prometheus::{write_metric, Metric, MetricFamily};
use snapshot::{Config, SnapshotClient, SnapshotError};
use solana_client::rpc_client::RpcClient;
use solana_program::clock::{Epoch, Slot};
use solana_sdk::commitment_config::CommitmentConfig;
use tiny_http::{Header, Request, Response, Server};

pub type Result<T> = std::result::Result<T, SnapshotError>;

#[derive(Parser, Debug)]
pub struct Opts {
    /// URL of cluster to connect to (e.g., https://api.devnet.solana.com for solana devnet)
    #[clap(long, default_value = "http://127.0.0.1:8899")]
    cluster: String,

    /// Listen address and port for the http server.
    #[clap(long, default_value = "0.0.0.0:8928")]
    listen: String,

    /// Poll interval in seconds.
    #[clap(long, default_value = "5")]
    poll_interval_seconds: u32,
}

#[derive(Clone)]
pub struct Metrics {
    /// Current observed slot.
    current_slot: Slot,

    /// Current observed slot.
    current_epoch: Epoch,

    /// Solana version.
    solana_version: String,

    /// Time we finished all RPC calls.
    produced_at: SystemTime,

    /// Number of times that we polled Solana (possibly more than one RPC call per poll).
    pub polls: u64,

    /// Number of times that we received an error.
    pub errors: u64,
}

impl Metrics {
    pub fn write_prometheus<W: io::Write>(&self, out: &mut W) -> io::Result<()> {
        write_metric(
            out,
            &MetricFamily {
                name: "hydrant_polls_total",
                help: "Number of times we polled since start",
                type_: "counter",
                metrics: vec![Metric::new(self.polls)],
            },
        )?;

        write_metric(
            out,
            &MetricFamily {
                name: "hydrant_errors_total",
                help: "Number of times we encountered an error while polling",
                type_: "counter",
                metrics: vec![Metric::new(self.errors)],
            },
        )?;

        write_metric(
            out,
            &MetricFamily {
                name: "solana_current_slot",
                help: "Current slot this validator is at",
                type_: "gauge",
                metrics: vec![Metric::new(self.current_slot).at(self.produced_at)],
            },
        )?;

        write_metric(
            out,
            &MetricFamily {
                name: "solana_current_epoch",
                help: "Current epoch this validator is at",
                type_: "gauge",
                metrics: vec![Metric::new(self.current_epoch).at(self.produced_at)],
            },
        )?;

        write_metric(
            out,
            &MetricFamily {
                name: "solana_version",
                help: "version of the Solana node",
                type_: "gauge",
                metrics: vec![Metric::new(1)
                    .with_label("version", self.solana_version.clone())
                    .at(self.produced_at)],
            },
        )?;

        Ok(())
    }
}

pub type MetricsMutex = Mutex<Arc<Metrics>>;

fn serve_request(
    request: Request,
    metrics_mutex: &MetricsMutex,
) -> core::result::Result<(), std::io::Error> {
    // Take the current snapshot. This only holds the lock briefly, and does
    // not prevent other threads from updating the snapshot while this request
    // handler is running.
    let snapshot = metrics_mutex.lock().unwrap().clone();

    // It might be that no snapshot is available yet. This happens when we just
    // started the server, and the main loop has not yet queried the RPC for the
    // latest state.

    let mut out: Vec<u8> = Vec::new();
    match snapshot.write_prometheus(&mut out) {
        Ok(_) => {
            let content_type = Header::from_bytes(
                &b"Content-Type"[..],
                &b"text/plain; version=0.0.4; charset=UTF-8"[..],
            )
            .expect("Static header value, does not fail at runtime.");
            request.respond(Response::from_data(out).with_header(content_type))
        }
        Err(err) => request.respond(Response::from_string(err.to_string()).with_status_code(500)),
    }
}

fn start_http_server(opts: &Opts, metrics_mutex: Arc<MetricsMutex>) -> Vec<JoinHandle<()>> {
    let server = match Server::http(opts.listen.clone()) {
        Ok(server) => Arc::new(server),
        Err(err) => {
            eprintln!(
                "Error: {}\nFailed to start http server on {}. Is the daemon already running?",
                err, &opts.listen,
            );
            std::process::exit(1);
        }
    };

    println!("Http server listening on {}", &opts.listen);

    // Spawn a number of http handler threads, so we can handle requests in
    // parallel.
    (0..num_cpus::get())
        .map(|i| {
            // Create one db connection per thread.
            let server_clone = server.clone();
            let snapshot_mutex_clone = metrics_mutex.clone();
            std::thread::Builder::new()
                .name(format!("http_handler_{}", i))
                .spawn(move || {
                    for request in server_clone.incoming_requests() {
                        // Ignore any errors; if we fail to respond, then there's little
                        // we can do about it here ... the client should just retry.
                        let _ = serve_request(request, &*snapshot_mutex_clone);
                    }
                })
                .expect("Failed to spawn http handler thread.")
        })
        .collect()
}

fn main() {
    let opts = Opts::parse();
    solana_logger::setup_with_default("solana=info");

    let rpc_client =
        RpcClient::new_with_commitment(opts.cluster.clone(), CommitmentConfig::confirmed());
    let snapshot_client = SnapshotClient::new(rpc_client);

    let mut config = Config {
        client: snapshot_client,
    };

    let mut daemon = Daemon::new(&mut config, &opts);
    let _http_threads = start_http_server(&opts, daemon.snapshot_mutex.clone());
    daemon.run();
}

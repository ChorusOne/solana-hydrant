use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use crate::{snapshot::SnapshotClientConfig, Metrics, MetricsMutex, Opts};
use rand::{rngs::ThreadRng, Rng};
use solana_program::clock::Clock;

pub struct Daemon<'a> {
    pub config: &'a mut SnapshotClientConfig<'a>,
    opts: &'a Opts,

    /// Random number generator used for exponential backoff with jitter on errors.
    pub rng: ThreadRng,

    /// The instant after we successfully queried the on-chain state for the last time.
    pub last_read_success: Instant,

    /// Metrics counters to track status.
    pub metrics: Metrics,

    /// Mutex where we publish the latest snapshot for use by the webserver.
    pub snapshot_mutex: Arc<MetricsMutex>,
}

struct RpcData {
    clock: Clock,
    version: String,
}

impl<'a> Daemon<'a> {
    pub fn new(config: &'a mut SnapshotClientConfig<'a>, opts: &'a Opts) -> Self {
        let metrics = Metrics {
            current_slot: 0,
            current_epoch: 0,
            solana_version: "0.0.0".to_owned(),
            polls: 0,
            errors: 0,
            produced_at: SystemTime::UNIX_EPOCH,
        };
        Daemon {
            config,
            opts,
            rng: rand::thread_rng(),
            last_read_success: Instant::now(),
            metrics: metrics.clone(),
            snapshot_mutex: Arc::new(Mutex::new(Arc::new(metrics))),
        }
    }

    fn get_sleep_time_after_error(&mut self) -> Duration {
        // For the sleep time we use exponential backoff with jitter [1]. By taking
        // the time since the last success as the target sleep time, we get
        // exponential backoff. We clamp this to ensure we don't wait indefinitely.
        // 1: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
        let time_since_last_success = self.last_read_success.elapsed();
        let min_sleep_time = Duration::from_secs_f32(0.2);
        let max_sleep_time = Duration::from_secs_f32(300.0);
        let target_sleep_time = time_since_last_success.clamp(min_sleep_time, max_sleep_time);
        let sleep_time = self
            .rng
            .gen_range(Duration::from_secs(0)..target_sleep_time);
        println!("Sleeping {:?} after error ...", sleep_time);
        sleep_time
    }

    pub fn run(&mut self) -> ! {
        loop {
            self.metrics.polls += 1;
            let sleep_time = match self.config.with_snapshot(|config| {
                let clock = config.client.get_clock()?;
                let version = config.client.get_version()?;
                Ok(RpcData {
                    clock,
                    version: version.solana_core,
                })
            }) {
                Ok(rpc_data) => {
                    // Update metrics from RPC.
                    self.metrics.current_slot = rpc_data.clock.slot;
                    self.metrics.current_epoch = rpc_data.clock.epoch;
                    self.metrics.solana_version = rpc_data.version;
                    self.metrics.produced_at = SystemTime::now();

                    // Update metrics snapshot.
                    *self.snapshot_mutex.lock().unwrap() = Arc::new(self.metrics.clone());
                    std::time::Duration::from_secs(self.opts.poll_interval_seconds as u64)
                }
                Err(err) => {
                    println!("Error while obtaining on-chain state.");
                    err.print_pretty();
                    self.metrics.errors += 1;
                    self.get_sleep_time_after_error()
                }
            };
            std::thread::sleep(sleep_time);
        }
    }
}

// fn get_metrics_from_solana_rpc(config: &mut SnapshotClientConfig, opts: &Opts) -> ListenerResult {
//     let result = config.with_snapshot(|config| {
//         let clock = config.client.get_clock()?;
//         Ok(clock)
//     });

//     match result {
//         Err(err) => ListenerResult::ErrSnapshot(err),
//         Ok(clock) => {}
//     }
// }

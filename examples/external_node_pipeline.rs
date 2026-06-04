use piper::{
    BufferLease, Node, NodeContext, PiperConfig, RecvOutputError, SendInputError,
    TelemetryLogConfig, anchor, node, pipeline,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const BATCH_COUNT: usize = 8_192;
const BATCH_SIZE: usize = 256;
const MANAGED_ROUNDS: usize = 122_880;
const EXTERNAL_ROUNDS: usize = 122_880;
const EXTERNAL_WORKERS: usize = 2;
const CSV_TELEMETRY_INTERVAL: Duration = Duration::from_millis(50);

static MANAGED_BATCHES: AtomicUsize = AtomicUsize::new(0);
static EXTERNAL_BATCHES: AtomicUsize = AtomicUsize::new(0);

type Batch = Vec<u64>;
type BatchLease = BufferLease<Batch>;

#[derive(Debug, Error)]
enum ExampleError {
    #[error("external worker failed")]
    ExternalWorker,
}

struct Prepare;

impl Node for Prepare {
    type Input = Batch;
    type Output = Batch;
    type Error = ExampleError;
    type State = ();

    fn init(&self) -> std::result::Result<Self::State, Self::Error> {
        Ok(())
    }

    fn process(
        &self,
        _state: &mut Self::State,
        input: Self::Input,
        ctx: &mut NodeContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error> {
        ctx.emit(
            input
                .into_iter()
                .map(|value| value.rotate_left(11) ^ 0xa076_1d64)
                .collect(),
        );
        Ok(())
    }
}

struct ManagedHash;

impl Node for ManagedHash {
    type Input = Batch;
    type Output = BatchLease;
    type Error = ExampleError;
    type State = ();

    fn init(&self) -> std::result::Result<Self::State, Self::Error> {
        Ok(())
    }

    fn process(
        &self,
        _state: &mut Self::State,
        input: Self::Input,
        ctx: &mut NodeContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error> {
        MANAGED_BATCHES.fetch_add(1, Ordering::Relaxed);
        let mut output = ctx.acquire_output();
        output.extend(hash_batch(&input, MANAGED_ROUNDS));
        ctx.emit(output);
        Ok(())
    }
}

struct Normalize;

impl Node for Normalize {
    type Input = BatchLease;
    type Output = Batch;
    type Error = ExampleError;
    type State = ();

    fn init(&self) -> std::result::Result<Self::State, Self::Error> {
        Ok(())
    }

    fn process(
        &self,
        _state: &mut Self::State,
        input: Self::Input,
        ctx: &mut NodeContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error> {
        ctx.emit(input.into_inner());
        Ok(())
    }
}

pipeline! {
    pub struct ExternalNodePipeline {
        type Input = Batch;
        type Output = Batch;
        type Error = ExampleError;

        config = config();
        nodes = {
            prepare = node("prepare", Prepare),
            managed_hash = anchor(ManagedHash)
                .max_threads(max_parallelism())
                .with_reusable_output(|| Vec::<u64>::with_capacity(BATCH_SIZE)),
            external_hash = external_node(Batch, BatchLease)
                .with_reusable_output(|| Vec::<u64>::with_capacity(BATCH_SIZE)),
            normalize = node("normalize", Normalize),
        };
        graph = {
            input -> prepare;
            prepare -> [managed_hash, external_hash];
            [managed_hash, external_hash] -> normalize;
            normalize -> output;
        };
    }
}

fn config() -> PiperConfig {
    PiperConfig {
        sample_interval: Duration::from_millis(5),
        poll_interval: Duration::from_millis(2),
        global_worker_cap: Some(max_parallelism().saturating_mul(2).max(4)),
        csv_telemetry: Some(
            TelemetryLogConfig::new(format!(
                "piper_external_node_{}.piper.csv",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
            ))
            .interval(CSV_TELEMETRY_INTERVAL),
        ),
    }
}

fn max_parallelism() -> usize {
    thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
        .max(1)
}

fn spawn_external_hash_workers(
    external_hash: piper::ExternalNode<Batch, BatchLease, ExampleError>,
) -> Vec<thread::JoinHandle<()>> {
    (0..EXTERNAL_WORKERS)
        .map(|_| {
            let external_hash = external_hash.clone();
            thread::spawn(move || {
                loop {
                    match external_hash.recv_timeout(Duration::from_millis(5)) {
                        Ok(batch) => {
                            EXTERNAL_BATCHES.fetch_add(1, Ordering::Relaxed);
                            let mut output = external_hash.acquire_output();
                            output.extend(hash_batch(&batch, EXTERNAL_ROUNDS));
                            if let Err(SendInputError) = external_hash.send(output) {
                                break;
                            }
                        }
                        Err(RecvOutputError::Timeout)
                            if external_hash.is_shutting_down() || external_hash.is_aborting() =>
                        {
                            break;
                        }
                        Err(RecvOutputError::Timeout) => continue,
                        Err(RecvOutputError::Closed) => break,
                        Err(RecvOutputError::TypeMismatch) => {
                            // User-owned workers can report fatal failures with fail(...).
                            external_hash.fail(ExampleError::ExternalWorker);
                            break;
                        }
                    }
                }
            })
        })
        .collect()
}

fn hash_batch(input: &[u64], rounds: usize) -> Batch {
    input
        .iter()
        .copied()
        .map(|value| {
            let mut acc = value;
            for round in 0..rounds {
                acc = acc
                    .wrapping_mul(0x9e37_79b1_85eb_ca87)
                    .rotate_left(((acc ^ round as u64) & 31) as u32)
                    ^ round as u64;
            }
            acc
        })
        .collect()
}

fn main() -> piper::Result<(), ExampleError> {
    let started = Instant::now();
    let run = ExternalNodePipeline::start()?;
    let sender = run.sender();
    let receiver = run.receiver();
    let workers = spawn_external_hash_workers(run.external_hash.clone());
    let received_batches = Arc::new(AtomicUsize::new(0));
    let received_counter = Arc::clone(&received_batches);

    let output_drainer = thread::spawn(move || {
        while received_counter.load(Ordering::Relaxed) < BATCH_COUNT {
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(batch) => {
                    drop(batch);
                    received_counter.fetch_add(1, Ordering::Relaxed);
                }
                Err(RecvOutputError::Timeout) => continue,
                Err(RecvOutputError::Closed) => break,
                Err(RecvOutputError::TypeMismatch) => {
                    panic!("pipeline output type mismatch");
                }
            }
        }
    });

    let producer = thread::spawn(move || {
        for batch_index in 0..BATCH_COUNT {
            let start = (batch_index * BATCH_SIZE) as u64;
            let batch: Vec<_> = (0..BATCH_SIZE)
                .map(|offset| start + offset as u64)
                .collect();
            sender.send(batch).expect("pipeline input is open");
        }
    });

    producer.join().expect("producer thread should not panic");
    output_drainer
        .join()
        .expect("output drainer thread should not panic");
    run.shutdown();

    for worker in workers {
        worker
            .join()
            .expect("external worker thread should not panic");
    }

    let telemetry = run.get_telemetry();
    let external_node_snapshot = telemetry
        .nodes
        .iter()
        .find(|node| node.name == "external_hash");
    let (external_input_rate, external_output_rate) = external_node_snapshot
        .map(|node| (node.external_input_rate, node.external_output_rate))
        .unwrap_or_default();

    run.join()?;

    let managed = MANAGED_BATCHES.load(Ordering::Relaxed);
    let external = EXTERNAL_BATCHES.load(Ordering::Relaxed);
    let received = received_batches.load(Ordering::Relaxed);
    println!("managed_hash batches: {managed}");
    println!("external_hash batches: {external}");
    println!("outputs received: {received}");
    println!("external input rate: {external_input_rate:.2} batches/s");
    println!("external output rate: {external_output_rate:.2} batches/s");
    println!("anchors observed: {}", telemetry.anchors.len());
    println!("elapsed: {:.2?}", started.elapsed());

    Ok(())
}

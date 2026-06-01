use piper::{
    BufferLease, FeederLinkConfig, Node, NodeContext, NodeExt, PipelineGraphBuilder, Piper,
    PiperConfig, TelemetryLogConfig, anchor, node,
};
use std::io::{self, Write};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const BATCH_COUNT: usize = 15_000;
const BATCH_SIZE: usize = 2_048;
const COMPUTE_ROUNDS: usize = 12_288;
const FIXED_ROUNDS: usize = 3_072;
const TELEMETRY_INTERVAL: Duration = Duration::from_millis(100);
const MANAGER_SAMPLE_INTERVAL: Duration = Duration::from_millis(10);
const CSV_TELEMETRY_INTERVAL: Duration = MANAGER_SAMPLE_INTERVAL;
const OUTPUT_DRAINER_POLL: Duration = Duration::from_millis(5);

static COMPUTE_BATCHES: AtomicUsize = AtomicUsize::new(0);
static FIXED_BATCHES: AtomicUsize = AtomicUsize::new(0);

type Batch = Vec<u64>;
type BatchLease = BufferLease<Vec<u64>>;

#[derive(Debug, Error)]
enum ExampleError {}

struct Prepare;

impl Node for Prepare {
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
        let mut output = ctx.acquire_output();
        output.extend(
            input
                .into_iter()
                .map(|value| value.wrapping_mul(31).rotate_left(7) ^ 0x9e37_79b9_7f4a_7c15),
        );
        ctx.emit(output);
        Ok(())
    }
}

struct Compute;

impl Node for Compute {
    type Input = BatchLease;
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
        COMPUTE_BATCHES.fetch_add(1, Ordering::Relaxed);
        let mut output = ctx.acquire_output();
        compute_batch(&input, &mut output, COMPUTE_ROUNDS);
        ctx.emit(output);
        Ok(())
    }
}

struct FixedCompute;

impl Node for FixedCompute {
    type Input = BatchLease;
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
        FIXED_BATCHES.fetch_add(1, Ordering::Relaxed);
        let mut output = ctx.acquire_output();
        compute_batch(&input, &mut output, FIXED_ROUNDS);
        ctx.emit(output);
        Ok(())
    }
}

struct Emit;

impl Node for Emit {
    type Input = BatchLease;
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
        ctx.emit(input);
        Ok(())
    }
}

fn compute_batch(input: &[u64], output: &mut Vec<u64>, rounds: usize) {
    for &value in input {
        let mut acc = value;
        for round in 0..rounds {
            acc = acc
                .wrapping_mul(0x9e37_79b1_85eb_ca87)
                .rotate_left(((acc ^ round as u64) & 31) as u32)
                ^ (round as u64).wrapping_mul(0xc2b2_ae3d_27d4_eb4f);
        }
        output.push(acc);
    }
}

fn build_graph() -> piper::PipelineGraph<Batch, BatchLease, ExampleError> {
    let mut builder = PipelineGraphBuilder::<Batch, ExampleError>::new();
    let input = builder.input();
    let fork = builder.link();
    let joined = builder.link();
    let prepare = Prepare.with_reusable_output(|| Vec::<u64>::with_capacity(BATCH_SIZE));
    let compute = anchor(Compute)
        .max_threads(max_parallelism())
        .initial_threads(max_parallelism().div_ceil(2).max(1))
        .with_reusable_output(|| Vec::<u64>::with_capacity(BATCH_SIZE));
    let fixed_compute = anchor(FixedCompute)
        .fixed_threads(2)
        .with_reusable_output(|| Vec::<u64>::with_capacity(BATCH_SIZE));
    let emit = node("emit", Emit);
    builder.add_node_to(input, prepare, fork);
    builder.add_node_to(fork, compute, joined);
    builder.add_node_to(fork, fixed_compute, joined);
    let out = builder.add_node(joined, emit);
    builder.feeder_link(fork, FeederLinkConfig::default());
    builder.finish(out)
}

fn max_parallelism() -> usize {
    thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
        .max(1)
}

fn output_drainers() -> usize {
    max_parallelism().div_ceil(4).max(2)
}

fn config() -> PiperConfig {
    PiperConfig {
        sample_interval: MANAGER_SAMPLE_INTERVAL,
        poll_interval: Duration::from_millis(5),
        global_worker_cap: None,
        csv_telemetry: Some(
            TelemetryLogConfig::new(format!(
                "piper_scaling_pipeline_feeder_{}.piper.csv",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
            ))
            .interval(CSV_TELEMETRY_INTERVAL),
        ),
    }
}

fn main() -> piper::Result<(), ExampleError> {
    let graph = build_graph();
    let piper = Piper::start(config(), graph)?;
    let sender = piper.sender();
    let receiver = piper.receiver();
    let received_batches = Arc::new(AtomicUsize::new(0));
    let output_drainers = spawn_output_drainers(receiver, Arc::clone(&received_batches));

    let producer = thread::spawn(move || {
        for batch_index in 0..BATCH_COUNT {
            let start = (batch_index * BATCH_SIZE) as u64;
            let batch: Vec<_> = (0..BATCH_SIZE)
                .map(|offset| start + offset as u64)
                .collect();
            sender.send(batch).expect("pipeline input is open");
        }
    });

    let mut producer = Some(producer);
    let mut producer_joined = false;
    let mut next_telemetry = Instant::now();

    while received_batches.load(Ordering::Relaxed) < BATCH_COUNT {
        if !producer_joined && producer.as_ref().is_some_and(|handle| handle.is_finished()) {
            producer
                .take()
                .expect("producer exists")
                .join()
                .expect("producer thread should not panic");
            producer_joined = true;
        }

        if next_telemetry.elapsed() >= TELEMETRY_INTERVAL {
            print_progress(received_batches.load(Ordering::Relaxed));
            next_telemetry = Instant::now();
        }

        thread::sleep(Duration::from_millis(10));
    }

    print_progress(received_batches.load(Ordering::Relaxed));
    println!();

    if let Some(producer) = producer {
        producer.join().expect("producer thread should not panic");
    }

    for drainer in output_drainers {
        drainer.join().expect("output drainer should not panic");
    }

    let received = received_batches.load(Ordering::Relaxed);
    piper.shutdown();

    let telemetry = piper.get_telemetry();
    piper.join()?;

    let compute = COMPUTE_BATCHES.load(Ordering::Relaxed);
    let fixed = FIXED_BATCHES.load(Ordering::Relaxed);
    println!("compute batches: {compute}");
    println!("fixed_compute batches: {fixed}");
    println!("total branch batches: {}", compute + fixed);
    println!("outputs received: {received}");
    println!("anchors observed: {}", telemetry.anchors.len());

    Ok(())
}

fn spawn_output_drainers(
    receiver: piper::PiperReceiver<BatchLease>,
    received_batches: Arc<AtomicUsize>,
) -> Vec<thread::JoinHandle<()>> {
    (0..output_drainers())
        .map(|_| {
            let receiver = receiver.clone();
            let received_batches = Arc::clone(&received_batches);
            thread::spawn(move || {
                while received_batches.load(Ordering::Relaxed) < BATCH_COUNT {
                    match receiver.recv_timeout(OUTPUT_DRAINER_POLL) {
                        Ok(batch) => {
                            drop(batch);
                            received_batches.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(piper::RecvOutputError::Timeout) => {}
                        Err(piper::RecvOutputError::Closed) => break,
                        Err(piper::RecvOutputError::TypeMismatch) => {
                            panic!("pipeline output type mismatch");
                        }
                    }
                }
            })
        })
        .collect()
}

fn print_progress(received_batches: usize) {
    let progress = (received_batches as f64 / BATCH_COUNT as f64) * 100.0;
    print!("\r{:>6.2}%", progress);
    io::stdout().flush().expect("flush progress output");
}

use piper::{
    BufferLease, Node, NodeContext, NodeExt, PiperConfig, TelemetryLogConfig, anchor, node,
    pipeline,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const BATCH_COUNT: usize = 16_384;
const BATCH_SIZE: usize = 512;
const HEAVY_ROUNDS: usize = 9_216;
const FIXED_ROUNDS: usize = 2_304;
const CSV_TELEMETRY_INTERVAL: Duration = Duration::from_millis(50);

static HEAVY_BATCHES: AtomicUsize = AtomicUsize::new(0);
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
                .map(|value| value.rotate_left(11) ^ 0xa076_1d64),
        );
        ctx.emit(output);
        Ok(())
    }
}

struct HeavyHash;

impl Node for HeavyHash {
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
        HEAVY_BATCHES.fetch_add(1, Ordering::Relaxed);
        let mut output = ctx.acquire_output();
        hash_batch(&input, &mut output, HEAVY_ROUNDS);
        ctx.emit(output);
        Ok(())
    }
}

struct FixedHash;

impl Node for FixedHash {
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
        hash_batch(&input, &mut output, FIXED_ROUNDS);
        ctx.emit(output);
        Ok(())
    }
}

struct Normalize;

impl Node for Normalize {
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

pipeline! {
    pub struct ForkJoinPipeline {
        type Input = Batch;
        type Output = BatchLease;
        type Error = ExampleError;

        config = config();
        nodes = {
            prepare = Prepare.with_reusable_output(|| Vec::<u64>::with_capacity(BATCH_SIZE)),
            heavy_hash = anchor(HeavyHash)
                .max_threads(max_parallelism())
                .with_reusable_output(|| Vec::<u64>::with_capacity(BATCH_SIZE)),
            fixed_hash = anchor(FixedHash)
                .fixed_threads(2)
                .with_reusable_output(|| Vec::<u64>::with_capacity(BATCH_SIZE)),
            normalize = node("normalize", Normalize),
        };
        graph = {
            input -> prepare;
            prepare -> [heavy_hash, fixed_hash];
            [heavy_hash, fixed_hash] -> normalize;
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
                "piper_fork_join_{}.piper.csv",
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

fn output_drainers() -> usize {
    max_parallelism().saturating_mul(2).max(4)
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
                    match receiver.try_recv() {
                        Ok(batch) => {
                            drop(batch);
                            received_batches.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(piper::TryRecvOutputError::Empty) => {
                            std::hint::spin_loop();
                        }
                        Err(piper::TryRecvOutputError::Closed) => break,
                        Err(piper::TryRecvOutputError::TypeMismatch) => {
                            panic!("pipeline output type mismatch");
                        }
                    }
                }
            })
        })
        .collect()
}

fn hash_batch(input: &[u64], output: &mut Vec<u64>, rounds: usize) {
    output.extend(input.iter().copied().map(|value| {
        let mut acc = value;
        for round in 0..rounds {
            acc = acc
                .wrapping_mul(0x9e37_79b1_85eb_ca87)
                .rotate_left(((acc ^ round as u64) & 31) as u32)
                ^ round as u64;
        }
        acc
    }));
}

fn main() -> piper::Result<(), ExampleError> {
    let piper = ForkJoinPipeline::start()?;
    let sender = piper.sender();
    let receiver = piper.receiver();
    let received_batches = Arc::new(AtomicUsize::new(0));
    let output_drainers = spawn_output_drainers(receiver, Arc::clone(&received_batches));
    thread::yield_now();

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
    for drainer in output_drainers {
        drainer
            .join()
            .expect("output drainer thread should not panic");
    }
    let received = received_batches.load(Ordering::Relaxed);
    piper.shutdown();

    let telemetry = piper.get_telemetry();
    piper.join()?;

    let heavy = HEAVY_BATCHES.load(Ordering::Relaxed);
    let fixed = FIXED_BATCHES.load(Ordering::Relaxed);
    println!("heavy_hash batches: {heavy}");
    println!("fixed_hash batches: {fixed}");
    println!("total branch batches: {}", heavy + fixed);
    println!("outputs received: {received}");
    println!("anchors observed: {}", telemetry.anchors.len());

    Ok(())
}

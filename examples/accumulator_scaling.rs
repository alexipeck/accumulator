use piper::{
    Node, NodeContext, NodeScalePolicy, PiperConfig, PiperError, PiperWithState, anchor,
    node_with_state_merge, pipeline,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use thiserror::Error;

const KEY_COUNT: usize = 16;
const BATCH_COUNT: usize = 1_600;
const BATCH_SIZE: usize = 256;
const FIRST_BURST_BATCHES: usize = 400;
const PAUSE_AFTER_FIRST_BURST: Duration = Duration::from_secs(2);
#[cfg(not(test))]
const PROCESS_DELAY: Duration = Duration::from_millis(8);
#[cfg(test)]
const PROCESS_DELAY: Duration = Duration::from_millis(1);
const MANAGER_SAMPLE_INTERVAL: Duration = Duration::from_millis(5);
const TELEMETRY_POLL_INTERVAL: Duration = Duration::from_millis(5);

static MERGE_CALLS: AtomicUsize = AtomicUsize::new(0);

type Batch = Vec<Sample>;
type AccumulatorState = Vec<KahanState>;
type AccumulatorRun = PiperWithState<Batch, usize, AccumulatorState, ExampleError, true>;

#[derive(Debug, Error)]
enum ExampleError {
    #[error("state length mismatch: target={target}, source={source_len}")]
    StateLengthMismatch {
        target: usize,
        source_len: usize,
    },
    #[error("pipeline output closed while draining")]
    OutputClosed,
}

#[derive(Clone, Copy, Debug)]
struct Sample {
    key: usize,
    value: f64,
}

#[derive(Clone, Debug, Default)]
struct KahanState {
    sum: f64,
    compensation: f64,
    count: u64,
}

impl KahanState {
    fn add_value(&mut self, value: f64) {
        self.add_compensated(value);
        self.count += 1;
    }

    fn merge_from(&mut self, source: KahanState) {
        self.add_compensated(source.sum - source.compensation);
        self.count += source.count;
    }

    fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.corrected_sum() / self.count as f64
        }
    }

    fn corrected_sum(&self) -> f64 {
        self.sum - self.compensation
    }

    fn add_compensated(&mut self, value: f64) {
        let y = value - self.compensation;
        let t = self.sum + y;
        self.compensation = (t - self.sum) - y;
        self.sum = t;
    }
}

struct Accumulate;

impl Node for Accumulate {
    type Input = Batch;
    type Output = usize;
    type Error = ExampleError;
    type State = AccumulatorState;

    fn init(&self) -> std::result::Result<Self::State, Self::Error> {
        Ok(empty_state())
    }

    fn process(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        ctx: &mut NodeContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error> {
        let batch_len = input.len();
        for sample in input {
            apply_sample(state, sample);
        }
        thread::sleep(PROCESS_DELAY);
        ctx.emit(batch_len);
        Ok(())
    }
}

pipeline! {
    struct AccumulatorScalingPipeline {
        type Input = Batch;
        type Output = usize;
        type Error = ExampleError;

        config = config();
        nodes = [
            anchor(node_with_state_merge("accumulate", Accumulate, merge_accumulator))
                .with_scale_policy(scale_policy()),
        ];
        return_state = AccumulatorState;
    }
}

#[derive(Clone, Copy)]
struct Workload {
    batches: usize,
    batch_size: usize,
    first_burst_batches: usize,
    pause_after_first_burst: Duration,
}

impl Workload {
    fn demo() -> Self {
        Self {
            batches: BATCH_COUNT,
            batch_size: BATCH_SIZE,
            first_burst_batches: FIRST_BURST_BATCHES,
            pause_after_first_burst: PAUSE_AFTER_FIRST_BURST,
        }
    }

    #[cfg(test)]
    fn total_samples(self) -> usize {
        self.batches * self.batch_size
    }
}

struct RunSummary {
    final_state: AccumulatorState,
    drained_samples: usize,
    elapsed: Duration,
    max_active_threads: usize,
    merge_calls_before_join: usize,
    total_merge_calls: usize,
}

fn main() -> piper::Result<(), ExampleError> {
    let summary = run_workload(Workload::demo())?;
    let final_samples = state_sample_count(&summary.final_state);
    let checksum = state_checksum(&summary.final_state);
    let mean_checksum = state_mean_checksum(&summary.final_state);

    println!("merge-aware accumulator scaling example");
    println!("  drained samples          : {}", summary.drained_samples);
    println!("  returned-state samples   : {final_samples}");
    println!("  elapsed                  : {:?}", summary.elapsed);
    println!("  sum checksum             : {checksum:.12}");
    println!("  mean checksum            : {mean_checksum:.12}");
    println!("  max active workers       : {}", summary.max_active_threads);
    println!(
        "  merge calls before join  : {}",
        summary.merge_calls_before_join
    );
    println!("  total merge calls        : {}", summary.total_merge_calls);
    Ok(())
}

fn config() -> PiperConfig {
    PiperConfig {
        sample_interval: MANAGER_SAMPLE_INTERVAL,
        poll_interval: Duration::from_millis(5),
        global_worker_cap: None,
        csv_telemetry: None,
    }
}

fn scale_policy() -> NodeScalePolicy {
    NodeScalePolicy {
        initial_threads: 1,
        max_threads: max_parallelism().min(4).max(2),
        target_queue_seconds: 0.001,
        low_queue_seconds: 0.001,
        scale_down_after: Duration::from_millis(50),
        underutilized_busy_ratio: 0.95,
    }
}

fn max_parallelism() -> usize {
    thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
        .max(1)
}

fn run_workload(workload: Workload) -> piper::Result<RunSummary, ExampleError> {
    MERGE_CALLS.store(0, Ordering::Relaxed);
    let started = Instant::now();

    let piper = AccumulatorScalingPipeline::start()?;
    let sender = piper.sender();
    let receiver = piper.receiver();
    let expected_batches = workload.batches;
    let drainer = thread::spawn(move || -> std::result::Result<usize, ExampleError> {
        let mut drained = 0usize;
        for _ in 0..expected_batches {
            drained += receiver.recv().map_err(|_| ExampleError::OutputClosed)?;
        }
        Ok(drained)
    });

    let mut max_active_threads = active_accumulator_threads(&piper);
    let first_burst = workload.first_burst_batches.min(workload.batches);
    send_batch_range(&sender, 0, first_burst, workload.batch_size)?;

    let pause_deadline = Instant::now() + workload.pause_after_first_burst;
    while Instant::now() < pause_deadline {
        max_active_threads = max_active_threads.max(active_accumulator_threads(&piper));
        thread::sleep(TELEMETRY_POLL_INTERVAL);
    }

    for batch_index in first_burst..workload.batches {
        send_batch(&sender, batch_index, workload.batch_size)?;
        max_active_threads = max_active_threads.max(active_accumulator_threads(&piper));
    }

    while !drainer.is_finished() {
        max_active_threads = max_active_threads.max(active_accumulator_threads(&piper));
        thread::sleep(TELEMETRY_POLL_INTERVAL);
    }

    let drained_samples = join_drainer(drainer)?;
    let merge_calls_before_join = MERGE_CALLS.load(Ordering::Relaxed);
    piper.shutdown();
    let final_state = piper.join_merged()?;
    let total_merge_calls = MERGE_CALLS.load(Ordering::Relaxed);

    Ok(RunSummary {
        final_state,
        drained_samples,
        elapsed: started.elapsed(),
        max_active_threads,
        merge_calls_before_join,
        total_merge_calls,
    })
}

fn send_batch_range(
    sender: &piper::PiperSender<Batch>,
    start: usize,
    end: usize,
    batch_size: usize,
) -> piper::Result<(), ExampleError> {
    for batch_index in start..end {
        send_batch(sender, batch_index, batch_size)?;
    }
    Ok(())
}

fn send_batch(
    sender: &piper::PiperSender<Batch>,
    batch_index: usize,
    batch_size: usize,
) -> piper::Result<(), ExampleError> {
    sender
        .send(make_batch(batch_index, batch_size))
        .map_err(|error| PiperError::Internal {
            worker: "producer".to_string(),
            message: error.to_string(),
        })
}

fn join_drainer(
    drainer: thread::JoinHandle<std::result::Result<usize, ExampleError>>,
) -> piper::Result<usize, ExampleError> {
    let result = drainer.join().map_err(|payload| PiperError::WorkerPanicked {
        worker: "output-drainer".to_string(),
        message: piper::panic_payload_to_string(payload),
    })?;
    result.map_err(|error| PiperError::Internal {
        worker: "output-drainer".to_string(),
        message: error.to_string(),
    })
}

fn active_accumulator_threads(piper: &AccumulatorRun) -> usize {
    piper
        .get_telemetry()
        .nodes
        .into_iter()
        .find(|node| node.name == "accumulate")
        .map(|node| node.active_threads)
        .unwrap_or(0)
}

fn make_batch(batch_index: usize, batch_size: usize) -> Batch {
    let start = batch_index * batch_size;
    (start..start + batch_size).map(sample_at).collect()
}

fn sample_at(index: usize) -> Sample {
    let key = (index * 37 + 11) % KEY_COUNT;
    let mixed = (index as u64)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .rotate_left((index % 29) as u32)
        ^ 0xa076_1d64_78bd_642f;
    let value = (mixed % 20_001) as f64 / 97.0 - 103.0;
    Sample { key, value }
}

fn empty_state() -> AccumulatorState {
    vec![KahanState::default(); KEY_COUNT]
}

#[cfg(test)]
fn expected_state(workload: Workload) -> AccumulatorState {
    let mut state = empty_state();
    for batch_index in 0..workload.batches {
        for sample in make_batch(batch_index, workload.batch_size) {
            apply_sample(&mut state, sample);
        }
    }
    state
}

fn apply_sample(state: &mut AccumulatorState, sample: Sample) {
    state[sample.key].add_value(sample.value);
}

fn merge_accumulator(
    target: &mut AccumulatorState,
    source: AccumulatorState,
) -> std::result::Result<(), ExampleError> {
    MERGE_CALLS.fetch_add(1, Ordering::Relaxed);
    merge_state_into(target, source)
}

fn merge_state_into(
    target: &mut AccumulatorState,
    source: AccumulatorState,
) -> std::result::Result<(), ExampleError> {
    if target.len() != source.len() {
        return Err(ExampleError::StateLengthMismatch {
            target: target.len(),
            source_len: source.len(),
        });
    }

    for (target_key, source_key) in target.iter_mut().zip(source) {
        target_key.merge_from(source_key);
    }
    Ok(())
}

fn state_sample_count(state: &AccumulatorState) -> u64 {
    state.iter().map(|key| key.count).sum()
}

fn state_checksum(state: &AccumulatorState) -> f64 {
    state.iter().map(KahanState::corrected_sum).sum()
}

fn state_mean_checksum(state: &AccumulatorState) -> f64 {
    state.iter().map(KahanState::mean).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_merged_final_state() {
        let workload = Workload {
            batches: 24,
            batch_size: 32,
            first_burst_batches: 8,
            pause_after_first_burst: Duration::from_millis(20),
        };
        let summary = run_workload(workload).unwrap();
        let expected = expected_state(workload);

        assert_eq!(summary.drained_samples, workload.total_samples());
        assert_eq!(
            state_sample_count(&summary.final_state),
            workload.total_samples() as u64
        );
        assert_state_close(&summary.final_state, &expected);
    }

    #[test]
    fn merge_helper_combines_disjoint_partial_states() {
        let mut left = empty_state();
        let mut right = empty_state();
        let mut expected = empty_state();

        for index in 0..128 {
            apply_sample(&mut left, sample_at(index));
            apply_sample(&mut expected, sample_at(index));
        }
        for index in 128..256 {
            apply_sample(&mut right, sample_at(index));
            apply_sample(&mut expected, sample_at(index));
        }

        merge_state_into(&mut left, right).unwrap();
        assert_state_close(&left, &expected);
    }

    fn assert_state_close(actual: &AccumulatorState, expected: &AccumulatorState) {
        assert_eq!(actual.len(), expected.len());
        for (index, (actual_key, expected_key)) in actual.iter().zip(expected).enumerate() {
            assert_eq!(
                actual_key.count, expected_key.count,
                "count mismatch for key {index}"
            );
            let delta = (actual_key.corrected_sum() - expected_key.corrected_sum()).abs();
            assert!(
                delta <= 1.0e-8,
                "sum mismatch for key {index}: actual={}, expected={}, delta={delta}",
                actual_key.corrected_sum(),
                expected_key.corrected_sum()
            );
        }
    }
}

use piper::{
    BufferLease, IntoNodeSpec, NodeThreadPolicyKind, PipelineGraph, PipelineGraphBuilder, Piper,
    PiperConfig, NodeScalePolicy, PiperError, SingleThreadWeightedBranchConfig, Node, NodeContext,
    TelemetryLogConfig, anchor, inline_node, node, node_with_state_merge, panic_payload_to_string,
    pipeline,
};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;

#[derive(Debug, Error)]
enum TestError {
    #[error("test error")]
    Test,
}

fn config() -> PiperConfig {
    PiperConfig {
        sample_interval: Duration::from_millis(1),
        poll_interval: Duration::from_millis(1),
        global_worker_cap: Some(8),
        csv_telemetry: None,
    }
}

struct Double;

impl Node for Double {
    type Input = u32;
    type Output = u32;
    type Error = TestError;
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
        ctx.emit(input * 2);
        Ok(())
    }
}

struct FormatValue;

impl Node for FormatValue {
    type Input = u32;
    type Output = String;
    type Error = TestError;
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
        ctx.emit(format!("value={input}"));
        Ok(())
    }
}

struct Pass;

impl Node for Pass {
    type Input = u32;
    type Output = u32;
    type Error = TestError;
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
    struct ExternalJoinPipeline {
        type Input = u32;
        type Output = u32;
        type Error = TestError;

        config = config();
        nodes = {
            external = external_node(u32, u32),
            pass = node("pass", Pass),
        };
        graph = {
            input -> external;
            external -> pass;
            pass -> output;
        };
    }
}

struct CountPass {
    count: Arc<AtomicUsize>,
}

impl Node for CountPass {
    type Input = u32;
    type Output = u32;
    type Error = TestError;
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
        self.count.fetch_add(1, Ordering::Relaxed);
        ctx.emit(input);
        Ok(())
    }
}

fn one_stage_graph<S>(stage_like: S) -> PipelineGraph<u32, u32, TestError>
where
    S: IntoNodeSpec<TestError, Input = u32, Output = u32>,
{
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let output = builder.add_node(input, stage_like);
    builder.finish(output)
}

struct CountingFinal;

impl Node for CountingFinal {
    type Input = u32;
    type Output = u32;
    type Error = TestError;
    type State = usize;

    fn init(&self) -> std::result::Result<Self::State, Self::Error> {
        Ok(0)
    }

    fn process(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        ctx: &mut NodeContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error> {
        *state += 1;
        ctx.emit(input);
        Ok(())
    }
}

fn merge_usize(target: &mut usize, source: usize) -> std::result::Result<(), TestError> {
    *target += source;
    Ok(())
}

struct SlowCountingFinal {
    processed: Arc<AtomicUsize>,
}

impl Node for SlowCountingFinal {
    type Input = u32;
    type Output = u32;
    type Error = TestError;
    type State = usize;

    fn init(&self) -> std::result::Result<Self::State, Self::Error> {
        Ok(0)
    }

    fn process(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        ctx: &mut NodeContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error> {
        std::thread::sleep(Duration::from_millis(2));
        *state += 1;
        self.processed.fetch_add(1, Ordering::Relaxed);
        ctx.emit(input);
        Ok(())
    }
}

fn fast_scale_policy() -> NodeScalePolicy {
    NodeScalePolicy {
        initial_threads: 1,
        max_threads: 4,
        target_queue_seconds: 0.001,
        low_queue_seconds: 0.001,
        scale_down_after: Duration::from_millis(10),
        underutilized_busy_ratio: 0.95,
    }
}

#[test]
fn state_returning_final_node_join_returns_worker_states() {
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let output = builder.add_node(input, node("count", CountingFinal).fixed_threads(2));
    let piper = piper::PiperWithState::<u32, u32, usize, TestError>::start(
        config(),
        builder.finish_with_state::<u32, usize>(output),
    )
    .unwrap();

    for value in 0..10 {
        piper.sender().send(value).unwrap();
    }
    piper.shutdown();
    for _ in 0..10 {
        piper.receiver().recv_timeout(Duration::from_secs(1)).unwrap();
    }

    let states = piper.join().unwrap();
    assert_eq!(states.iter().sum::<usize>(), 10);
    assert_eq!(states.len(), 2);
}

#[test]
fn state_returning_mergeable_final_node_join_merged_returns_one_state() {
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let output = builder.add_node(
        input,
        node_with_state_merge("count", CountingFinal, merge_usize).fixed_threads(2),
    );
    let piper = piper::PiperWithState::<u32, u32, usize, TestError, true>::start(
        config(),
        builder.finish_with_merged_state::<u32, usize>(output),
    )
    .unwrap();

    for value in 0..12 {
        piper.sender().send(value).unwrap();
    }
    piper.shutdown();
    for _ in 0..12 {
        piper.receiver().recv_timeout(Duration::from_secs(1)).unwrap();
    }

    assert_eq!(piper.join_merged().unwrap(), 12);
}

#[test]
fn merge_scale_down_preserves_state_and_runs_on_target_worker() {
    let processed = Arc::new(AtomicUsize::new(0));
    let merges = Arc::new(AtomicUsize::new(0));
    let merges_for_closure = Arc::clone(&merges);
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let output = builder.add_node(
        input,
        node_with_state_merge(
            "count",
            SlowCountingFinal {
                processed: Arc::clone(&processed),
            },
            move |target: &mut usize, source: usize| {
                merges_for_closure.fetch_add(1, Ordering::Relaxed);
                *target += source;
                Ok(())
            },
        )
        .with_scale_policy(fast_scale_policy()),
    );
    let piper = piper::PiperWithState::<u32, u32, usize, TestError, true>::start(
        config(),
        builder.finish_with_merged_state::<u32, usize>(output),
    )
    .unwrap();
    let receiver = piper.receiver();
    let received = Arc::new(AtomicUsize::new(0));
    let received_for_thread = Arc::clone(&received);
    let drainer = std::thread::spawn(move || {
        while received_for_thread.load(Ordering::Relaxed) < 160 {
            if receiver.recv_timeout(Duration::from_secs(1)).is_ok() {
                received_for_thread.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    for value in 0..160 {
        piper.sender().send(value).unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_scale_up = false;
    while Instant::now() < deadline {
        let active = piper.get_telemetry().nodes[0].active_threads;
        saw_scale_up |= active > 1;
        if saw_scale_up && received.load(Ordering::Relaxed) == 160 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(saw_scale_up, "node did not scale up before workload completed");

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && merges.load(Ordering::Relaxed) == 0 {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        merges.load(Ordering::Relaxed) > 0,
        "node did not perform a merge-down"
    );

    piper.shutdown();
    drainer.join().unwrap();
    assert_eq!(piper.join_merged().unwrap(), 160);
    assert_eq!(processed.load(Ordering::Relaxed), 160);
}

#[test]
fn associated_type_stages_stream_outputs_and_default_cleanup_is_optional() {
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let doubled = builder.add_node(input, anchor(node("double", Double)).max_threads(1));
    let output = builder.add_node(doubled, node("format", FormatValue));
    let piper = Piper::<u32, String, TestError>::start(config(), builder.finish(output)).unwrap();

    let sender = piper.sender();
    let receiver = piper.receiver();
    for value in 1..=3 {
        sender.send(value).unwrap();
    }

    piper.shutdown();

    let mut outputs = Vec::new();
    for _ in 0..3 {
        outputs.push(receiver.recv_timeout(Duration::from_secs(1)).unwrap());
    }

    piper.join().unwrap();
    outputs.sort();
    assert_eq!(outputs, ["value=2", "value=4", "value=6"]);
}

#[test]
fn get_telemetry_reports_operational_state() {
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let left = builder.add_node(input, node("left", Pass));
    let output = builder.add_node(left, anchor(node("right", Pass)).max_threads(1));
    let piper = Piper::<u32, u32, TestError>::start(config(), builder.finish(output)).unwrap();

    std::thread::sleep(Duration::from_millis(20));
    let telemetry = piper.get_telemetry();

    assert_eq!(telemetry.links.len(), 3);
    assert_eq!(telemetry.nodes.len(), 2);
    assert_eq!(telemetry.nodes[0].active_threads, 1);
    assert_eq!(telemetry.nodes[1].active_threads, 1);
    assert_eq!(telemetry.anchors.len(), 1);
    assert_eq!(telemetry.anchors[0].node_index, 1);
    assert_eq!(telemetry.nodes[1].max_thread_count, Some(1));
    assert_eq!(telemetry.global_worker_cap, 8);
    assert_eq!(telemetry.total_active_workers, 2);
    assert!(telemetry.nodes.iter().any(|stage| stage.is_anchor));
    assert!(telemetry.parked_threads >= 2);

    piper.abort();
    piper.join().unwrap();
}

#[test]
fn fixed_anchor_does_not_reserve_parked_worker() {
    let piper = Piper::<u32, u32, TestError>::start(
        config(),
        one_stage_graph(anchor(node("fixed", Pass)).fixed_threads(1)),
    )
    .unwrap();

    std::thread::sleep(Duration::from_millis(20));
    let telemetry = piper.get_telemetry();

    assert_eq!(telemetry.nodes.len(), 1);
    assert_eq!(telemetry.nodes[0].active_threads, 1);
    assert_eq!(
        telemetry.nodes[0].thread_policy_kind,
        NodeThreadPolicyKind::Fixed
    );
    assert!(telemetry.nodes[0].is_anchor);
    assert_eq!(telemetry.parked_threads, 0);

    piper.abort();
    piper.join().unwrap();
}

#[test]
fn abort_skips_inline_builder_cleanup() {
    let cleaned = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cleaned_for_stage = std::sync::Arc::clone(&cleaned);

    let piper = Piper::<u32, u32, TestError>::start(
        config(),
        one_stage_graph(
            anchor(
                inline_node(
                    "cleanup",
                    || -> std::result::Result<(), TestError> { Ok(()) },
                    |_state: &mut (), input: u32, ctx: &mut NodeContext<u32, TestError>| {
                        ctx.emit(input);
                        Ok(())
                    },
                )
                .with_cleanup(move |_state| {
                    cleaned_for_stage.store(true, std::sync::atomic::Ordering::Release);
                    Ok(())
                }),
            )
            .max_threads(1),
        ),
    )
    .unwrap();

    piper.abort();
    piper.join().unwrap();

    assert!(!cleaned.load(std::sync::atomic::Ordering::Acquire));
}

#[test]
fn user_process_failure_fails_pipeline() {
    let piper = Piper::<u32, u32, TestError>::start(
        config(),
        one_stage_graph(anchor(node("fail", Fail)).max_threads(1)),
    )
    .unwrap();

    piper.sender().send(1).unwrap();
    let error = piper.join().expect_err("process error should fail join");
    assert!(matches!(error, PiperError::UserProcess { .. }));
}

#[test]
fn fork_join_graph_work_shares_and_merges_outputs() {
    let left = Arc::new(AtomicUsize::new(0));
    let right = Arc::new(AtomicUsize::new(0));
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let fork = builder.add_node(input, node("prepare", Pass));
    let merged = builder.link();
    builder.add_node_to(
        fork,
        node(
            "left",
            CountPass {
                count: Arc::clone(&left),
            },
        ),
        merged,
    );
    builder.add_node_to(
        fork,
        anchor(node(
            "right",
            CountPass {
                count: Arc::clone(&right),
            },
        ))
        .fixed_threads(1),
        merged,
    );
    let piper = Piper::<u32, u32, TestError>::start(config(), builder.finish(merged)).unwrap();

    for value in 0..100 {
        piper.sender().send(value).unwrap();
    }
    piper.shutdown();
    for _ in 0..100 {
        piper
            .receiver()
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
    }

    assert_eq!(
        left.load(Ordering::Relaxed) + right.load(Ordering::Relaxed),
        100
    );
    assert_eq!(piper.get_telemetry().anchors.len(), 1);
    assert_eq!(
        piper.get_telemetry().nodes.iter().find(|n| n.is_anchor).unwrap().fixed_thread_count,
        Some(1)
    );
    piper.join().unwrap();
}

#[test]
fn external_node_bridges_user_loop_into_managed_node() {
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let external_output = builder.link::<u32>();
    let external_token =
        builder.add_external_node_to::<u32, u32>(input, "external", external_output);
    let output = builder.add_node(external_output, node("pass", Pass));
    let mut piper = Piper::<u32, u32, TestError>::start(config(), builder.finish(output)).unwrap();
    let external = piper.take_external_node(external_token);

    let worker = std::thread::spawn(move || {
        loop {
            match external.recv_timeout(Duration::from_millis(5)) {
                Ok(value) => external.send(value + 1).unwrap(),
                Err(piper::RecvOutputError::Timeout) if external.is_shutting_down() => break,
                Err(piper::RecvOutputError::Timeout) => continue,
                Err(piper::RecvOutputError::Closed) => break,
                Err(error) => panic!("external recv failed: {error}"),
            }
        }
    });

    piper.sender().send(41).unwrap();
    piper.shutdown();
    assert_eq!(
        piper
            .receiver()
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        42
    );
    worker.join().unwrap();
    piper.join().unwrap();
}

#[test]
fn external_node_acquires_reusable_output_buffer() {
    let factory_count = Arc::new(AtomicUsize::new(0));
    let factory_count_for_builder = Arc::clone(&factory_count);

    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let external_output = builder.link::<BufferLease<Vec<u32>>>();
    let external_token = builder.add_external_node_to_with_reusable_output(
        input,
        "external",
        external_output,
        move || {
            factory_count_for_builder.fetch_add(1, Ordering::Relaxed);
            Vec::<u32>::new()
        },
    );
    let mut piper = Piper::<u32, BufferLease<Vec<u32>>, TestError>::start(
        config(),
        builder.finish(external_output),
    )
    .unwrap();
    let external = piper.take_external_node(external_token);

    let worker = std::thread::spawn(move || {
        loop {
            match external.recv_timeout(Duration::from_millis(5)) {
                Ok(value) => {
                    let mut lease = external.acquire_output();
                    lease.push(value);
                    external.send(lease).unwrap();
                }
                Err(piper::RecvOutputError::Timeout) if external.is_shutting_down() => break,
                Err(piper::RecvOutputError::Timeout) => continue,
                Err(piper::RecvOutputError::Closed) => break,
                Err(error) => panic!("external recv failed: {error}"),
            }
        }
    });

    piper.sender().send(1).unwrap();
    let first = piper
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(&*first, &[1]);
    drop(first);

    piper.sender().send(2).unwrap();
    let second = piper
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(&*second, &[2]);
    drop(second);

    assert_eq!(factory_count.load(Ordering::Relaxed), 1);

    piper.shutdown();
    worker.join().unwrap();
    piper.join().unwrap();
}

#[test]
fn external_node_acquire_output_panics_without_factory() {
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let output = builder.link::<u32>();
    let external_token = builder.add_external_node_to::<u32, u32>(input, "external", output);
    let mut piper = Piper::<u32, u32, TestError>::start(config(), builder.finish(output)).unwrap();
    let external = piper.take_external_node(external_token);

    let panic_payload = catch_unwind(AssertUnwindSafe(|| {
        let _ = external.acquire_output();
    }))
    .expect_err("acquire_output should panic without factory");
    let message = panic_payload_to_string(panic_payload);
    assert!(message.contains("external_node.acquire_output()"));

    piper.shutdown();
    drop(external);
    piper.join().unwrap();
}

#[test]
fn external_node_failure_is_reported_from_join() {
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let output = builder.link::<u32>();
    let external_token = builder.add_external_node_to::<u32, u32>(input, "external", output);
    let mut piper = Piper::<u32, u32, TestError>::start(config(), builder.finish(output)).unwrap();
    let external = piper.take_external_node(external_token);

    external.fail(TestError::Test);
    drop(external);
    std::thread::sleep(Duration::from_millis(50));
    let error = piper.join().expect_err("external failure should fail join");
    assert!(matches!(
        error,
        PiperError::ExternalNode {
            node,
            error: TestError::Test
        } if node == "external"
    ));
}

#[test]
fn join_drops_canonical_external_handles_before_supervisor_join() {
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let output = builder.link::<u32>();
    let _external_token = builder.add_external_node_to::<u32, u32>(input, "external", output);
    let piper = Piper::<u32, u32, TestError>::start(config(), builder.finish(output)).unwrap();

    piper.join().unwrap();
}

#[test]
fn generated_run_join_drops_external_handles_before_supervisor_join() {
    let run = ExternalJoinPipeline::start().unwrap();

    run.join().unwrap();
}

#[test]
fn external_node_telemetry_uses_link_rates() {
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let output = builder.link::<u32>();
    let external_token = builder.add_external_node_to::<u32, u32>(input, "external", output);
    let mut piper = Piper::<u32, u32, TestError>::start(config(), builder.finish(output)).unwrap();
    let external = piper.take_external_node(external_token);

    piper.sender().send(7).unwrap();
    let value = external.recv_timeout(Duration::from_secs(1)).unwrap();
    external.send(value + 1).unwrap();
    std::thread::sleep(Duration::from_millis(20));

    let telemetry = piper.get_telemetry();
    let external_stage = telemetry
        .nodes
        .iter()
        .find(|stage| stage.name == "external")
        .unwrap();
    assert!(external_stage.is_external);
    assert_eq!(external_stage.active_threads, 0);
    assert_eq!(external_stage.busy_ratio, 0.0);
    assert_eq!(external_stage.service_time, Duration::ZERO);
    assert_eq!(external_stage.per_worker_throughput, 0.0);
    assert!(external_stage.external_input_rate > 0.0);
    assert!(external_stage.external_output_rate > 0.0);

    assert_eq!(
        piper
            .receiver()
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        8
    );
    piper.shutdown();
    drop(external);
    piper.join().unwrap();
}

#[test]
fn piper_allows_zero_or_multiple_anchors() {
    let no_anchor =
        Piper::<u32, u32, TestError>::start(config(), one_stage_graph(node("pass", Pass)))
            .unwrap();
    no_anchor.abort();
    no_anchor.join().unwrap();

    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let left = builder.add_node(input, anchor(node("left", Pass)).max_threads(1));
    let output = builder.add_node(left, anchor(node("right", Pass)).max_threads(1));
    let two_anchors =
        Piper::<u32, u32, TestError>::start(config(), builder.finish(output)).unwrap();
    assert_eq!(two_anchors.get_telemetry().anchors.len(), 2);
    two_anchors.abort();
    two_anchors.join().unwrap();
}

fn read_telemetry_log(path: &std::path::Path) -> (serde_json::Value, Vec<String>, Vec<String>) {
    let content = fs::read_to_string(path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines[0], "# piper-telemetry-log-v2");
    assert!(lines[1].starts_with("# manifest "));
    let manifest: serde_json::Value =
        serde_json::from_str(lines[1].strip_prefix("# manifest ").unwrap()).unwrap();
    let header = lines[2].split(',').map(str::to_string).collect::<Vec<_>>();
    let samples = lines[3..]
        .iter()
        .filter(|line| !line.is_empty())
        .map(|line| line.to_string())
        .collect();
    (manifest, header, samples)
}

fn fork_join_telemetry_graph() -> PipelineGraph<u32, u32, TestError> {
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let fork = builder.add_node(input, node("prepare", Pass));
    let merged = builder.link();
    builder.add_node_to(fork, node("left", Pass), merged);
    builder.add_node_to(fork, node("right", Pass), merged);
    let output = builder.add_node(merged, node("merge", Pass));
    builder.finish(output)
}

#[test]
fn telemetry_log_writes_manifest_header_and_samples() {
    let path = std::env::temp_dir().join(format!(
        "piper_test_{}_{}.piper.csv",
        std::process::id(),
        std::thread::current().name().unwrap_or("runtime")
    ));
    let _ = fs::remove_file(&path);

    let piper = Piper::<u32, u32, TestError>::start(
        PiperConfig {
            csv_telemetry: Some(TelemetryLogConfig::new(&path).interval(Duration::from_millis(5))),
            ..config()
        },
        one_stage_graph(anchor(node("pass", Pass)).max_threads(1)),
    )
    .unwrap();
    piper.sender().send(1).unwrap();
    assert_eq!(
        piper
            .receiver()
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        1
    );
    piper.shutdown();
    piper.join().unwrap();

    let (manifest, header, samples) = read_telemetry_log(&path);
    assert_eq!(manifest["format"], "piper-telemetry-log-v2");
    assert_eq!(manifest["version"], 2);
    assert!(
        manifest["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|stage| stage["name"] == "pass")
    );
    assert!(manifest["links"].as_array().unwrap().len() >= 2);
    let link = manifest["links"]
        .as_array()
        .unwrap()
        .iter()
        .find(|link| {
            link["producers"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("input"))
        })
        .expect("input link");
    assert!(
        link["consumers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|consumer| consumer.as_str() == Some("node0"))
    );
    assert!(!header.iter().any(|column| column == "node0_name"));
    assert!(!header.iter().any(|column| column == "anchor_count"));
    assert!(header.contains(&"node0_service_time_ms".to_string()));
    assert!(header.contains(&"link0_arrival_rate".to_string()));
    assert!(!samples.is_empty());

    let existing = Piper::<u32, u32, TestError>::start(
        PiperConfig {
            csv_telemetry: Some(TelemetryLogConfig::new(&path)),
            ..config()
        },
        one_stage_graph(anchor(node("pass", Pass)).max_threads(1)),
    );
    let Err(existing) = existing else {
        panic!("existing telemetry log path should fail");
    };
    assert!(matches!(existing, PiperError::Telemetry { .. }));
    let _ = fs::remove_file(&path);
}

#[test]
fn telemetry_log_manifest_classifies_fork_and_join_links() {
    let path = std::env::temp_dir().join(format!(
        "piper_fork_join_telemetry_{}_{}.piper.csv",
        std::process::id(),
        std::thread::current().name().unwrap_or("runtime")
    ));
    let _ = fs::remove_file(&path);

    let piper = Piper::<u32, u32, TestError>::start(
        PiperConfig {
            csv_telemetry: Some(TelemetryLogConfig::new(&path).interval(Duration::from_millis(5))),
            ..config()
        },
        fork_join_telemetry_graph(),
    )
    .unwrap();
    piper.sender().send(1).unwrap();
    assert_eq!(
        piper
            .receiver()
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        1
    );
    piper.shutdown();
    piper.join().unwrap();

    let (manifest, _, _) = read_telemetry_log(&path);
    let links = manifest["links"].as_array().unwrap();
    let fork_link = links
        .iter()
        .find(|link| link["kind"] == "fork")
        .expect("fork link");
    assert_eq!(fork_link["producers"], serde_json::json!(["node0"]));
    assert!(
        fork_link["consumers"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("node1"))
    );
    assert!(
        fork_link["consumers"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("node2"))
    );
    let join_link = links
        .iter()
        .find(|link| link["kind"] == "join")
        .expect("join link");
    assert!(
        join_link["producers"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("node1"))
    );
    assert!(
        join_link["producers"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("node2"))
    );
    assert_eq!(join_link["consumers"], serde_json::json!(["output"]));
    let _ = fs::remove_file(&path);
}

struct SlowCount {
    count: Arc<AtomicUsize>,
}

impl Node for SlowCount {
    type Input = u32;
    type Output = u32;
    type Error = TestError;
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
        self.count.fetch_add(1, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(25));
        ctx.emit(input);
        Ok(())
    }
}

fn weighted_branch_graph(
    _left: Arc<AtomicUsize>,
    _right: Arc<AtomicUsize>,
    left_stage: impl IntoNodeSpec<TestError, Input = u32, Output = u32>,
    right_stage: impl IntoNodeSpec<TestError, Input = u32, Output = u32>,
) -> PipelineGraph<u32, u32, TestError> {
    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let branch_links = builder.add_single_thread_weighted_branch(
        input,
        "branch",
        SingleThreadWeightedBranchConfig {
            target_queue_seconds: 0.5,
            ewma_half_life: Duration::from_millis(25),
        },
    );
    let merged = builder.link();
    builder.add_node_to(branch_links.left, left_stage, merged);
    builder.add_node_to(branch_links.right, right_stage, merged);
    let output = builder.add_node(merged, node("merge", Pass));
    builder.finish(output)
}

#[test]
fn weighted_branch_routes_each_input_exactly_once() {
    let left = Arc::new(AtomicUsize::new(0));
    let right = Arc::new(AtomicUsize::new(0));
    let piper = Piper::<u32, u32, TestError>::start(
        config(),
        weighted_branch_graph(
            Arc::clone(&left),
            Arc::clone(&right),
            node(
                "left",
                CountPass {
                    count: Arc::clone(&left),
                },
            ),
            node(
                "right",
                CountPass {
                    count: Arc::clone(&right),
                },
            ),
        ),
    )
    .unwrap();

    for value in 0..100 {
        piper.sender().send(value).unwrap();
    }
    for _ in 0..100 {
        piper
            .receiver()
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
    }

    assert_eq!(
        left.load(Ordering::Relaxed) + right.load(Ordering::Relaxed),
        100
    );
    piper.shutdown();
    piper.join().unwrap();
}

#[test]
fn weighted_branch_routes_more_to_faster_downstream_after_backlog() {
    let left = Arc::new(AtomicUsize::new(0));
    let right = Arc::new(AtomicUsize::new(0));
    let piper = Piper::<u32, u32, TestError>::start(
        PiperConfig {
            sample_interval: Duration::from_millis(2),
            ..config()
        },
        weighted_branch_graph(
            Arc::clone(&left),
            Arc::clone(&right),
            node(
                "slow",
                SlowCount {
                    count: Arc::clone(&left),
                },
            ),
            node(
                "fast",
                CountPass {
                    count: Arc::clone(&right),
                },
            ),
        ),
    )
    .unwrap();

    for value in 0..400 {
        piper.sender().send(value).unwrap();
        if value % 4 == 3 {
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    std::thread::sleep(Duration::from_secs(2));
    piper.shutdown();
    piper.join().unwrap();

    let left_count = left.load(Ordering::Relaxed);
    let right_count = right.load(Ordering::Relaxed);
    assert_eq!(left_count + right_count, 400);
    assert!(right_count > left_count);
    assert!(right_count > left_count + left_count / 4);
}

#[test]
fn weighted_branch_snapshot_reports_output_links() {
    let left = Arc::new(AtomicUsize::new(0));
    let right = Arc::new(AtomicUsize::new(0));
    let piper = Piper::<u32, u32, TestError>::start(
        config(),
        weighted_branch_graph(
            Arc::clone(&left),
            Arc::clone(&right),
            node(
                "left",
                CountPass {
                    count: Arc::clone(&left),
                },
            ),
            node(
                "right",
                CountPass {
                    count: Arc::clone(&right),
                },
            ),
        ),
    )
    .unwrap();

    std::thread::sleep(Duration::from_millis(20));
    let telemetry = piper.get_telemetry();
    let branch = telemetry
        .nodes
        .iter()
        .find(|stage| stage.name == "branch")
        .expect("branch stage");
    assert_eq!(branch.output_links.len(), 2);
    assert_eq!(branch.output_link, branch.output_links[0]);
    assert_eq!(branch.active_threads, 1);
    assert_eq!(branch.desired_workers, 1);
    assert!(!branch.is_anchor);
    assert!(!branch.is_external);

    let merge = telemetry
        .nodes
        .iter()
        .find(|stage| stage.name == "merge")
        .expect("merge stage");
    assert_eq!(merge.output_links.len(), 1);

    piper.abort();
    piper.join().unwrap();
}

fn weighted_branch_telemetry_graph() -> PipelineGraph<u32, u32, TestError> {
    let left = Arc::new(AtomicUsize::new(0));
    let right = Arc::new(AtomicUsize::new(0));
    weighted_branch_graph(left, right, node("left", Pass), node("right", Pass))
}

#[test]
fn weighted_branch_telemetry_manifest_lists_both_output_links() {
    let path = std::env::temp_dir().join(format!(
        "piper_weighted_branch_telemetry_{}_{}.piper.csv",
        std::process::id(),
        std::thread::current().name().unwrap_or("runtime")
    ));
    let _ = fs::remove_file(&path);

    let piper = Piper::<u32, u32, TestError>::start(
        PiperConfig {
            csv_telemetry: Some(TelemetryLogConfig::new(&path).interval(Duration::from_millis(5))),
            ..config()
        },
        weighted_branch_telemetry_graph(),
    )
    .unwrap();
    piper.sender().send(1).unwrap();
    piper.shutdown();
    piper.join().unwrap();

    let (manifest, header, _) = read_telemetry_log(&path);
    let branch = manifest["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|stage| stage["name"] == "branch")
        .expect("branch stage");
    assert_eq!(branch["output_links"].as_array().unwrap().len(), 2);
    assert_eq!(
        branch["output_link"],
        branch["output_links"].as_array().unwrap()[0]
    );

    let links = manifest["links"].as_array().unwrap();
    let left_link = links
        .iter()
        .find(|link| {
            link["producers"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("node0"))
                && link["consumers"]
                    .as_array()
                    .unwrap()
                    .contains(&serde_json::json!("node1"))
        })
        .expect("left branch output link");
    let right_link = links
        .iter()
        .find(|link| {
            link["producers"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("node0"))
                && link["consumers"]
                    .as_array()
                    .unwrap()
                    .contains(&serde_json::json!("node2"))
        })
        .expect("right branch output link");
    assert_ne!(left_link["id"], right_link["id"]);

    assert!(header.iter().any(|column| column.starts_with("link")));
    assert!(
        !header
            .iter()
            .any(|column| column.contains("branch_internal"))
    );
    let _ = fs::remove_file(&path);
}

#[cfg(feature = "feeder")]
#[test]
fn weighted_branch_rejects_feeder_on_output_links() {
    use piper::FeederLinkConfig;

    let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
    let input = builder.input();
    let left = builder.link::<u32>();
    let right = builder.link::<u32>();
    builder.feeder_link(left, FeederLinkConfig::default());
    builder.add_single_thread_weighted_branch_to(
        input,
        "branch",
        left,
        right,
        SingleThreadWeightedBranchConfig::default(),
    );
    let merged = builder.link();
    builder.add_node_to(left, node("left", Pass), merged);
    builder.add_node_to(right, node("right", Pass), merged);
    let output = builder.add_node(merged, node("merge", Pass));
    let graph = builder.finish(output);

    let Err(error) = Piper::<u32, u32, TestError>::start(config(), graph) else {
        panic!("feeder output on weighted branch should fail");
    };
    assert!(matches!(
        error,
        PiperError::UnsupportedWeightedBranchFeederOutput { .. }
    ));
}

#[cfg(feature = "feeder")]
mod feeder_tests {
    use super::*;
    use piper::{FeederLinkConfig, RecvOutputError, TryRecvOutputError};
    use std::num::NonZeroUsize;

    fn feeder_config() -> FeederLinkConfig {
        FeederLinkConfig::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        )
    }

    fn feeder_test_config() -> PiperConfig {
        PiperConfig {
            sample_interval: Duration::from_millis(50),
            poll_interval: Duration::from_millis(2),
            global_worker_cap: Some(8),
            csv_telemetry: None,
        }
    }

    #[test]
    fn feeder_boundary_input_external_output() {
        let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
        let input = builder.input();
        let output = builder.link::<u32>();
        let cfg = feeder_config();
        builder.feeder_link(input, cfg.clone());
        builder.feeder_link(output, cfg);
        let external_token = builder.add_external_node_to::<u32, u32>(input, "external", output);
        let mut piper =
            Piper::<u32, u32, TestError>::start(feeder_test_config(), builder.finish(output))
                .unwrap();
        let external = piper.take_external_node(external_token);

        piper.sender().send(3).unwrap();
        let value = external.recv_timeout(Duration::from_secs(1)).unwrap();
        external.send(value + 1).unwrap();
        assert_eq!(
            piper
                .receiver()
                .recv_timeout(Duration::from_secs(1))
                .unwrap(),
            4
        );
        piper.shutdown();
        drop(external);
        piper.join().unwrap();
    }

    #[test]
    fn feeder_external_input_clone_drains_concurrently() {
        let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
        let input = builder.input();
        let output = builder.link::<u32>();
        builder.feeder_link(input, feeder_config());
        let external_token = builder.add_external_node_to::<u32, u32>(input, "external", output);
        let mut piper =
            Piper::<u32, u32, TestError>::start(feeder_test_config(), builder.finish(output))
                .unwrap();
        let external_a = piper.take_external_node(external_token);
        let external_b = external_a.clone();

        const ITEMS: u32 = 16;
        for value in 0..ITEMS {
            piper.sender().send(value).unwrap();
        }
        piper.shutdown();

        let worker = std::thread::spawn(move || {
            loop {
                match external_a.recv_timeout(Duration::from_secs(1)) {
                    Ok(value) => {
                        external_a.send(value + 100).unwrap();
                    }
                    Err(RecvOutputError::Closed) => break,
                    Err(RecvOutputError::Timeout) => break,
                    Err(error) => panic!("external recv failed: {error}"),
                }
            }
        });

        loop {
            match external_b.recv_timeout(Duration::from_secs(1)) {
                Ok(value) => {
                    external_b.send(value + 100).unwrap();
                }
                Err(RecvOutputError::Closed) => break,
                Err(RecvOutputError::Timeout) => break,
                Err(error) => panic!("external recv failed: {error}"),
            }
        }
        worker.join().unwrap();

        let mut outputs = Vec::new();
        let receiver = piper.receiver();
        while outputs.len() < ITEMS as usize {
            outputs.push(receiver.recv_timeout(Duration::from_secs(1)).unwrap());
        }
        outputs.sort_unstable();
        let expected: Vec<u32> = (0..ITEMS).map(|value| value + 100).collect();
        assert_eq!(outputs, expected);

        piper.shutdown();
        drop(external_b);
        piper.join().unwrap();
    }

    #[test]
    fn feeder_external_output_before_managed_node() {
        let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
        let input = builder.input();
        let external_out = builder.link::<u32>();
        let output = builder.link::<u32>();
        builder.feeder_link(external_out, feeder_config());
        let external_token =
            builder.add_external_node_to::<u32, u32>(input, "external", external_out);
        builder.add_node_to(
            external_out,
            anchor(node("pass", Pass)).fixed_threads(1),
            output,
        );
        let mut piper =
            Piper::<u32, u32, TestError>::start(feeder_test_config(), builder.finish(output))
                .unwrap();
        let external = piper.take_external_node(external_token);

        piper.sender().send(5).unwrap();
        let value = external.recv_timeout(Duration::from_secs(1)).unwrap();
        external.send(value).unwrap();
        assert_eq!(
            piper
                .receiver()
                .recv_timeout(Duration::from_secs(1))
                .expect("pipeline output"),
            5
        );
        piper.shutdown();
        drop(external);
        piper.join().unwrap();
    }

    #[test]
    fn feeder_receiver_timeout_and_try_while_running_and_closed_after_shutdown() {
        let mut builder = PipelineGraphBuilder::<u32, TestError>::new();
        let input = builder.input();
        let output = builder.link::<u32>();
        builder.feeder_link(output, feeder_config());
        let external_token = builder.add_external_node_to::<u32, u32>(input, "external", output);
        let mut piper =
            Piper::<u32, u32, TestError>::start(feeder_test_config(), builder.finish(output))
                .unwrap();
        let external = piper.take_external_node(external_token);
        let receiver = piper.receiver();

        assert!(matches!(
            receiver.try_recv(),
            Err(TryRecvOutputError::Empty)
        ));
        assert!(matches!(
            receiver.recv_timeout(Duration::from_millis(5)),
            Err(RecvOutputError::Timeout)
        ));
        assert!(matches!(
            external.try_recv(),
            Err(TryRecvOutputError::Empty)
        ));
        assert!(matches!(
            external.recv_timeout(Duration::from_millis(5)),
            Err(RecvOutputError::Timeout)
        ));

        piper.shutdown();
        drop(external);
        piper.join().unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            match receiver.try_recv() {
                Err(TryRecvOutputError::Closed) => break,
                Err(TryRecvOutputError::Empty) if std::time::Instant::now() >= deadline => {
                    panic!("feeder receiver did not close after shutdown");
                }
                Ok(_) | Err(TryRecvOutputError::TypeMismatch) => {
                    panic!("unexpected item after shutdown");
                }
                Err(TryRecvOutputError::Empty) => {
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        }
        assert!(matches!(
            receiver.recv_timeout(Duration::from_millis(5)),
            Err(RecvOutputError::Closed)
        ));
    }
}

struct Fail;

impl Node for Fail {
    type Input = u32;
    type Output = u32;
    type Error = TestError;
    type State = ();

    fn init(&self) -> std::result::Result<Self::State, Self::Error> {
        Ok(())
    }

    fn process(
        &self,
        _state: &mut Self::State,
        _input: Self::Input,
        _ctx: &mut NodeContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error> {
        Err(TestError::Test)
    }
}

#[test]
fn scalable_non_anchor_node_is_not_marked_anchor() {
    let piper = Piper::<u32, u32, TestError>::start(
        config(),
        one_stage_graph(node("pass", Pass).scalable_threads(2, 4)),
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let telemetry = piper.get_telemetry();
    assert!(!telemetry.nodes[0].is_anchor);
    assert_eq!(
        telemetry.nodes[0].thread_policy_kind,
        NodeThreadPolicyKind::Scalable
    );
    piper.abort();
    piper.join().unwrap();
}

#[test]
fn max_threads_without_anchor_does_not_create_anchor() {
    let piper = Piper::<u32, u32, TestError>::start(
        config(),
        one_stage_graph(node("pass", Pass).max_threads(2)),
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let telemetry = piper.get_telemetry();
    assert!(!telemetry.nodes[0].is_anchor);
    assert_eq!(telemetry.anchors.len(), 0);
    piper.abort();
    piper.join().unwrap();
}

#[test]
fn anchor_without_explicit_policy_gets_default_scalable() {
    let piper = Piper::<u32, u32, TestError>::start(
        config(),
        one_stage_graph(anchor(node("pass", Pass))),
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let telemetry = piper.get_telemetry();
    assert!(telemetry.nodes[0].is_anchor);
    assert_eq!(
        telemetry.nodes[0].thread_policy_kind,
        NodeThreadPolicyKind::Scalable
    );
    assert!(telemetry.nodes[0].max_thread_count.unwrap() >= 1);
    piper.abort();
    piper.join().unwrap();
}

#[test]
fn telemetry_v2_manifest_uses_nodes_and_node_columns() {
    let path = std::env::temp_dir().join(format!(
        "piper_v2_telemetry_{}_{}.piper.csv",
        std::process::id(),
        std::thread::current().name().unwrap_or("runtime")
    ));
    let _ = fs::remove_file(&path);

    let piper = Piper::<u32, u32, TestError>::start(
        PiperConfig {
            csv_telemetry: Some(TelemetryLogConfig::new(&path).interval(Duration::from_millis(5))),
            ..config()
        },
        one_stage_graph(node("pass", Pass).scalable_threads(1, 2)),
    )
    .unwrap();
    piper.sender().send(1).unwrap();
    let _ = piper.receiver().recv_timeout(Duration::from_secs(1)).unwrap();
    piper.shutdown();
    piper.join().unwrap();

    let (manifest, header, _) = read_telemetry_log(&path);
    assert_eq!(manifest["format"], "piper-telemetry-log-v2");
    assert_eq!(manifest["version"], 2);
    assert!(manifest.get("nodes").is_some());
    assert!(manifest.get("stages").is_none());
    assert!(header.iter().any(|c| c.starts_with("node0_")));
    assert!(!header.iter().any(|c| c.starts_with("stage0_")));
    let _ = fs::remove_file(&path);
}

#[cfg(not(any(feature = "channel-kanal", feature = "channel-crossbeam")))]
compile_error!("enable `channel-kanal` or `channel-crossbeam`");

use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use std::any::Any;
#[cfg(feature = "feeder")]
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
#[cfg(feature = "feeder")]
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use thiserror::Error;

pub mod channel;
pub use parking_lot;
pub use piper_macros::pipeline;

type Message = Box<dyn Any + Send>;
type DynAcquire = Arc<dyn Any + Send + Sync>;
type AcquireFn<Out> = dyn Fn() -> Out + Send + Sync + 'static;

pub type Result<T, E = String> = std::result::Result<T, PiperError<E>>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PiperError<E: Debug + Display = String> {
    #[error("worker count must be greater than 0")]
    ZeroWorkers,

    #[error("Piper requires at least one node")]
    NoNodes,

    #[error("Piper graph is invalid: {message}")]
    InvalidGraph { message: String },

    #[error("node thread count must be greater than 0")]
    InvalidThreadCount,

    #[error("invalid weighted branch `{branch}` config: {message}")]
    InvalidWeightedBranchConfig { branch: String, message: String },

    #[error("weighted branch `{branch}` output link {link} cannot use feeder")]
    UnsupportedWeightedBranchFeederOutput { branch: String, link: usize },

    #[error("failed to spawn worker thread `{worker}`")]
    SpawnFailed {
        worker: String,
        #[source]
        source: std::io::Error,
    },

    #[error("worker thread `{worker}` panicked: {message}")]
    WorkerPanicked { worker: String, message: String },

    #[error("init closure failed in worker `{worker}`: {error}")]
    UserInit { worker: String, error: E },

    #[error("process closure failed in worker `{worker}`: {error}")]
    UserProcess { worker: String, error: E },

    #[error("cleanup closure failed in worker `{worker}`: {error}")]
    UserCleanup { worker: String, error: E },

    #[error("merge closure failed in worker `{worker}`: {error}")]
    UserMerge { worker: String, error: E },

    #[error("finalize closure failed in worker `{worker}`: {error}")]
    UserFinalize { worker: String, error: E },

    #[error("node `{node}` does not provide a state merge function")]
    MissingStateMerge { node: String },

    #[error("external node `{node}` failed: {error}")]
    ExternalNode { node: String, error: E },

    #[error("internal Piper failure in worker `{worker}`: {message}")]
    Internal { worker: String, message: String },

    #[error("Piper telemetry failure: {message}")]
    Telemetry { message: String },
}

#[derive(Clone)]
pub struct PipeConfig {
    pub num_workers: usize,
    pub poll_interval: Duration,
    pub cancel: Arc<AtomicBool>,
}

pub struct Pipe<Msg, Output, UserErr = String>
where
    UserErr: Debug + Display + Send + 'static,
{
    sender: channel::Sender<Msg>,
    workers: Vec<(
        String,
        JoinHandle<std::result::Result<Output, PiperError<UserErr>>>,
    )>,
}

impl<Msg, Output, UserErr> Pipe<Msg, Output, UserErr>
where
    Msg: Send + 'static,
    Output: Send + 'static,
    UserErr: Debug + Display + Send + 'static,
{
    pub fn new<Storage, Init, Process, Finalize>(
        config: PipeConfig,
        init: Init,
        process: Process,
        finalize: Finalize,
    ) -> Result<Self, UserErr>
    where
        Storage: Send + 'static,
        Init: Fn() -> std::result::Result<Storage, UserErr> + Send + Sync + 'static,
        Process: Fn(&mut Storage, Msg) -> std::result::Result<(), UserErr> + Send + Sync + 'static,
        Finalize: Fn(Storage) -> std::result::Result<Output, UserErr> + Send + Sync + 'static,
    {
        if config.num_workers == 0 {
            return Err(PiperError::ZeroWorkers);
        }

        let (sender, receiver) = channel::unbounded::<Msg>();
        let init = Arc::new(init);
        let process = Arc::new(process);
        let finalize = Arc::new(finalize);
        let mut workers = Vec::with_capacity(config.num_workers);

        for worker_index in 0..config.num_workers {
            let name = format!("pipe-worker-{worker_index}");
            let receiver = receiver.clone();
            let cancel = Arc::clone(&config.cancel);
            let poll_interval = config.poll_interval;
            let init = Arc::clone(&init);
            let process = Arc::clone(&process);
            let finalize = Arc::clone(&finalize);
            let thread_name = name.clone();

            let worker = thread::Builder::new()
                .name(name.clone())
                .spawn(
                    move || -> std::result::Result<Output, PiperError<UserErr>> {
                        let mut storage = init().map_err(|error| PiperError::UserInit {
                            worker: thread_name.clone(),
                            error,
                        })?;
                        loop {
                            if cancel.load(Ordering::Acquire) {
                                break;
                            }
                            match receiver.recv_timeout(poll_interval) {
                                Ok(msg) => process(&mut storage, msg).map_err(|error| {
                                    PiperError::UserProcess {
                                        worker: thread_name.clone(),
                                        error,
                                    }
                                })?,
                                Err(channel::RecvTimeoutError::Timeout) => continue,
                                Err(channel::RecvTimeoutError::Closed) => break,
                            }
                        }
                        finalize(storage).map_err(|error| PiperError::UserFinalize {
                            worker: thread_name.clone(),
                            error,
                        })
                    },
                )
                .map_err(|source| PiperError::SpawnFailed {
                    worker: name.clone(),
                    source,
                })?;

            workers.push((name, worker));
        }

        drop(receiver);

        Ok(Pipe { sender, workers })
    }

    pub fn sender(&self) -> channel::Sender<Msg> {
        self.sender.clone()
    }

    pub fn num_workers(&self) -> usize {
        self.workers.len()
    }

    pub fn join(self) -> Result<Vec<Output>, UserErr> {
        drop(self.sender);
        let mut results = Vec::with_capacity(self.workers.len());
        for (name, worker) in self.workers {
            let inner_result = worker
                .join()
                .map_err(|payload| PiperError::WorkerPanicked {
                    worker: name.clone(),
                    message: panic_payload_to_string(payload),
                })?;
            let output = inner_result?;
            results.push(output);
        }
        Ok(results)
    }
}

#[derive(Clone, Debug)]
pub struct PiperConfig {
    pub sample_interval: Duration,
    pub poll_interval: Duration,
    pub global_worker_cap: Option<usize>,
    pub csv_telemetry: Option<TelemetryLogConfig>,
}

impl Default for PiperConfig {
    fn default() -> Self {
        Self {
            sample_interval: Duration::from_millis(10),
            poll_interval: Duration::from_millis(10),
            global_worker_cap: None,
            csv_telemetry: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TelemetryLogConfig {
    pub path: PathBuf,
    pub interval: Duration,
}

impl TelemetryLogConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            interval: Duration::from_millis(250),
        }
    }

    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum QueueTrend {
    Starved,
    FastDraining,
    Draining,
    Stable,
    Growing,
    FastGrowing,
    Runaway,
}

impl QueueTrend {
    pub fn code(self) -> u8 {
        self as u8
    }

    pub fn is_draining(self) -> bool {
        matches!(
            self,
            QueueTrend::Starved | QueueTrend::FastDraining | QueueTrend::Draining
        )
    }

    pub fn is_growing(self) -> bool {
        matches!(
            self,
            QueueTrend::Growing | QueueTrend::FastGrowing | QueueTrend::Runaway
        )
    }
}

#[derive(Clone, Debug)]
pub struct LinkSnapshot {
    pub index: usize,
    pub len: usize,
    pub trend: QueueTrend,
    pub arrival_rate: f64,
    pub drain_rate: f64,
    pub net_rate: f64,
    pub smoothed_len: f64,
}

#[derive(Clone, Debug)]
pub struct NodeScalePolicy {
    pub initial_threads: usize,
    pub max_threads: usize,
    pub target_queue_seconds: f64,
    pub low_queue_seconds: f64,
    pub scale_down_after: Duration,
    pub underutilized_busy_ratio: f64,
}

impl Default for NodeScalePolicy {
    fn default() -> Self {
        let max_threads = thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1)
            .max(1);
        let initial_threads = max_threads.div_ceil(2).max(1);
        Self {
            initial_threads,
            max_threads,
            target_queue_seconds: 1.0,
            low_queue_seconds: 0.25,
            scale_down_after: Duration::from_millis(500),
            underutilized_busy_ratio: 0.35,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeThreadPolicyKind {
    Fixed,
    Scalable,
    ImplicitSupport,
}

#[derive(Clone, Debug)]
pub struct NodeSnapshot {
    pub index: usize,
    pub name: String,
    pub input_link: usize,
    pub output_link: usize,
    pub output_links: Vec<usize>,
    pub active_threads: usize,
    pub processed_count: u64,
    pub busy_ratio: f64,
    pub service_time: Duration,
    pub per_worker_throughput: f64,
    pub desired_workers: usize,
    pub scaling_state: NodeScalingState,
    pub is_anchor: bool,
    pub thread_policy_kind: NodeThreadPolicyKind,
    pub fixed_thread_count: Option<usize>,
    pub max_thread_count: Option<usize>,
    pub target_queue_seconds: Option<f64>,
    pub low_queue_seconds: Option<f64>,
    pub backlog_seconds: f64,
    pub is_external: bool,
    pub external_input_rate: f64,
    pub external_output_rate: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeScalingState {
    Eligible,
    Merging,
    Settling,
    BackingOff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnchorPressureReason {
    InputUnderfed,
    OutputBackpressure,
    BudgetPressure,
}

#[derive(Clone, Debug)]
pub struct AnchorSnapshot {
    pub node_index: usize,
    pub node_name: String,
    pub active_threads: usize,
    pub last_pressure_reason: Option<AnchorPressureReason>,
}

#[derive(Clone, Debug)]
pub struct PiperSnapshot {
    pub links: Vec<LinkSnapshot>,
    pub nodes: Vec<NodeSnapshot>,
    pub anchors: Vec<AnchorSnapshot>,
    pub parked_threads: usize,
    pub total_active_workers: usize,
    pub global_worker_cap: usize,
    pub budget_pressure: bool,
    pub output_backpressure: bool,
    pub shutdown_requested: bool,
    pub abort_requested: bool,
    pub pending_scale_operation: bool,
}

#[derive(Debug, Error)]
#[error("Piper input channel is closed")]
pub struct SendInputError;

#[derive(Debug, Error)]
pub enum RecvOutputError {
    #[error("Piper output channel is closed")]
    Closed,
    #[error("Piper output channel timed out")]
    Timeout,
    #[error("Piper output type mismatch")]
    TypeMismatch,
}

#[derive(Debug, Error)]
pub enum TryRecvOutputError {
    #[error("Piper output channel is closed")]
    Closed,
    #[error("Piper output channel is empty")]
    Empty,
    #[error("Piper output type mismatch")]
    TypeMismatch,
}

pub struct ExternalNodeToken<In, Out>
where
    In: Send + 'static,
    Out: Send + 'static,
{
    index: usize,
    _marker: PhantomData<fn(In) -> Out>,
}

impl<In, Out> Clone for ExternalNodeToken<In, Out>
where
    In: Send + 'static,
    Out: Send + 'static,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<In, Out> Copy for ExternalNodeToken<In, Out>
where
    In: Send + 'static,
    Out: Send + 'static,
{
}

impl<In, Out> ExternalNodeToken<In, Out>
where
    In: Send + 'static,
    Out: Send + 'static,
{
    pub fn index(self) -> usize {
        self.index
    }
}

pub struct ExternalNode<In, Out, E = String>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    name: String,
    input: LinkReceiver,
    input_stats: Arc<LinkStats>,
    output: LinkSender,
    output_stats: Arc<LinkStats>,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    internal_failure: channel::Sender<InternalFailure>,
    output_acquire: Option<Arc<AcquireFn<Out>>>,
    _marker: PhantomData<fn(In, Out, E)>,
}

impl<In, Out, E> Clone for ExternalNode<In, Out, E>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            input: self.input.clone_for_external(),
            input_stats: Arc::clone(&self.input_stats),
            output: self.output.clone(),
            output_stats: Arc::clone(&self.output_stats),
            shutdown: Arc::clone(&self.shutdown),
            abort: Arc::clone(&self.abort),
            internal_failure: self.internal_failure.clone(),
            output_acquire: self.output_acquire.clone(),
            _marker: PhantomData,
        }
    }
}

impl<In, Out, E> ExternalNode<In, Out, E>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    pub fn acquire_output(&self) -> Out {
        let acquire = self.output_acquire.as_ref().unwrap_or_else(|| {
            panic!(
                "external_node.acquire_output() was called for an external node without a reusable output factory"
            )
        });
        acquire()
    }

    pub fn recv(&self) -> std::result::Result<In, RecvOutputError> {
        let input = self.input.recv().map_err(|_| RecvOutputError::Closed)?;
        self.input_stats.drains.fetch_add(1, Ordering::Relaxed);
        input
            .downcast::<In>()
            .map(|value| *value)
            .map_err(|_| RecvOutputError::TypeMismatch)
    }

    pub fn recv_timeout(&self, duration: Duration) -> std::result::Result<In, RecvOutputError> {
        let input = self
            .input
            .recv_timeout(duration)
            .map_err(|error| match error {
                LinkRecvError::Timeout => RecvOutputError::Timeout,
                LinkRecvError::Closed => RecvOutputError::Closed,
            })?;
        self.input_stats.drains.fetch_add(1, Ordering::Relaxed);
        input
            .downcast::<In>()
            .map(|value| *value)
            .map_err(|_| RecvOutputError::TypeMismatch)
    }

    pub fn try_recv(&self) -> std::result::Result<In, TryRecvOutputError> {
        let input = match self.input.try_recv() {
            Ok(input) => input,
            Err(LinkTryRecvError::Empty) => return Err(TryRecvOutputError::Empty),
            Err(LinkTryRecvError::Closed) => return Err(TryRecvOutputError::Closed),
        };
        self.input_stats.drains.fetch_add(1, Ordering::Relaxed);
        input
            .downcast::<In>()
            .map(|value| *value)
            .map_err(|_| TryRecvOutputError::TypeMismatch)
    }

    pub fn send(&self, output: Out) -> std::result::Result<(), SendInputError> {
        match self.output.send(Box::new(output)) {
            Ok(()) => {
                self.output_stats.arrivals.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(_) => {
                if !self.shutdown.load(Ordering::Acquire) && !self.abort.load(Ordering::Acquire) {
                    self.abort.store(true, Ordering::Release);
                    let _ = self
                        .internal_failure
                        .send(InternalFailure::internal(format!(
                            "external node `{}` output channel closed unexpectedly",
                            self.name
                        )));
                }
                Err(SendInputError)
            }
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    pub fn is_aborting(&self) -> bool {
        self.abort.load(Ordering::Acquire)
    }

    pub fn abort(&self) {
        self.abort.store(true, Ordering::Release);
    }

    pub fn fail(&self, error: E) {
        let _ = self
            .internal_failure
            .send(InternalFailure::external(self.name.clone(), error));
        self.abort.store(true, Ordering::Release);
    }
}

pub struct PiperSender<In> {
    inner: LinkSender,
    stats: Arc<LinkStats>,
    shutdown: Arc<AtomicBool>,
    _marker: PhantomData<fn(In)>,
}

impl<In> Clone for PiperSender<In> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            stats: Arc::clone(&self.stats),
            shutdown: Arc::clone(&self.shutdown),
            _marker: PhantomData,
        }
    }
}

impl<In> PiperSender<In>
where
    In: Send + 'static,
{
    pub fn send(&self, input: In) -> std::result::Result<(), SendInputError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(SendInputError);
        }
        self.inner
            .send(Box::new(input))
            .map_err(|_| SendInputError)?;
        self.stats.arrivals.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.shutdown.load(Ordering::Acquire) || self.inner.is_closed()
    }
}

pub struct PiperReceiver<Out> {
    inner: LinkReceiver,
    stats: Arc<LinkStats>,
    _marker: PhantomData<fn() -> Out>,
}

impl<Out> Clone for PiperReceiver<Out> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            stats: Arc::clone(&self.stats),
            _marker: PhantomData,
        }
    }
}

impl<Out> PiperReceiver<Out>
where
    Out: Send + 'static,
{
    pub fn recv(&self) -> std::result::Result<Out, RecvOutputError> {
        let output = self.inner.recv().map_err(|_| RecvOutputError::Closed)?;
        self.stats.drains.fetch_add(1, Ordering::Relaxed);
        output
            .downcast::<Out>()
            .map(|value| *value)
            .map_err(|_| RecvOutputError::TypeMismatch)
    }

    pub fn recv_timeout(&self, duration: Duration) -> std::result::Result<Out, RecvOutputError> {
        let output = self
            .inner
            .recv_timeout(duration)
            .map_err(|error| match error {
                LinkRecvError::Timeout => RecvOutputError::Timeout,
                LinkRecvError::Closed => RecvOutputError::Closed,
            })?;
        self.stats.drains.fetch_add(1, Ordering::Relaxed);
        output
            .downcast::<Out>()
            .map(|value| *value)
            .map_err(|_| RecvOutputError::TypeMismatch)
    }

    pub fn try_recv(&self) -> std::result::Result<Out, TryRecvOutputError> {
        let output = match self.inner.try_recv() {
            Ok(output) => output,
            Err(LinkTryRecvError::Empty) => return Err(TryRecvOutputError::Empty),
            Err(LinkTryRecvError::Closed) => return Err(TryRecvOutputError::Closed),
        };
        self.stats.drains.fetch_add(1, Ordering::Relaxed);
        output
            .downcast::<Out>()
            .map(|value| *value)
            .map_err(|_| TryRecvOutputError::TypeMismatch)
    }
}

pub trait Recycle {
    fn recycle(&mut self);
}

impl<T> Recycle for Vec<T> {
    fn recycle(&mut self) {
        self.clear();
    }
}

#[derive(Clone)]
struct LeaseRuntime {
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    internal_failure: channel::Sender<InternalFailure>,
}

pub struct BufferLease<T>
where
    T: Recycle + Send + 'static,
{
    value: Option<T>,
    recycle_sender: channel::Sender<T>,
    runtime: LeaseRuntime,
}

impl<T> BufferLease<T>
where
    T: Recycle + Send + 'static,
{
    fn new(value: T, recycle_sender: channel::Sender<T>, runtime: LeaseRuntime) -> Self {
        Self {
            value: Some(value),
            recycle_sender,
            runtime,
        }
    }

    pub fn into_inner(mut self) -> T {
        self.value
            .take()
            .expect("BufferLease value was already taken")
    }
}

impl<T> std::ops::Deref for BufferLease<T>
where
    T: Recycle + Send + 'static,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
            .as_ref()
            .expect("BufferLease value was already taken")
    }
}

impl<T> std::ops::DerefMut for BufferLease<T>
where
    T: Recycle + Send + 'static,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
            .as_mut()
            .expect("BufferLease value was already taken")
    }
}

impl<T> Drop for BufferLease<T>
where
    T: Recycle + Send + 'static,
{
    fn drop(&mut self) {
        let Some(mut value) = self.value.take() else {
            return;
        };
        value.recycle();
        if self.recycle_sender.send(value).is_err()
            && !self.runtime.shutdown.load(Ordering::Acquire)
            && !self.runtime.abort.load(Ordering::Acquire)
        {
            self.runtime.abort.store(true, Ordering::Release);
            let _ = self.internal_failure("recycle channel closed while returning BufferLease");
        }
    }
}

impl<T> BufferLease<T>
where
    T: Recycle + Send + 'static,
{
    fn internal_failure(&self, message: impl Into<String>) -> std::result::Result<(), ()> {
        self.runtime
            .internal_failure
            .send(InternalFailure::internal(message))
            .map_err(|_| ())
    }
}

pub struct NodeContext<Out, E = String>
where
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    output: LinkSender,
    output_stats: Arc<LinkStats>,
    output_acquire: Option<Arc<AcquireFn<Out>>>,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    internal_failure: channel::Sender<InternalFailure>,
    _marker: PhantomData<fn(E)>,
}

impl<Out, E> NodeContext<Out, E>
where
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    pub fn emit(&mut self, output: Out) {
        match self.output.send(Box::new(output) as Message) {
            Ok(()) => {
                self.output_stats.arrivals.fetch_add(1, Ordering::Relaxed);
            }
            Err(_)
                if !self.shutdown.load(Ordering::Acquire)
                    && !self.abort.load(Ordering::Acquire) =>
            {
                self.abort.store(true, Ordering::Release);
                let _ = self.internal_failure.send(InternalFailure::internal(
                    "node output channel closed unexpectedly",
                ));
            }
            Err(_) => {}
        }
    }

    pub fn acquire_output(&self) -> Out {
        let acquire = self.output_acquire.as_ref().unwrap_or_else(|| {
            panic!("ctx.acquire_output() was called for a node without a reusable output factory")
        });
        acquire()
    }

    pub fn is_aborting(&self) -> bool {
        self.abort.load(Ordering::Acquire)
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

pub trait Node: Send + Sync + 'static {
    type Input: Send + 'static;
    type Output: Send + 'static;
    type Error: Debug + Display + Send + 'static;
    type State: Send + 'static;

    fn init(&self) -> std::result::Result<Self::State, Self::Error>;

    fn process(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        ctx: &mut NodeContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error>;

    fn cleanup(&self, _state: Self::State) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}

trait DynNode<E>: Send + Sync
where
    E: Debug + Display + Send + 'static,
{
    fn init_box(&self) -> std::result::Result<Box<dyn Any + Send>, NodeFailure<E>>;

    fn process_box(
        &self,
        state: &mut dyn Any,
        input: Message,
        ctx: RuntimeNodeContext,
    ) -> std::result::Result<(), NodeFailure<E>>;

    fn cleanup_box(&self, state: Box<dyn Any + Send>) -> std::result::Result<(), NodeFailure<E>>;

    fn merge_box(
        &self,
        _target: &mut dyn Any,
        _source: Box<dyn Any + Send>,
    ) -> std::result::Result<(), NodeFailure<E>> {
        Err(NodeFailure::Internal(
            "node does not provide a state merge function".to_string(),
        ))
    }

    fn can_merge_state(&self) -> bool {
        false
    }
}

struct RuntimeNodeContext {
    output: LinkSender,
    output_stats: Arc<LinkStats>,
    output_acquire: Option<DynAcquire>,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    internal_failure: channel::Sender<InternalFailure>,
}

struct NodeAdapter<S>
where
    S: Node,
{
    stage: S,
}

impl<S> DynNode<S::Error> for NodeAdapter<S>
where
    S: Node,
{
    fn init_box(&self) -> std::result::Result<Box<dyn Any + Send>, NodeFailure<S::Error>> {
        self.stage
            .init()
            .map(|state| Box::new(state) as Box<dyn Any + Send>)
            .map_err(NodeFailure::Init)
    }

    fn process_box(
        &self,
        state: &mut dyn Any,
        input: Message,
        ctx: RuntimeNodeContext,
    ) -> std::result::Result<(), NodeFailure<S::Error>> {
        let state = state
            .downcast_mut::<S::State>()
            .ok_or_else(|| NodeFailure::Internal("node state type mismatch".to_string()))?;
        let input = input
            .downcast::<S::Input>()
            .map(|input| *input)
            .map_err(|_| NodeFailure::Internal("node input type mismatch".to_string()))?;
        let output_acquire = match ctx.output_acquire {
            Some(acquire) => Some(
                Arc::downcast::<Arc<AcquireFn<S::Output>>>(acquire)
                    .map_err(|_| {
                        NodeFailure::Internal("node output factory type mismatch".to_string())
                    })?
                    .as_ref()
                    .clone(),
            ),
            None => None,
        };
        let mut ctx = NodeContext {
            output: ctx.output,
            output_stats: ctx.output_stats,
            output_acquire,
            shutdown: ctx.shutdown,
            abort: ctx.abort,
            internal_failure: ctx.internal_failure,
            _marker: PhantomData,
        };
        self.stage
            .process(state, input, &mut ctx)
            .map_err(NodeFailure::Process)
    }

    fn cleanup_box(
        &self,
        state: Box<dyn Any + Send>,
    ) -> std::result::Result<(), NodeFailure<S::Error>> {
        let state = state
            .downcast::<S::State>()
            .map(|state| *state)
            .map_err(|_| NodeFailure::Internal("node cleanup state type mismatch".to_string()))?;
        self.stage.cleanup(state).map_err(NodeFailure::Cleanup)
    }
}

struct MergeNodeAdapter<S, Merge>
where
    S: Node,
    Merge: Fn(&mut S::State, S::State) -> std::result::Result<(), S::Error>
        + Send
        + Sync
        + 'static,
{
    stage: S,
    merge: Merge,
}

impl<S, Merge> DynNode<S::Error> for MergeNodeAdapter<S, Merge>
where
    S: Node,
    Merge: Fn(&mut S::State, S::State) -> std::result::Result<(), S::Error>
        + Send
        + Sync
        + 'static,
{
    fn init_box(&self) -> std::result::Result<Box<dyn Any + Send>, NodeFailure<S::Error>> {
        self.stage
            .init()
            .map(|state| Box::new(state) as Box<dyn Any + Send>)
            .map_err(NodeFailure::Init)
    }

    fn process_box(
        &self,
        state: &mut dyn Any,
        input: Message,
        ctx: RuntimeNodeContext,
    ) -> std::result::Result<(), NodeFailure<S::Error>> {
        let state = state
            .downcast_mut::<S::State>()
            .ok_or_else(|| NodeFailure::Internal("node state type mismatch".to_string()))?;
        let input = input
            .downcast::<S::Input>()
            .map(|input| *input)
            .map_err(|_| NodeFailure::Internal("node input type mismatch".to_string()))?;
        let output_acquire = match ctx.output_acquire {
            Some(acquire) => Some(
                Arc::downcast::<Arc<AcquireFn<S::Output>>>(acquire)
                    .map_err(|_| {
                        NodeFailure::Internal("node output factory type mismatch".to_string())
                    })?
                    .as_ref()
                    .clone(),
            ),
            None => None,
        };
        let mut ctx = NodeContext {
            output: ctx.output,
            output_stats: ctx.output_stats,
            output_acquire,
            shutdown: ctx.shutdown,
            abort: ctx.abort,
            internal_failure: ctx.internal_failure,
            _marker: PhantomData,
        };
        self.stage
            .process(state, input, &mut ctx)
            .map_err(NodeFailure::Process)
    }

    fn cleanup_box(
        &self,
        state: Box<dyn Any + Send>,
    ) -> std::result::Result<(), NodeFailure<S::Error>> {
        let state = state
            .downcast::<S::State>()
            .map(|state| *state)
            .map_err(|_| NodeFailure::Internal("node cleanup state type mismatch".to_string()))?;
        self.stage.cleanup(state).map_err(NodeFailure::Cleanup)
    }

    fn merge_box(
        &self,
        target: &mut dyn Any,
        source: Box<dyn Any + Send>,
    ) -> std::result::Result<(), NodeFailure<S::Error>> {
        let target = target
            .downcast_mut::<S::State>()
            .ok_or_else(|| NodeFailure::Internal("node merge target state type mismatch".to_string()))?;
        let source = source
            .downcast::<S::State>()
            .map(|source| *source)
            .map_err(|_| NodeFailure::Internal("node merge source state type mismatch".to_string()))?;
        (self.merge)(target, source).map_err(NodeFailure::Merge)
    }

    fn can_merge_state(&self) -> bool {
        true
    }
}

struct InlineNode<Init, Process, Cleanup, State, In, Out, E>
where
    Init: Fn() -> std::result::Result<State, E> + Send + Sync + 'static,
    Process: Fn(&mut State, In, &mut NodeContext<Out, E>) -> std::result::Result<(), E>
        + Send
        + Sync
        + 'static,
    Cleanup: Fn(State) -> std::result::Result<(), E> + Send + Sync + 'static,
    State: Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    init: Init,
    process: Process,
    cleanup: Cleanup,
    _marker: PhantomData<fn(State, In, Out, E)>,
}

impl<Init, Process, Cleanup, State, In, Out, E> Node
    for InlineNode<Init, Process, Cleanup, State, In, Out, E>
where
    Init: Fn() -> std::result::Result<State, E> + Send + Sync + 'static,
    Process: Fn(&mut State, In, &mut NodeContext<Out, E>) -> std::result::Result<(), E>
        + Send
        + Sync
        + 'static,
    Cleanup: Fn(State) -> std::result::Result<(), E> + Send + Sync + 'static,
    State: Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    type Input = In;
    type Output = Out;
    type Error = E;
    type State = State;

    fn init(&self) -> std::result::Result<Self::State, Self::Error> {
        (self.init)()
    }

    fn process(
        &self,
        state: &mut Self::State,
        input: Self::Input,
        ctx: &mut NodeContext<Self::Output, Self::Error>,
    ) -> std::result::Result<(), Self::Error> {
        (self.process)(state, input, ctx)
    }

    fn cleanup(&self, state: Self::State) -> std::result::Result<(), Self::Error> {
        (self.cleanup)(state)
    }
}

#[derive(Clone, Debug, Default)]
struct ThreadPolicyHints {
    fixed_threads: Option<usize>,
    max_threads: Option<usize>,
    initial_threads: Option<usize>,
    scale_policy: Option<NodeScalePolicy>,
}

pub struct NodeSpec<In, Out, E>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    name: String,
    stage: Arc<dyn DynNode<E>>,
    output_acquire_builder: Option<Arc<dyn OutputAcquireBuilder + Send + Sync>>,
    is_anchor: bool,
    thread_hints: ThreadPolicyHints,
    _marker: PhantomData<fn(In) -> Out>,
}

impl<In, Out, E> NodeSpec<In, Out, E>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    pub fn with_reusable_output<T, Factory>(mut self, factory: Factory) -> Self
    where
        Out: BufferLeaseOutput<T>,
        T: Recycle + Send + 'static,
        Factory: Fn() -> T + Send + Sync + 'static,
    {
        self.output_acquire_builder = Some(reusable_output_acquire_builder(factory));
        self
    }

    pub fn with_scale_policy(mut self, policy: NodeScalePolicy) -> Self {
        self.thread_hints.scale_policy = Some(policy);
        self
    }

    pub fn scalable_threads(mut self, initial_threads: usize, max_threads: usize) -> Self {
        self.thread_hints.initial_threads = Some(initial_threads);
        self.thread_hints.max_threads = Some(max_threads);
        self
    }

    pub fn max_threads(mut self, max_threads: usize) -> Self {
        self.thread_hints.max_threads = Some(max_threads);
        self
    }

    pub fn initial_threads(mut self, initial_threads: usize) -> Self {
        self.thread_hints.initial_threads = Some(initial_threads);
        self
    }

    pub fn fixed_threads(mut self, fixed_threads: usize) -> Self {
        self.thread_hints.fixed_threads = Some(fixed_threads);
        self
    }
}

pub trait BufferLeaseOutput<T> {}

impl<T> BufferLeaseOutput<T> for BufferLease<T> where T: Recycle + Send + 'static {}

pub trait IntoNodeSpec<E>
where
    E: Debug + Display + Send + 'static,
{
    type Input: Send + 'static;
    type Output: Send + 'static;

    fn into_node_spec(self) -> NodeSpec<Self::Input, Self::Output, E>;
}

impl<In, Out, E> IntoNodeSpec<E> for NodeSpec<In, Out, E>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    type Input = In;
    type Output = Out;

    fn into_node_spec(self) -> NodeSpec<In, Out, E> {
        self
    }
}

impl<S> IntoNodeSpec<S::Error> for S
where
    S: Node,
{
    type Input = S::Input;
    type Output = S::Output;

    fn into_node_spec(self) -> NodeSpec<S::Input, S::Output, S::Error> {
        node(default_node_name::<S>(), self)
    }
}

pub trait NodeExt: Node + Sized {
    fn with_reusable_output<T, Factory>(
        self,
        factory: Factory,
    ) -> NodeSpec<Self::Input, Self::Output, Self::Error>
    where
        Self::Output: BufferLeaseOutput<T>,
        T: Recycle + Send + 'static,
        Factory: Fn() -> T + Send + Sync + 'static,
    {
        node(default_node_name::<Self>(), self).with_reusable_output(factory)
    }
}

impl<S> NodeExt for S where S: Node {}

pub fn node<S>(name: impl Into<String>, stage_impl: S) -> NodeSpec<S::Input, S::Output, S::Error>
where
    S: Node,
{
    NodeSpec {
        name: name.into(),
        stage: Arc::new(NodeAdapter { stage: stage_impl }),
        output_acquire_builder: None,
        is_anchor: false,
        thread_hints: ThreadPolicyHints::default(),
        _marker: PhantomData,
    }
}

pub fn node_with_state_merge<S, Merge>(
    name: impl Into<String>,
    stage_impl: S,
    merge: Merge,
) -> NodeSpec<S::Input, S::Output, S::Error>
where
    S: Node,
    Merge: Fn(&mut S::State, S::State) -> std::result::Result<(), S::Error>
        + Send
        + Sync
        + 'static,
{
    NodeSpec {
        name: name.into(),
        stage: Arc::new(MergeNodeAdapter {
            stage: stage_impl,
            merge,
        }),
        output_acquire_builder: None,
        is_anchor: false,
        thread_hints: ThreadPolicyHints::default(),
        _marker: PhantomData,
    }
}

pub fn anchor<S, E>(node_like: S) -> NodeSpec<S::Input, S::Output, E>
where
    S: IntoNodeSpec<E>,
    E: Debug + Display + Send + 'static,
{
    let mut spec = node_like.into_node_spec();
    spec.is_anchor = true;
    spec
}

fn default_node_name<S>() -> String {
    std::any::type_name::<S>()
        .rsplit("::")
        .next()
        .unwrap_or("node")
        .to_string()
}

pub struct InlineNodeBuilder<Init, Process, Cleanup, State, In, Out, E>
where
    Init: Fn() -> std::result::Result<State, E> + Send + Sync + 'static,
    Process: Fn(&mut State, In, &mut NodeContext<Out, E>) -> std::result::Result<(), E>
        + Send
        + Sync
        + 'static,
    Cleanup: Fn(State) -> std::result::Result<(), E> + Send + Sync + 'static,
    State: Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    name: String,
    init: Init,
    process: Process,
    cleanup: Cleanup,
    _marker: PhantomData<fn(State, In, Out, E)>,
}

impl<Init, Process, Cleanup, State, In, Out, E>
    InlineNodeBuilder<Init, Process, Cleanup, State, In, Out, E>
where
    Init: Fn() -> std::result::Result<State, E> + Send + Sync + 'static,
    Process: Fn(&mut State, In, &mut NodeContext<Out, E>) -> std::result::Result<(), E>
        + Send
        + Sync
        + 'static,
    Cleanup: Fn(State) -> std::result::Result<(), E> + Send + Sync + 'static,
    State: Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    pub fn with_cleanup<NextCleanup>(
        self,
        cleanup: NextCleanup,
    ) -> InlineNodeBuilder<Init, Process, NextCleanup, State, In, Out, E>
    where
        NextCleanup: Fn(State) -> std::result::Result<(), E> + Send + Sync + 'static,
    {
        InlineNodeBuilder {
            name: self.name,
            init: self.init,
            process: self.process,
            cleanup,
            _marker: PhantomData,
        }
    }

    pub fn with_reusable_output<T, Factory>(self, factory: Factory) -> NodeSpec<In, Out, E>
    where
        Out: BufferLeaseOutput<T>,
        T: Recycle + Send + 'static,
        Factory: Fn() -> T + Send + Sync + 'static,
    {
        self.into_node_spec().with_reusable_output(factory)
    }
}

impl<Init, Process, Cleanup, State, In, Out, E> IntoNodeSpec<E>
    for InlineNodeBuilder<Init, Process, Cleanup, State, In, Out, E>
where
    Init: Fn() -> std::result::Result<State, E> + Send + Sync + 'static,
    Process: Fn(&mut State, In, &mut NodeContext<Out, E>) -> std::result::Result<(), E>
        + Send
        + Sync
        + 'static,
    Cleanup: Fn(State) -> std::result::Result<(), E> + Send + Sync + 'static,
    State: Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    type Input = In;
    type Output = Out;

    fn into_node_spec(self) -> NodeSpec<In, Out, E> {
        node(
            self.name,
            InlineNode {
                init: self.init,
                process: self.process,
                cleanup: self.cleanup,
                _marker: PhantomData::<fn(State, In, Out, E)>,
            },
        )
    }
}

fn default_inline_cleanup<State, E>(_state: State) -> std::result::Result<(), E> {
    Ok(())
}

pub fn inline_node<Init, Process, State, In, Out, E>(
    name: impl Into<String>,
    init: Init,
    process: Process,
) -> InlineNodeBuilder<Init, Process, fn(State) -> std::result::Result<(), E>, State, In, Out, E>
where
    Init: Fn() -> std::result::Result<State, E> + Send + Sync + 'static,
    Process: Fn(&mut State, In, &mut NodeContext<Out, E>) -> std::result::Result<(), E>
        + Send
        + Sync
        + 'static,
    State: Send + 'static,
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    InlineNodeBuilder {
        name: name.into(),
        init,
        process,
        cleanup: default_inline_cleanup::<State, E>,
        _marker: PhantomData,
    }
}

trait OutputAcquireBuilder {
    fn build(&self, runtime: LeaseRuntime) -> DynAcquire;
}

fn reusable_output_acquire_builder<T, Factory>(
    factory: Factory,
) -> Arc<dyn OutputAcquireBuilder + Send + Sync>
where
    T: Recycle + Send + 'static,
    Factory: Fn() -> T + Send + Sync + 'static,
{
    Arc::new(RecycleAcquireBuilder {
        factory: Arc::new(factory),
        _marker: PhantomData::<fn() -> T>,
    })
}

struct RecycleAcquireBuilder<T>
where
    T: Recycle + Send + 'static,
{
    factory: Arc<dyn Fn() -> T + Send + Sync>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> OutputAcquireBuilder for RecycleAcquireBuilder<T>
where
    T: Recycle + Send + 'static,
{
    fn build(&self, runtime: LeaseRuntime) -> DynAcquire {
        let (recycle_sender, recycle_receiver) = channel::unbounded::<T>();
        let factory = Arc::clone(&self.factory);
        let acquire: Arc<AcquireFn<BufferLease<T>>> = Arc::new(move || {
            let value = match recycle_receiver.try_recv() {
                Ok(value) => value,
                Err(_) => factory(),
            };
            BufferLease::new(value, recycle_sender.clone(), runtime.clone())
        });
        Arc::new(acquire)
    }
}

#[derive(Debug)]
enum NodeFailure<E> {
    Init(E),
    Process(E),
    Cleanup(E),
    Merge(E),
    Internal(String),
}

enum InternalFailure {
    Internal { message: String },
    Telemetry { message: String },
    External { node: String, error: Message },
}

impl InternalFailure {
    fn internal(message: impl Into<String>) -> Self {
        InternalFailure::Internal {
            message: message.into(),
        }
    }

    fn telemetry(message: impl Into<String>) -> Self {
        InternalFailure::Telemetry {
            message: message.into(),
        }
    }

    fn external<E>(node: String, error: E) -> Self
    where
        E: Debug + Display + Send + 'static,
    {
        InternalFailure::External {
            node,
            error: Box::new(error),
        }
    }
}

struct UntypedExternalNode {
    name: String,
    input: LinkReceiver,
    input_stats: Arc<LinkStats>,
    output: LinkSender,
    output_stats: Arc<LinkStats>,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    internal_failure: channel::Sender<InternalFailure>,
    output_acquire: Option<DynAcquire>,
}

impl UntypedExternalNode {
    fn into_typed<In, Out, E>(self) -> ExternalNode<In, Out, E>
    where
        In: Send + 'static,
        Out: Send + 'static,
        E: Debug + Display + Send + 'static,
    {
        let output_acquire = match self.output_acquire {
            Some(acquire) => Some(
                Arc::downcast::<Arc<AcquireFn<Out>>>(acquire)
                    .unwrap_or_else(|_| panic!("external node output factory type mismatch"))
                    .as_ref()
                    .clone(),
            ),
            None => None,
        };
        ExternalNode {
            name: self.name,
            input: self.input,
            input_stats: self.input_stats,
            output: self.output,
            output_stats: self.output_stats,
            shutdown: self.shutdown,
            abort: self.abort,
            internal_failure: self.internal_failure,
            output_acquire,
            _marker: PhantomData,
        }
    }
}

#[cfg(feature = "feeder")]
#[derive(Debug, Clone)]
pub struct FeederLinkConfig {
    pub scheduler_threads: NonZeroUsize,
    pub water_low: NonZeroUsize,
    pub water_high: NonZeroUsize,
}

#[cfg(feature = "feeder")]
impl FeederLinkConfig {
    pub fn new(
        scheduler_threads: NonZeroUsize,
        water_low: NonZeroUsize,
        water_high: NonZeroUsize,
    ) -> Self {
        Self {
            scheduler_threads,
            water_low,
            water_high,
        }
    }
}

#[cfg(feature = "feeder")]
impl Default for FeederLinkConfig {
    fn default() -> Self {
        Self {
            scheduler_threads: NonZeroUsize::new(4).unwrap(),
            water_low: NonZeroUsize::new(16).unwrap(),
            water_high: NonZeroUsize::new(64).unwrap(),
        }
    }
}

enum RecvPoll {
    Item(Message),
    Timeout,
    Disconnected,
}

enum LinkRecvError {
    Timeout,
    Closed,
}

enum LinkTryRecvError {
    Empty,
    Closed,
}

#[derive(Clone)]
struct LinkSender {
    inner: LinkSenderInner,
}

#[derive(Clone)]
enum LinkSenderInner {
    Standard(channel::Sender<Message>),
    #[cfg(feature = "feeder")]
    Feeder(feeder::FeederTx<Message>),
}

impl LinkSender {
    fn send(&self, message: Message) -> std::result::Result<(), ()> {
        match &self.inner {
            LinkSenderInner::Standard(sender) => sender.send(message).map_err(|_| ()),
            #[cfg(feature = "feeder")]
            LinkSenderInner::Feeder(tx) => tx.send(message).map_err(|_| ()),
        }
    }

    fn is_closed(&self) -> bool {
        match &self.inner {
            LinkSenderInner::Standard(sender) => sender.is_closed(),
            #[cfg(feature = "feeder")]
            LinkSenderInner::Feeder(_) => false,
        }
    }
}

#[cfg(feature = "feeder")]
struct FeederLinkReceiver {
    feeder: Arc<feeder::Feeder<Message>>,
    low: NonZeroUsize,
    high: NonZeroUsize,
    rx: Arc<feeder::FeederRx<Message>>,
    closed: Arc<AtomicBool>,
}

#[cfg(feature = "feeder")]
impl FeederLinkReceiver {
    fn new(feeder: Arc<feeder::Feeder<Message>>, low: NonZeroUsize, high: NonZeroUsize) -> Self {
        let rx = feeder
            .rx(low, high)
            .expect("feeder consumer handle unavailable");
        Self {
            feeder,
            low,
            high,
            rx: Arc::new(rx),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn fork(&self) -> Self {
        Self::new(Arc::clone(&self.feeder), self.low, self.high)
    }

    fn clone_shared(&self) -> Self {
        Self {
            feeder: Arc::clone(&self.feeder),
            low: self.low,
            high: self.high,
            rx: Arc::clone(&self.rx),
            closed: Arc::clone(&self.closed),
        }
    }

    fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn poll_try_get_one(&self) -> feeder::TryGetOne<Message> {
        let result = self.rx.try_get_one();
        if matches!(
            result,
            feeder::TryGetOne::NoMoreWork | feeder::TryGetOne::Cancelled
        ) {
            self.mark_closed();
        }
        result
    }
}

struct LinkReceiver {
    inner: LinkReceiverInner,
}

impl Clone for LinkReceiver {
    fn clone(&self) -> Self {
        match &self.inner {
            LinkReceiverInner::Standard(receiver) => LinkReceiver {
                inner: LinkReceiverInner::Standard(receiver.clone()),
            },
            #[cfg(feature = "feeder")]
            LinkReceiverInner::Feeder(input) => LinkReceiver {
                inner: LinkReceiverInner::Feeder(input.clone_shared()),
            },
        }
    }
}

enum LinkReceiverInner {
    Standard(channel::Receiver<Message>),
    #[cfg(feature = "feeder")]
    Feeder(FeederLinkReceiver),
}

impl LinkReceiver {
    fn clone_for_external(&self) -> Self {
        match &self.inner {
            LinkReceiverInner::Standard(receiver) => LinkReceiver {
                inner: LinkReceiverInner::Standard(receiver.clone()),
            },
            #[cfg(feature = "feeder")]
            LinkReceiverInner::Feeder(input) => LinkReceiver {
                inner: LinkReceiverInner::Feeder(input.fork()),
            },
        }
    }

    fn recv(&self) -> std::result::Result<Message, LinkRecvError> {
        match &self.inner {
            LinkReceiverInner::Standard(receiver) => {
                receiver.recv().map_err(|_| LinkRecvError::Closed)
            }
            #[cfg(feature = "feeder")]
            LinkReceiverInner::Feeder(input) => input.rx.get_one().map_err(|_| {
                input.mark_closed();
                LinkRecvError::Closed
            }),
        }
    }

    fn recv_timeout(&self, duration: Duration) -> std::result::Result<Message, LinkRecvError> {
        match &self.inner {
            LinkReceiverInner::Standard(receiver) => match receiver.recv_timeout(duration) {
                Ok(message) => Ok(message),
                Err(channel::RecvTimeoutError::Timeout) => Err(LinkRecvError::Timeout),
                Err(channel::RecvTimeoutError::Closed) => Err(LinkRecvError::Closed),
            },
            #[cfg(feature = "feeder")]
            LinkReceiverInner::Feeder(input) => {
                if input.is_closed() {
                    return Err(LinkRecvError::Closed);
                }
                let deadline = Instant::now() + duration;
                loop {
                    match input.poll_try_get_one() {
                        feeder::TryGetOne::Item(item) => return Ok(item),
                        feeder::TryGetOne::Empty | feeder::TryGetOne::InShutdown => {
                            if Instant::now() >= deadline {
                                return Err(LinkRecvError::Timeout);
                            }
                            let remaining = deadline.saturating_duration_since(Instant::now());
                            thread::sleep(Duration::from_millis(1).min(remaining));
                        }
                        feeder::TryGetOne::NoMoreWork | feeder::TryGetOne::Cancelled => {
                            return Err(LinkRecvError::Closed);
                        }
                    }
                }
            }
        }
    }

    fn try_recv(&self) -> std::result::Result<Message, LinkTryRecvError> {
        match &self.inner {
            LinkReceiverInner::Standard(receiver) => match receiver.try_recv() {
                Ok(message) => Ok(message),
                Err(channel::TryRecvError::Empty) => Err(LinkTryRecvError::Empty),
                Err(channel::TryRecvError::Closed) => Err(LinkTryRecvError::Closed),
            },
            #[cfg(feature = "feeder")]
            LinkReceiverInner::Feeder(input) => match input.poll_try_get_one() {
                feeder::TryGetOne::Item(item) => Ok(item),
                feeder::TryGetOne::Empty | feeder::TryGetOne::InShutdown => {
                    Err(LinkTryRecvError::Empty)
                }
                feeder::TryGetOne::NoMoreWork | feeder::TryGetOne::Cancelled => {
                    Err(LinkTryRecvError::Closed)
                }
            },
        }
    }

    fn recv_poll(&self, timeout: Duration) -> RecvPoll {
        #[cfg(feature = "feeder")]
        if let LinkReceiverInner::Feeder(input) = &self.inner {
            if input.is_closed() {
                return RecvPoll::Disconnected;
            }
            let deadline = Instant::now() + timeout;
            loop {
                match input.poll_try_get_one() {
                    feeder::TryGetOne::Item(item) => return RecvPoll::Item(item),
                    feeder::TryGetOne::NoMoreWork | feeder::TryGetOne::Cancelled => {
                        return RecvPoll::Disconnected;
                    }
                    feeder::TryGetOne::Empty | feeder::TryGetOne::InShutdown => {
                        if Instant::now() >= deadline {
                            return RecvPoll::Timeout;
                        }
                        thread::sleep(
                            Duration::from_millis(1)
                                .min(deadline.saturating_duration_since(Instant::now())),
                        );
                    }
                }
            }
        }
        match self.recv_timeout(timeout) {
            Ok(message) => RecvPoll::Item(message),
            Err(LinkRecvError::Timeout) => RecvPoll::Timeout,
            Err(LinkRecvError::Closed) => RecvPoll::Disconnected,
        }
    }

    fn is_terminated(&self) -> bool {
        match &self.inner {
            LinkReceiverInner::Standard(receiver) => receiver.is_terminated(),
            #[cfg(feature = "feeder")]
            LinkReceiverInner::Feeder(input) => input.is_closed(),
        }
    }

    fn should_stop_on_shutdown_timeout(&self, is_input_stage: bool) -> bool {
        match &self.inner {
            LinkReceiverInner::Standard(receiver) => is_input_stage && receiver.is_empty(),
            #[cfg(feature = "feeder")]
            LinkReceiverInner::Feeder(_) => true,
        }
    }
}

enum LinkKind {
    Standard {
        sender: Option<channel::Sender<Message>>,
        receiver: channel::Receiver<Message>,
    },
    #[cfg(feature = "feeder")]
    Feeder {
        feeder: Arc<feeder::Feeder<Message>>,
        low: NonZeroUsize,
        high: NonZeroUsize,
    },
}

struct Link {
    stats: Arc<LinkStats>,
    kind: LinkKind,
}

impl Link {
    fn make_output(&self) -> LinkSender {
        match &self.kind {
            LinkKind::Standard { sender, .. } => LinkSender {
                inner: LinkSenderInner::Standard(
                    sender.as_ref().expect("standard sender exists").clone(),
                ),
            },
            #[cfg(feature = "feeder")]
            LinkKind::Feeder { feeder, .. } => LinkSender {
                inner: LinkSenderInner::Feeder(
                    feeder.tx().expect("feeder producer handle unavailable"),
                ),
            },
        }
    }

    fn make_input(&self) -> LinkReceiver {
        match &self.kind {
            LinkKind::Standard { receiver, .. } => LinkReceiver {
                inner: LinkReceiverInner::Standard(receiver.clone()),
            },
            #[cfg(feature = "feeder")]
            LinkKind::Feeder {
                feeder, low, high, ..
            } => LinkReceiver {
                inner: LinkReceiverInner::Feeder(FeederLinkReceiver::new(
                    Arc::clone(feeder),
                    *low,
                    *high,
                )),
            },
        }
    }

    fn close(&mut self) {
        match &mut self.kind {
            LinkKind::Standard { sender, .. } => {
                sender.take();
            }
            #[cfg(feature = "feeder")]
            LinkKind::Feeder { feeder, .. } => {
                feeder.graceful_shutdown();
            }
        }
    }

    fn abort(&mut self) {
        match &mut self.kind {
            LinkKind::Standard { sender, .. } => {
                sender.take();
            }
            #[cfg(feature = "feeder")]
            LinkKind::Feeder { feeder, .. } => {
                feeder.cancel();
            }
        }
    }

    fn queue_len(&self) -> Option<usize> {
        match &self.kind {
            LinkKind::Standard { receiver, .. } => Some(receiver.len()),
            #[cfg(feature = "feeder")]
            LinkKind::Feeder { .. } => None,
        }
    }
}

#[derive(Default)]
struct LinkStats {
    arrivals: AtomicU64,
    drains: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
pub struct SingleThreadWeightedBranchConfig {
    pub target_queue_seconds: f64,
    pub ewma_half_life: Duration,
}

impl Default for SingleThreadWeightedBranchConfig {
    fn default() -> Self {
        Self {
            target_queue_seconds: 4.0,
            ewma_half_life: Duration::from_secs(1),
        }
    }
}

pub struct WeightedBranchLinks<T>
where
    T: Send + 'static,
{
    pub left: GraphLink<T>,
    pub right: GraphLink<T>,
}

pub struct GraphLink<T>
where
    T: Send + 'static,
{
    index: usize,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for GraphLink<T>
where
    T: Send + 'static,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for GraphLink<T> where T: Send + 'static {}

impl<T> GraphLink<T>
where
    T: Send + 'static,
{
    pub fn index(self) -> usize {
        self.index
    }
}

pub struct PipelineGraph<In, Out, E = String>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    input_link: usize,
    output_link: usize,
    link_count: usize,
    nodes: Vec<GraphNodeSpec<E>>,
    external_count: usize,
    state_return: Option<StateReturnConfig>,
    #[cfg(feature = "feeder")]
    feeder_links: HashMap<usize, FeederLinkConfig>,
    _marker: PhantomData<fn(In) -> Out>,
}

pub struct PipelineGraphWithState<In, Out, State, E = String, const MERGE: bool = false>
where
    In: Send + 'static,
    Out: Send + 'static,
    State: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    graph: PipelineGraph<In, Out, E>,
    _marker: PhantomData<fn() -> State>,
}

#[derive(Clone, Copy)]
struct StateReturnConfig {
    require_merge: bool,
}

pub struct PipelineGraphBuilder<In, E = String>
where
    In: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    link_count: usize,
    nodes: Vec<GraphNodeSpec<E>>,
    external_count: usize,
    #[cfg(feature = "feeder")]
    feeder_links: HashMap<usize, FeederLinkConfig>,
    _marker: PhantomData<fn(In)>,
}

impl<In, E> PipelineGraphBuilder<In, E>
where
    In: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            link_count: 1,
            nodes: Vec::new(),
            external_count: 0,
            #[cfg(feature = "feeder")]
            feeder_links: HashMap::new(),
            _marker: PhantomData,
        }
    }

    pub fn input(&self) -> GraphLink<In> {
        GraphLink {
            index: 0,
            _marker: PhantomData,
        }
    }

    pub fn link<T>(&mut self) -> GraphLink<T>
    where
        T: Send + 'static,
    {
        let index = self.link_count;
        self.link_count += 1;
        GraphLink {
            index,
            _marker: PhantomData,
        }
    }

    pub fn add_node<S>(
        &mut self,
        input: GraphLink<S::Input>,
        stage_like: S,
    ) -> GraphLink<S::Output>
    where
        S: IntoNodeSpec<E>,
    {
        let output = self.link();
        self.add_node_to(input, stage_like, output);
        output
    }

    pub fn add_node_to<S>(
        &mut self,
        input: GraphLink<S::Input>,
        stage_like: S,
        output: GraphLink<S::Output>,
    ) where
        S: IntoNodeSpec<E>,
    {
        let stage = stage_like.into_node_spec();
        self.nodes.push(GraphNodeSpec {
            name: stage.name,
            stage: Some(stage.stage),
            output_acquire_builder: stage.output_acquire_builder,
            is_anchor: stage.is_anchor,
            thread_hints: stage.thread_hints,
            input_link: input.index,
            output_links: vec![output.index],
            weighted_branch_config: None,
            external_index: None,
        });
    }

    pub fn add_single_thread_weighted_branch<T>(
        &mut self,
        input: GraphLink<T>,
        name: impl Into<String>,
        config: SingleThreadWeightedBranchConfig,
    ) -> WeightedBranchLinks<T>
    where
        T: Send + 'static,
    {
        let left = self.link();
        let right = self.link();
        self.add_single_thread_weighted_branch_to(input, name, left, right, config);
        WeightedBranchLinks { left, right }
    }

    pub fn add_single_thread_weighted_branch_to<T>(
        &mut self,
        input: GraphLink<T>,
        name: impl Into<String>,
        left: GraphLink<T>,
        right: GraphLink<T>,
        config: SingleThreadWeightedBranchConfig,
    ) where
        T: Send + 'static,
    {
        self.nodes.push(GraphNodeSpec {
            name: name.into(),
            stage: None,
            output_acquire_builder: None,
            is_anchor: false,
            thread_hints: ThreadPolicyHints::default(),
            input_link: input.index,
            output_links: vec![left.index, right.index],
            weighted_branch_config: Some(config),
            external_index: None,
        });
    }

    pub fn add_external_node<ExtIn, ExtOut>(
        &mut self,
        input: GraphLink<ExtIn>,
        name: impl Into<String>,
    ) -> (GraphLink<ExtOut>, ExternalNodeToken<ExtIn, ExtOut>)
    where
        ExtIn: Send + 'static,
        ExtOut: Send + 'static,
    {
        let output = self.link();
        let token = self.add_external_node_to(input, name, output);
        (output, token)
    }

    pub fn add_external_node_to<ExtIn, ExtOut>(
        &mut self,
        input: GraphLink<ExtIn>,
        name: impl Into<String>,
        output: GraphLink<ExtOut>,
    ) -> ExternalNodeToken<ExtIn, ExtOut>
    where
        ExtIn: Send + 'static,
        ExtOut: Send + 'static,
    {
        let token = ExternalNodeToken {
            index: self.external_count,
            _marker: PhantomData,
        };
        self.external_count += 1;
        self.nodes.push(GraphNodeSpec {
            name: name.into(),
            stage: None,
            output_acquire_builder: None,
            is_anchor: false,
            thread_hints: ThreadPolicyHints::default(),
            input_link: input.index,
            output_links: vec![output.index],
            weighted_branch_config: None,
            external_index: Some(token.index),
        });
        token
    }

    pub fn add_external_node_with_reusable_output<ExtIn, ExtOut, T, Factory>(
        &mut self,
        input: GraphLink<ExtIn>,
        name: impl Into<String>,
        factory: Factory,
    ) -> (GraphLink<ExtOut>, ExternalNodeToken<ExtIn, ExtOut>)
    where
        ExtIn: Send + 'static,
        ExtOut: Send + BufferLeaseOutput<T> + 'static,
        T: Recycle + Send + 'static,
        Factory: Fn() -> T + Send + Sync + 'static,
    {
        let output = self.link();
        let token =
            self.add_external_node_to_with_reusable_output(input, name, output, factory);
        (output, token)
    }

    pub fn add_external_node_to_with_reusable_output<ExtIn, ExtOut, T, Factory>(
        &mut self,
        input: GraphLink<ExtIn>,
        name: impl Into<String>,
        output: GraphLink<ExtOut>,
        factory: Factory,
    ) -> ExternalNodeToken<ExtIn, ExtOut>
    where
        ExtIn: Send + 'static,
        ExtOut: Send + BufferLeaseOutput<T> + 'static,
        T: Recycle + Send + 'static,
        Factory: Fn() -> T + Send + Sync + 'static,
    {
        let token = ExternalNodeToken {
            index: self.external_count,
            _marker: PhantomData,
        };
        self.external_count += 1;
        self.nodes.push(GraphNodeSpec {
            name: name.into(),
            stage: None,
            output_acquire_builder: Some(reusable_output_acquire_builder(factory)),
            is_anchor: false,
            thread_hints: ThreadPolicyHints::default(),
            input_link: input.index,
            output_links: vec![output.index],
            weighted_branch_config: None,
            external_index: Some(token.index),
        });
        token
    }

    #[cfg(feature = "feeder")]
    pub fn feeder_link<T>(&mut self, link: GraphLink<T>, config: FeederLinkConfig)
    where
        T: Send + 'static,
    {
        self.feeder_links.insert(link.index(), config);
    }

    pub fn finish<Out>(self, output: GraphLink<Out>) -> PipelineGraph<In, Out, E>
    where
        Out: Send + 'static,
    {
        PipelineGraph {
            input_link: 0,
            output_link: output.index,
            link_count: self.link_count.max(output.index + 1),
            nodes: self.nodes,
            external_count: self.external_count,
            state_return: None,
            #[cfg(feature = "feeder")]
            feeder_links: self.feeder_links,
            _marker: PhantomData,
        }
    }

    pub fn finish_with_state<Out, State>(
        self,
        output: GraphLink<Out>,
    ) -> PipelineGraphWithState<In, Out, State, E, false>
    where
        Out: Send + 'static,
        State: Send + 'static,
    {
        let mut graph = self.finish(output);
        graph.state_return = Some(StateReturnConfig {
            require_merge: false,
        });
        PipelineGraphWithState {
            graph,
            _marker: PhantomData,
        }
    }

    pub fn finish_with_merged_state<Out, State>(
        self,
        output: GraphLink<Out>,
    ) -> PipelineGraphWithState<In, Out, State, E, true>
    where
        Out: Send + 'static,
        State: Send + 'static,
    {
        let mut graph = self.finish(output);
        graph.state_return = Some(StateReturnConfig {
            require_merge: true,
        });
        PipelineGraphWithState {
            graph,
            _marker: PhantomData,
        }
    }
}

impl<In, E> Default for PipelineGraphBuilder<In, E>
where
    In: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

struct GraphNodeSpec<E>
where
    E: Debug + Display + Send + 'static,
{
    name: String,
    stage: Option<Arc<dyn DynNode<E>>>,
    output_acquire_builder: Option<Arc<dyn OutputAcquireBuilder + Send + Sync>>,
    is_anchor: bool,
    thread_hints: ThreadPolicyHints,
    input_link: usize,
    output_links: Vec<usize>,
    weighted_branch_config: Option<SingleThreadWeightedBranchConfig>,
    external_index: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ResolvedThreadPolicy {
    Fixed(usize),
    Scalable(ResolvedScalePolicy),
    ImplicitSupport,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct ResolvedScalePolicy {
    initial_threads: usize,
    max_threads: usize,
    target_queue_seconds: f64,
    low_queue_seconds: f64,
    scale_down_after: Duration,
    underutilized_busy_ratio: f64,
}

fn thread_policy_kind(policy: ResolvedThreadPolicy) -> NodeThreadPolicyKind {
    match policy {
        ResolvedThreadPolicy::Fixed(_) => NodeThreadPolicyKind::Fixed,
        ResolvedThreadPolicy::Scalable(_) => NodeThreadPolicyKind::Scalable,
        ResolvedThreadPolicy::ImplicitSupport => NodeThreadPolicyKind::ImplicitSupport,
    }
}

fn fixed_thread_count(policy: ResolvedThreadPolicy) -> Option<usize> {
    match policy {
        ResolvedThreadPolicy::Fixed(count) => Some(count),
        _ => None,
    }
}

fn max_thread_count(policy: ResolvedThreadPolicy) -> Option<usize> {
    match policy {
        ResolvedThreadPolicy::Scalable(policy) => Some(policy.max_threads),
        _ => None,
    }
}

fn target_queue_seconds(policy: ResolvedThreadPolicy) -> Option<f64> {
    match policy {
        ResolvedThreadPolicy::Scalable(policy) => Some(policy.target_queue_seconds),
        _ => None,
    }
}

fn low_queue_seconds(policy: ResolvedThreadPolicy) -> Option<f64> {
    match policy {
        ResolvedThreadPolicy::Scalable(policy) => Some(policy.low_queue_seconds),
        _ => None,
    }
}

#[derive(Clone)]
struct RuntimeNode<E>
where
    E: Debug + Display + Send + 'static,
{
    name: String,
    stage: Option<Arc<dyn DynNode<E>>>,
    output_acquire: Option<DynAcquire>,
    is_anchor: bool,
    thread_policy: ResolvedThreadPolicy,
    input_link: usize,
    output_links: Vec<usize>,
    weighted_branch_config: Option<SingleThreadWeightedBranchConfig>,
    is_external: bool,
    external_index: Option<usize>,
    return_state_on_exit: bool,
}

enum WorkerCommand<E>
where
    E: Debug + Display + Send + 'static,
{
    Run(WorkerAssignment<E>),
    RunWeightedBranch(WeightedBranchAssignment),
    Stop,
}

struct WorkerAssignment<E>
where
    E: Debug + Display + Send + 'static,
{
    node_index: usize,
    stage: Arc<dyn DynNode<E>>,
    initial_state: Option<Message>,
    input: LinkReceiver,
    input_stats: Arc<LinkStats>,
    output: LinkSender,
    output_stats: Arc<LinkStats>,
    output_acquire: Option<DynAcquire>,
    is_input_stage: bool,
    retire: Arc<RetireControl>,
    merge_pending: Arc<AtomicBool>,
    merge_requests: channel::Receiver<MergeRequest>,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    poll_interval: Duration,
    internal_failure: channel::Sender<InternalFailure>,
    stats: Arc<WorkerStats>,
    return_state_on_exit: bool,
}

struct MergeRequest {
    source_worker_id: usize,
    state_receiver: channel::Receiver<Message>,
}

struct WeightedBranchAssignment {
    node_index: usize,
    input: LinkReceiver,
    input_stats: Arc<LinkStats>,
    left_output: LinkSender,
    left_output_stats: Arc<LinkStats>,
    right_output: LinkSender,
    right_output_stats: Arc<LinkStats>,
    left_queue_len: Arc<AtomicUsize>,
    right_queue_len: Arc<AtomicUsize>,
    controller: WeightedBranchController,
    sample_interval: Duration,
    is_input_stage: bool,
    retire: Arc<RetireControl>,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    poll_interval: Duration,
    internal_failure: channel::Sender<InternalFailure>,
    stats: Arc<WorkerStats>,
}

#[derive(Clone, Copy, Debug)]
struct WeightedBranchArmState {
    last_arrivals: u64,
    last_drains: u64,
    drain_rate_ewma: f64,
    net_rate_ewma: f64,
    smoothed_len: f64,
    ready: bool,
}

impl Default for WeightedBranchArmState {
    fn default() -> Self {
        Self {
            last_arrivals: 0,
            last_drains: 0,
            drain_rate_ewma: 0.0,
            net_rate_ewma: 0.0,
            smoothed_len: 0.0,
            ready: false,
        }
    }
}

struct WeightedBranchController {
    target_queue_seconds: f64,
    ewma_half_life_secs: f64,
    left: WeightedBranchArmState,
    right: WeightedBranchArmState,
    next_alternate_left: bool,
}

impl WeightedBranchController {
    fn new(config: SingleThreadWeightedBranchConfig) -> Self {
        Self {
            target_queue_seconds: config.target_queue_seconds,
            ewma_half_life_secs: config.ewma_half_life.as_secs_f64().max(0.000_001),
            left: WeightedBranchArmState::default(),
            right: WeightedBranchArmState::default(),
            next_alternate_left: true,
        }
    }

    fn sample_arms(
        &mut self,
        elapsed_secs: f64,
        left_stats: &LinkStats,
        right_stats: &LinkStats,
        left_queue_len: usize,
        right_queue_len: usize,
    ) {
        let alpha = 1.0 - 0.5_f64.powf(elapsed_secs / self.ewma_half_life_secs);
        Self::sample_arm(
            &mut self.left,
            left_stats,
            left_queue_len,
            elapsed_secs,
            alpha,
        );
        Self::sample_arm(
            &mut self.right,
            right_stats,
            right_queue_len,
            elapsed_secs,
            alpha,
        );
    }

    fn sample_arm(
        arm: &mut WeightedBranchArmState,
        stats: &LinkStats,
        queue_len: usize,
        elapsed_secs: f64,
        alpha: f64,
    ) {
        let arrivals = stats.arrivals.load(Ordering::Relaxed);
        let drains = stats.drains.load(Ordering::Relaxed);
        let delta_arrivals = arrivals.saturating_sub(arm.last_arrivals);
        let delta_drains = drains.saturating_sub(arm.last_drains);
        arm.last_arrivals = arrivals;
        arm.last_drains = drains;

        let arrival_rate = delta_arrivals as f64 / elapsed_secs;
        let drain_rate = delta_drains as f64 / elapsed_secs;
        let net_rate = arrival_rate - drain_rate;

        arm.drain_rate_ewma = ewma_alpha(arm.drain_rate_ewma, drain_rate, alpha);
        arm.net_rate_ewma = ewma_alpha(arm.net_rate_ewma, net_rate, alpha);
        arm.smoothed_len = ewma_alpha(arm.smoothed_len, queue_len as f64, alpha);

        if arm.smoothed_len >= 1.0 || arm.net_rate_ewma > 0.0 {
            arm.ready = true;
        }
    }

    fn arms_ready(&self) -> bool {
        self.left.ready && self.right.ready
    }

    fn choose_route(&mut self, left_queue_len: usize, right_queue_len: usize) -> bool {
        let left_len = Self::effective_queue_len(left_queue_len, self.left.smoothed_len);
        let right_len = Self::effective_queue_len(right_queue_len, self.right.smoothed_len);
        if !self.left.ready || !self.right.ready {
            let route_left = self.next_alternate_left;
            self.next_alternate_left = !self.next_alternate_left;
            return route_left;
        }

        let left_fill = Self::fill_ratio(
            left_len,
            self.left.drain_rate_ewma,
            self.target_queue_seconds,
        );
        let right_fill = Self::fill_ratio(
            right_len,
            self.right.drain_rate_ewma,
            self.target_queue_seconds,
        );

        if left_fill < right_fill {
            return true;
        }
        if right_fill < left_fill {
            return false;
        }

        let route_left = self.next_alternate_left;
        self.next_alternate_left = !self.next_alternate_left;
        route_left
    }

    fn effective_queue_len(observed_len: usize, smoothed_len: f64) -> usize {
        if observed_len == 0 {
            return 0;
        }

        let smoothed_len = if smoothed_len.is_finite() && smoothed_len > 0.0 {
            smoothed_len.ceil() as usize
        } else {
            0
        };

        observed_len.max(smoothed_len)
    }

    fn fill_ratio(queue_len: usize, drain_rate_ewma: f64, target_queue_seconds: f64) -> f64 {
        let virtual_capacity = Self::virtual_capacity(drain_rate_ewma, target_queue_seconds);
        queue_len as f64 / virtual_capacity.max(1) as f64
    }

    fn virtual_capacity(drain_rate_ewma: f64, target_queue_seconds: f64) -> usize {
        (drain_rate_ewma * target_queue_seconds).ceil().max(1.0) as usize
    }
}

fn ewma_alpha(previous: f64, sample: f64, alpha: f64) -> f64 {
    if previous == 0.0 {
        sample
    } else {
        (previous * (1.0 - alpha)) + (sample * alpha)
    }
}

fn validate_weighted_branch_config<E>(
    branch: &str,
    config: &SingleThreadWeightedBranchConfig,
) -> Result<(), E>
where
    E: Debug + Display + Send + 'static,
{
    if config.target_queue_seconds <= 0.0 || !config.target_queue_seconds.is_finite() {
        return Err(PiperError::InvalidWeightedBranchConfig {
            branch: branch.to_string(),
            message: "target_queue_seconds must be positive and finite".to_string(),
        });
    }
    if config.ewma_half_life == Duration::ZERO {
        return Err(PiperError::InvalidWeightedBranchConfig {
            branch: branch.to_string(),
            message: "ewma_half_life must be greater than zero".to_string(),
        });
    }
    Ok(())
}

enum WorkerEvent<E>
where
    E: Debug + Display + Send + 'static,
{
    Started {
        worker_id: usize,
    },
    Parked {
        worker_id: usize,
        node_index: usize,
        returned_state: Option<Message>,
    },
    MergeCompleted {
        worker_id: usize,
        node_index: usize,
        source_worker_id: usize,
    },
    Failed {
        worker_id: usize,
        node_index: usize,
        worker: String,
        failure: NodeFailure<E>,
    },
    Stopped,
}

struct WorkerSlot<E>
where
    E: Debug + Display + Send + 'static,
{
    name: String,
    command: channel::Sender<WorkerCommand<E>>,
    handle: Option<JoinHandle<()>>,
    active_node: Option<usize>,
    retire: Option<Arc<RetireControl>>,
    merge_pending: Option<Arc<AtomicBool>>,
    merge_sender: Option<channel::Sender<MergeRequest>>,
    stats: Arc<WorkerStats>,
}

struct RetireControl {
    requested: AtomicBool,
    transfer_sender: Mutex<Option<channel::Sender<Message>>>,
}

enum RetireAction {
    Cleanup,
    Transfer(channel::Sender<Message>),
}

impl RetireControl {
    fn new() -> Self {
        Self {
            requested: AtomicBool::new(false),
            transfer_sender: Mutex::new(None),
        }
    }

    fn request_cleanup(&self) {
        self.requested.store(true, Ordering::Release);
    }

    fn request_transfer(&self, sender: channel::Sender<Message>) {
        *self.transfer_sender.lock() = Some(sender);
        self.requested.store(true, Ordering::Release);
    }

    fn take_request(&self) -> Option<RetireAction> {
        if !self.requested.load(Ordering::Acquire) {
            return None;
        }
        let transfer = self.transfer_sender.lock().take();
        self.requested.store(false, Ordering::Release);
        Some(match transfer {
            Some(sender) => RetireAction::Transfer(sender),
            None => RetireAction::Cleanup,
        })
    }
}

#[derive(Default)]
struct WorkerStats {
    process_nanos: AtomicU64,
    wait_nanos: AtomicU64,
    processed_items: AtomicU64,
}

impl WorkerStats {
    fn reset(&self) {
        self.process_nanos.store(0, Ordering::Relaxed);
        self.wait_nanos.store(0, Ordering::Relaxed);
        self.processed_items.store(0, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScaleDirection {
    Add,
    Remove,
    Merge,
}

enum PendingScale {
    Add {
        worker_id: usize,
        node_index: usize,
    },
    Remove {
        worker_id: usize,
        node_index: usize,
    },
    MergeDown {
        source_worker_id: usize,
        target_worker_id: usize,
        node_index: usize,
        state_receiver: Option<channel::Receiver<Message>>,
        waiting_for_target: bool,
    },
}

struct NodeControl {
    is_external: bool,
    is_weighted_branch: bool,
    is_anchor: bool,
    thread_policy: ResolvedThreadPolicy,
    processed_count: u64,
    busy_ratio: f64,
    service_time_ewma: f64,
    per_worker_throughput: f64,
    desired_workers: usize,
    backlog_seconds: f64,
    last_sample_processed: u64,
    scaling_state: NodeScalingState,
    settling: bool,
    settle_samples: u32,
    settle_observed_work: bool,
    scale_down_eligible_since: Option<Instant>,
    last_anchor_pressure_reason: Option<AnchorPressureReason>,
    last_operation: Option<(ScaleDirection, Instant)>,
}

#[derive(Default)]
struct NodeSample {
    process_nanos: u64,
    wait_nanos: u64,
    processed_items: u64,
}

#[derive(Clone, Debug)]
struct LinkControl {
    last_arrivals: u64,
    last_drains: u64,
    len: usize,
    previous_len: usize,
    smoothed_len: f64,
    arrival_rate: f64,
    drain_rate: f64,
    net_rate: f64,
    trend: QueueTrend,
}

impl Default for LinkControl {
    fn default() -> Self {
        Self {
            last_arrivals: 0,
            last_drains: 0,
            len: 0,
            previous_len: 0,
            smoothed_len: 0.0,
            arrival_rate: 0.0,
            drain_rate: 0.0,
            net_rate: 0.0,
            trend: QueueTrend::Starved,
        }
    }
}

fn resolve_scale_policy<E>(mut policy: NodeScalePolicy) -> Result<ResolvedScalePolicy, E>
where
    E: Debug + Display + Send + 'static,
{
    let available = thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
        .max(1);
    if policy.max_threads == 0 {
        policy.max_threads = available;
    }
    if policy.initial_threads == 0 {
        policy.initial_threads = policy.max_threads.div_ceil(2).max(1);
    }
    if policy.max_threads == 0 || policy.initial_threads == 0 {
        return Err(PiperError::InvalidThreadCount);
    }
    Ok(ResolvedScalePolicy {
        max_threads: policy.max_threads,
        initial_threads: policy.initial_threads.min(policy.max_threads),
        target_queue_seconds: policy.target_queue_seconds,
        low_queue_seconds: policy.low_queue_seconds,
        scale_down_after: policy.scale_down_after,
        underutilized_busy_ratio: policy.underutilized_busy_ratio,
    })
}

fn resolve_thread_policy<E>(
    is_anchor: bool,
    hints: ThreadPolicyHints,
) -> Result<ResolvedThreadPolicy, E>
where
    E: Debug + Display + Send + 'static,
{
    if let Some(fixed_threads) = hints.fixed_threads {
        if fixed_threads == 0 {
            return Err(PiperError::InvalidThreadCount);
        }
        return Ok(ResolvedThreadPolicy::Fixed(fixed_threads));
    }

    let needs_scalable = hints.scale_policy.is_some()
        || hints.max_threads.is_some()
        || hints.initial_threads.is_some()
        || is_anchor;

    if needs_scalable {
        let mut policy = hints.scale_policy.unwrap_or_default();
        if let Some(max_threads) = hints.max_threads {
            policy.max_threads = max_threads;
        }
        if let Some(initial_threads) = hints.initial_threads {
            policy.initial_threads = initial_threads;
        }
        return Ok(ResolvedThreadPolicy::Scalable(resolve_scale_policy(policy)?));
    }

    Ok(ResolvedThreadPolicy::ImplicitSupport)
}

fn build_node_controls<E>(nodes: &[RuntimeNode<E>]) -> Vec<NodeControl>
where
    E: Debug + Display + Send + 'static,
{
    nodes
        .iter()
        .map(|node| NodeControl {
            is_external: node.is_external,
            is_weighted_branch: node.weighted_branch_config.is_some(),
            is_anchor: node.is_anchor,
            thread_policy: node.thread_policy,
            processed_count: 0,
            busy_ratio: 0.0,
            service_time_ewma: 0.0,
            per_worker_throughput: 0.0,
            desired_workers: 1,
            backlog_seconds: 0.0,
            last_sample_processed: 0,
            scaling_state: NodeScalingState::Eligible,
            settling: false,
            settle_samples: 0,
            settle_observed_work: false,
            scale_down_eligible_since: None,
            last_anchor_pressure_reason: None,
            last_operation: None,
        })
        .collect()
}

fn resolve_global_worker_cap(configured: Option<usize>, node_count: usize, required_base: usize) -> usize {
    let available = thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1)
        .max(1);
    configured
        .unwrap_or_else(|| available.saturating_mul(2).max(node_count))
        .max(node_count)
        .max(required_base)
}

fn startup_worker_count(policy: ResolvedThreadPolicy) -> (usize, usize) {
    match policy {
        ResolvedThreadPolicy::Fixed(count) => (count, 0),
        ResolvedThreadPolicy::Scalable(policy) => (1, policy.initial_threads.saturating_sub(1)),
        ResolvedThreadPolicy::ImplicitSupport => (1, 0),
    }
}

fn required_base_workers<E>(nodes: &[RuntimeNode<E>]) -> usize
where
    E: Debug + Display + Send + 'static,
{
    nodes
        .iter()
        .map(|node| {
            if node.is_external {
                0
            } else if node.weighted_branch_config.is_some() {
                1
            } else {
                startup_worker_count(node.thread_policy).0
                    + startup_worker_count(node.thread_policy).1
            }
        })
        .sum()
}

fn validate_graph_is_acyclic<E>(stages: &[GraphNodeSpec<E>]) -> Result<(), E>
where
    E: Debug + Display + Send + 'static,
{
    let mut adjacency = vec![Vec::<usize>::new(); stages.len()];
    for (producer_index, producer) in stages.iter().enumerate() {
        for (consumer_index, consumer) in stages.iter().enumerate() {
            if producer
                .output_links
                .iter()
                .any(|&output| output == consumer.input_link)
            {
                adjacency[producer_index].push(consumer_index);
            }
        }
    }

    fn visit(
        node: usize,
        adjacency: &[Vec<usize>],
        temporary: &mut [bool],
        permanent: &mut [bool],
    ) -> bool {
        if permanent[node] {
            return false;
        }
        if temporary[node] {
            return true;
        }
        temporary[node] = true;
        for child in &adjacency[node] {
            if visit(*child, adjacency, temporary, permanent) {
                return true;
            }
        }
        temporary[node] = false;
        permanent[node] = true;
        false
    }

    let mut temporary = vec![false; stages.len()];
    let mut permanent = vec![false; stages.len()];
    for index in 0..stages.len() {
        if visit(index, &adjacency, &mut temporary, &mut permanent) {
            return Err(PiperError::InvalidGraph {
                message: "graph cycles are not supported".to_string(),
            });
        }
    }
    Ok(())
}

pub struct Piper<In, Out, E = String>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    sender: PiperSender<In>,
    receiver: PiperReceiver<Out>,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    snapshot: Arc<RwLock<PiperSnapshot>>,
    supervisor: Option<JoinHandle<Result<SupervisorResult, E>>>,
    external_nodes: Vec<Option<UntypedExternalNode>>,
    state_return_stage: Option<Arc<dyn DynNode<E>>>,
}

impl<In, Out, E> Piper<In, Out, E>
where
    In: Send + 'static,
    Out: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    pub fn start(config: PiperConfig, graph: PipelineGraph<In, Out, E>) -> Result<Self, E> {
        if graph.nodes.is_empty() {
            return Err(PiperError::NoNodes);
        }

        let PipelineGraph {
            input_link,
            output_link,
            link_count,
            nodes: stages,
            external_count,
            state_return,
            #[cfg(feature = "feeder")]
            feeder_links,
            ..
        } = graph;

        if input_link >= link_count || output_link >= link_count {
            return Err(PiperError::InvalidGraph {
                message: "input or output link is outside the graph".to_string(),
            });
        }

        for stage in &stages {
            if stage.input_link >= link_count {
                return Err(PiperError::InvalidGraph {
                    message: format!("node `{}` references a missing link", stage.name),
                });
            }
            for &output_link in &stage.output_links {
                if output_link >= link_count {
                    return Err(PiperError::InvalidGraph {
                        message: format!("node `{}` references a missing link", stage.name),
                    });
                }
            }
            if let Some(config) = &stage.weighted_branch_config {
                validate_weighted_branch_config(&stage.name, config)?;
                if stage.output_links.len() != 2 {
                    return Err(PiperError::InvalidGraph {
                        message: format!(
                            "weighted branch `{}` must have exactly two output links",
                            stage.name
                        ),
                    });
                }
                #[cfg(feature = "feeder")]
                for (link_index, &output_link) in stage.output_links.iter().enumerate() {
                    if feeder_links.contains_key(&output_link) {
                        return Err(PiperError::UnsupportedWeightedBranchFeederOutput {
                            branch: stage.name.clone(),
                            link: link_index,
                        });
                    }
                }
            } else if stage.output_links.len() != 1 {
                return Err(PiperError::InvalidGraph {
                    message: format!("node `{}` must have exactly one output link", stage.name),
                });
            }
        }
        validate_graph_is_acyclic(&stages)?;

        let return_state_node_index = if let Some(config) = state_return {
            let producers: Vec<_> = stages
                .iter()
                .enumerate()
                .filter_map(|(index, stage)| {
                    stage
                        .output_links
                        .iter()
                        .any(|&link| link == output_link)
                        .then_some(index)
                })
                .collect();
            if producers.len() != 1 {
                return Err(PiperError::InvalidGraph {
                    message:
                        "return_state requires exactly one managed node to feed `output`"
                            .to_string(),
                });
            }
            let node_index = producers[0];
            let stage = &stages[node_index];
            if stage.external_index.is_some()
                || stage.weighted_branch_config.is_some()
                || stage.stage.is_none()
            {
                return Err(PiperError::InvalidGraph {
                    message:
                        "return_state requires the final output producer to be a managed node"
                            .to_string(),
                });
            }
            let stage_impl = stage.stage.as_ref().expect("managed state exists");
            if config.require_merge && !stage_impl.can_merge_state() {
                return Err(PiperError::MissingStateMerge {
                    node: stage.name.clone(),
                });
            }
            Some(node_index)
        } else {
            None
        };
        let state_return_stage = return_state_node_index
            .and_then(|index| stages[index].stage.as_ref().cloned());

        let anchor_count = stages.iter().filter(|stage| stage.is_anchor).count();
        if anchor_count == 0 && config.global_worker_cap == Some(0) {
            return Err(PiperError::InvalidGraph {
                message: "graphs without anchor nodes require a finite non-zero worker cap".to_string(),
            });
        }

        let shutdown = Arc::new(AtomicBool::new(false));
        let abort = Arc::new(AtomicBool::new(false));
        let (internal_failure_sender, internal_failure_receiver) = channel::unbounded();
        let lease_runtime = LeaseRuntime {
            shutdown: Arc::clone(&shutdown),
            abort: Arc::clone(&abort),
            internal_failure: internal_failure_sender.clone(),
        };

        let mut links = Vec::with_capacity(link_count);
        for link_index in 0..link_count {
            #[cfg(feature = "feeder")]
            let kind = if let Some(cfg) = feeder_links.get(&link_index) {
                let feeder = feeder::Feeder::<Message>::builder()
                    .scheduler_threads(cfg.scheduler_threads)
                    .build();
                LinkKind::Feeder {
                    feeder: Arc::new(feeder),
                    low: cfg.water_low,
                    high: cfg.water_high,
                }
            } else {
                let (sender, receiver) = channel::unbounded::<Message>();
                LinkKind::Standard {
                    sender: Some(sender),
                    receiver,
                }
            };
            #[cfg(not(feature = "feeder"))]
            let kind = {
                let _ = link_index;
                let (sender, receiver) = channel::unbounded::<Message>();
                LinkKind::Standard {
                    sender: Some(sender),
                    receiver,
                }
            };
            links.push(Link {
                stats: Arc::new(LinkStats::default()),
                kind,
            });
        }

        let input_sender = links[input_link].make_output();
        let output_receiver = links[output_link].make_input();
        let input_stats = Arc::clone(&links[input_link].stats);
        let output_stats = Arc::clone(&links[output_link].stats);

        let runtime_stages: Vec<_> = stages
            .into_iter()
            .enumerate()
            .map(|(index, stage)| {
                let thread_policy =
                    resolve_thread_policy(stage.is_anchor, stage.thread_hints)?;
                let is_external = stage.external_index.is_some();
                Ok(RuntimeNode {
                    name: stage.name,
                    stage: stage.stage,
                    output_acquire: stage
                        .output_acquire_builder
                        .map(|builder| builder.build(lease_runtime.clone())),
                    is_anchor: stage.is_anchor,
                    thread_policy,
                    input_link: stage.input_link,
                    output_links: stage.output_links,
                    weighted_branch_config: stage.weighted_branch_config,
                    is_external,
                    external_index: stage.external_index,
                    return_state_on_exit: return_state_node_index == Some(index),
                })
            })
            .collect::<Result<Vec<_>, E>>()?;

        let mut external_nodes: Vec<Option<UntypedExternalNode>> =
            (0..external_count).map(|_| None).collect();
        for stage in &runtime_stages {
            if let Some(external_index) = stage.external_index {
                external_nodes[external_index] = Some(UntypedExternalNode {
                    name: stage.name.clone(),
                    input: links[stage.input_link].make_input(),
                    input_stats: Arc::clone(&links[stage.input_link].stats),
                    output: links[stage.output_links[0]].make_output(),
                    output_stats: Arc::clone(&links[stage.output_links[0]].stats),
                    shutdown: Arc::clone(&shutdown),
                    abort: Arc::clone(&abort),
                    internal_failure: internal_failure_sender.clone(),
                    output_acquire: stage.output_acquire.clone(),
                });
            }
        }

        let required_base = required_base_workers(&runtime_stages);
        let global_worker_cap = resolve_global_worker_cap(
            config.global_worker_cap,
            runtime_stages.len(),
            required_base,
        );

        let snapshot = Arc::new(RwLock::new(PiperSnapshot {
            links: (0..link_count)
                .map(|index| LinkSnapshot {
                    index,
                    len: 0,
                    trend: QueueTrend::Starved,
                    arrival_rate: 0.0,
                    drain_rate: 0.0,
                    net_rate: 0.0,
                    smoothed_len: 0.0,
                })
                .collect(),
            nodes: runtime_stages
                .iter()
                .enumerate()
                .map(|(index, stage)| NodeSnapshot {
                    index,
                    name: stage.name.clone(),
                    input_link: stage.input_link,
                    output_link: stage.output_links[0],
                    output_links: stage.output_links.clone(),
                    active_threads: 0,
                    processed_count: 0,
                    busy_ratio: 0.0,
                    service_time: Duration::ZERO,
                    per_worker_throughput: 0.0,
                    desired_workers: 1,
                    scaling_state: NodeScalingState::Eligible,
                    is_anchor: stage.is_anchor,
                    thread_policy_kind: thread_policy_kind(stage.thread_policy),
                    fixed_thread_count: fixed_thread_count(stage.thread_policy),
                    max_thread_count: max_thread_count(stage.thread_policy),
                    target_queue_seconds: target_queue_seconds(stage.thread_policy),
                    low_queue_seconds: low_queue_seconds(stage.thread_policy),
                    backlog_seconds: 0.0,
                    is_external: stage.is_external,
                    external_input_rate: 0.0,
                    external_output_rate: 0.0,
                })
                .collect(),
            anchors: runtime_stages
                .iter()
                .enumerate()
                .filter_map(|(index, stage)| {
                    stage.is_anchor.then(|| AnchorSnapshot {
                        node_index: index,
                        node_name: stage.name.clone(),
                        active_threads: 0,
                        last_pressure_reason: None,
                    })
                })
                .collect(),
            parked_threads: 0,
            total_active_workers: 0,
            global_worker_cap,
            budget_pressure: false,
            output_backpressure: false,
            shutdown_requested: false,
            abort_requested: false,
            pending_scale_operation: false,
        }));

        let csv_recorder = start_telemetry_log_recorder(
            config.csv_telemetry.clone(),
            Arc::clone(&snapshot),
            internal_failure_sender.clone(),
            Arc::clone(&abort),
            input_link,
            output_link,
        )?;

        let supervisor_snapshot = Arc::clone(&snapshot);
        let supervisor_shutdown = Arc::clone(&shutdown);
        let supervisor_abort = Arc::clone(&abort);
        let (startup_ready_tx, startup_ready_rx) = channel::unbounded();
        let supervisor = thread::Builder::new()
            .name("piper-supervisor".to_string())
            .spawn(move || {
                run_supervisor(
                    config,
                    runtime_stages,
                    links,
                    input_link,
                    output_link,
                    supervisor_shutdown,
                    supervisor_abort,
                    supervisor_snapshot,
                    internal_failure_sender,
                    internal_failure_receiver,
                    csv_recorder,
                    startup_ready_tx,
                )
            })
            .map_err(|source| PiperError::SpawnFailed {
                worker: "piper-supervisor".to_string(),
                source,
            })?;
        if startup_ready_rx
            .recv_timeout(Duration::from_secs(10))
            .is_err()
        {
            return Err(PiperError::Internal {
                worker: "piper-supervisor".to_string(),
                message: "initial workers were not ready before startup completed".to_string(),
            });
        }

        Ok(Piper {
            sender: PiperSender {
                inner: input_sender,
                stats: input_stats,
                shutdown: Arc::clone(&shutdown),
                _marker: PhantomData,
            },
            receiver: PiperReceiver {
                inner: output_receiver,
                stats: output_stats,
                _marker: PhantomData,
            },
            shutdown,
            abort,
            snapshot,
            supervisor: Some(supervisor),
            external_nodes,
            state_return_stage,
        })
    }

    pub fn sender(&self) -> PiperSender<In> {
        self.sender.clone()
    }

    pub fn receiver(&self) -> PiperReceiver<Out> {
        self.receiver.clone()
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    pub fn abort(&self) {
        self.abort.store(true, Ordering::Release);
    }

    pub fn get_telemetry(&self) -> PiperSnapshot {
        self.snapshot.read().clone()
    }

    pub fn take_external_node<ExtIn, ExtOut>(
        &mut self,
        token: ExternalNodeToken<ExtIn, ExtOut>,
    ) -> ExternalNode<ExtIn, ExtOut, E>
    where
        ExtIn: Send + 'static,
        ExtOut: Send + 'static,
    {
        self.external_nodes
            .get_mut(token.index)
            .and_then(Option::take)
            .unwrap_or_else(|| panic!("external node handle {} is unavailable", token.index))
            .into_typed()
    }

    fn join_supervisor(mut self) -> Result<SupervisorResult, E> {
        self.shutdown();
        self.external_nodes.clear();
        if let Some(supervisor) = self.supervisor.take() {
            supervisor
                .join()
                .map_err(|payload| PiperError::WorkerPanicked {
                    worker: "piper-supervisor".to_string(),
                    message: panic_payload_to_string(payload),
                })?
        } else {
            Ok(SupervisorResult {
                returned_states: Vec::new(),
            })
        }
    }

    pub fn join(self) -> Result<(), E> {
        self.join_supervisor().map(|_| ())
    }

    fn join_returned_states<State>(self) -> Result<Vec<State>, E>
    where
        State: Send + 'static,
    {
        let result = self.join_supervisor()?;
        result
            .returned_states
            .into_iter()
            .map(|state| {
                state
                    .downcast::<State>()
                    .map(|state| *state)
                    .map_err(|_| PiperError::Internal {
                        worker: "piper-supervisor".to_string(),
                        message: "returned node state type mismatch".to_string(),
                    })
            })
            .collect()
    }

    fn join_merged_state<State>(self) -> Result<State, E>
    where
        State: Send + 'static,
    {
        let stage = self
            .state_return_stage
            .as_ref()
            .cloned()
            .ok_or_else(|| PiperError::Internal {
                worker: "piper-supervisor".to_string(),
                message: "pipeline does not have a returned state node".to_string(),
            })?;
        if !stage.can_merge_state() {
            return Err(PiperError::MissingStateMerge {
                node: "returned-state node".to_string(),
            });
        }

        let mut states = self.join_supervisor()?.returned_states.into_iter();
        let mut merged = states.next().ok_or_else(|| PiperError::Internal {
            worker: "piper-supervisor".to_string(),
            message: "no returned node state was collected".to_string(),
        })?;
        for state in states {
            stage
                .merge_box(merged.as_mut(), state)
                .map_err(|failure| node_failure_to_piper_error("piper-supervisor", failure))?;
        }
        merged
            .downcast::<State>()
            .map(|state| *state)
            .map_err(|_| PiperError::Internal {
                worker: "piper-supervisor".to_string(),
                message: "returned node state type mismatch".to_string(),
            })
    }
}

pub struct PiperWithState<In, Out, State, E = String, const MERGE: bool = false>
where
    In: Send + 'static,
    Out: Send + 'static,
    State: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    inner: Piper<In, Out, E>,
    _marker: PhantomData<fn() -> State>,
}

impl<In, Out, State, E, const MERGE: bool> PiperWithState<In, Out, State, E, MERGE>
where
    In: Send + 'static,
    Out: Send + 'static,
    State: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    pub fn start(
        config: PiperConfig,
        graph: PipelineGraphWithState<In, Out, State, E, MERGE>,
    ) -> Result<Self, E> {
        Piper::start(config, graph.graph).map(|inner| Self {
            inner,
            _marker: PhantomData,
        })
    }

    pub fn sender(&self) -> PiperSender<In> {
        self.inner.sender()
    }

    pub fn receiver(&self) -> PiperReceiver<Out> {
        self.inner.receiver()
    }

    pub fn shutdown(&self) {
        self.inner.shutdown();
    }

    pub fn abort(&self) {
        self.inner.abort();
    }

    pub fn get_telemetry(&self) -> PiperSnapshot {
        self.inner.get_telemetry()
    }

    pub fn take_external_node<ExtIn, ExtOut>(
        &mut self,
        token: ExternalNodeToken<ExtIn, ExtOut>,
    ) -> ExternalNode<ExtIn, ExtOut, E>
    where
        ExtIn: Send + 'static,
        ExtOut: Send + 'static,
    {
        self.inner.take_external_node(token)
    }

    pub fn join(self) -> Result<Vec<State>, E> {
        self.inner.join_returned_states()
    }
}

impl<In, Out, State, E> PiperWithState<In, Out, State, E, true>
where
    In: Send + 'static,
    Out: Send + 'static,
    State: Send + 'static,
    E: Debug + Display + Send + 'static,
{
    pub fn join_merged(self) -> Result<State, E> {
        self.inner.join_merged_state()
    }
}

fn node_failure_to_piper_error<E>(worker: &str, failure: NodeFailure<E>) -> PiperError<E>
where
    E: Debug + Display + Send + 'static,
{
    match failure {
        NodeFailure::Init(error) => PiperError::UserInit {
            worker: worker.to_string(),
            error,
        },
        NodeFailure::Process(error) => PiperError::UserProcess {
            worker: worker.to_string(),
            error,
        },
        NodeFailure::Cleanup(error) => PiperError::UserCleanup {
            worker: worker.to_string(),
            error,
        },
        NodeFailure::Merge(error) => PiperError::UserMerge {
            worker: worker.to_string(),
            error,
        },
        NodeFailure::Internal(message) => PiperError::Internal {
            worker: worker.to_string(),
            message,
        },
    }
}

const TELEMETRY_LOG_VERSION_LINE: &str = "# piper-telemetry-log-v2";
const TELEMETRY_MANIFEST_PREFIX: &str = "# manifest ";

#[derive(Serialize)]
struct TelemetryLogManifest {
    version: u32,
    format: &'static str,
    links: Vec<ManifestLink>,
    nodes: Vec<ManifestNode>,
    anchors: Vec<ManifestAnchor>,
    metrics: Vec<ManifestMetric>,
}

#[derive(Serialize)]
struct ManifestNode {
    id: String,
    index: usize,
    name: String,
    input_link: String,
    output_link: String,
    output_links: Vec<String>,
    is_anchor: bool,
    thread_policy_kind: String,
    is_external: bool,
}

#[derive(Serialize)]
struct ManifestLink {
    id: String,
    index: usize,
    kind: String,
    label: String,
    producers: Vec<String>,
    consumers: Vec<String>,
}

#[derive(Serialize)]
struct ManifestAnchor {
    id: String,
    index: usize,
    name: String,
}

#[derive(Serialize)]
struct ManifestMetric {
    column: String,
    object_id: String,
    object_kind: String,
    category: String,
    metric: String,
    label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    unit: Option<String>,
}

struct TelemetryLogRecorder {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

impl TelemetryLogRecorder {
    fn stop<E>(self) -> Result<(), E>
    where
        E: Debug + Display + Send + 'static,
    {
        self.stop.store(true, Ordering::Release);
        self.handle
            .join()
            .map_err(|payload| PiperError::WorkerPanicked {
                worker: "piper-telemetry-log".to_string(),
                message: panic_payload_to_string(payload),
            })
    }
}

fn start_telemetry_log_recorder<E>(
    config: Option<TelemetryLogConfig>,
    snapshot: Arc<RwLock<PiperSnapshot>>,
    failure_sender: channel::Sender<InternalFailure>,
    abort: Arc<AtomicBool>,
    input_link: usize,
    output_link: usize,
) -> Result<Option<TelemetryLogRecorder>, E>
where
    E: Debug + Display + Send + 'static,
{
    let Some(config) = config else {
        return Ok(None);
    };

    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&config.path)
        .map_err(|source| PiperError::Telemetry {
            message: format!(
                "failed to create telemetry log file `{}`: {source}",
                config.path.display()
            ),
        })?;
    let mut writer = BufWriter::new(file);
    let initial_snapshot = snapshot.read().clone();
    let manifest = build_telemetry_manifest(&initial_snapshot, input_link, output_link);
    writeln!(writer, "{TELEMETRY_LOG_VERSION_LINE}").map_err(|source| PiperError::Telemetry {
        message: format!(
            "failed to write telemetry log version line `{}`: {source}",
            config.path.display()
        ),
    })?;
    let manifest_json =
        serde_json::to_string(&manifest).map_err(|source| PiperError::Telemetry {
            message: format!(
                "failed to serialize telemetry manifest `{}`: {source}",
                config.path.display()
            ),
        })?;
    writeln!(writer, "{TELEMETRY_MANIFEST_PREFIX}{manifest_json}").map_err(|source| {
        PiperError::Telemetry {
            message: format!(
                "failed to write telemetry manifest `{}`: {source}",
                config.path.display()
            ),
        }
    })?;
    writeln!(writer, "{}", telemetry_log_header(&manifest)).map_err(|source| {
        PiperError::Telemetry {
            message: format!(
                "failed to write telemetry log header `{}`: {source}",
                config.path.display()
            ),
        }
    })?;
    writer.flush().map_err(|source| PiperError::Telemetry {
        message: format!(
            "failed to flush telemetry log header `{}`: {source}",
            config.path.display()
        ),
    })?;

    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let path = config.path.clone();
    let handle = thread::Builder::new()
        .name("piper-telemetry-log".to_string())
        .spawn(move || {
            let started = Instant::now();
            loop {
                thread::sleep(config.interval);
                let stopping = thread_stop.load(Ordering::Acquire) || abort.load(Ordering::Acquire);
                let snapshot = snapshot.read().clone();
                if let Err(source) = writeln!(
                    writer,
                    "{}",
                    telemetry_log_row(started.elapsed(), &snapshot, &manifest)
                )
                .and_then(|_| writer.flush())
                {
                    abort.store(true, Ordering::Release);
                    let _ = failure_sender.send(InternalFailure::telemetry(format!(
                        "failed to write telemetry log file `{}`: {source}",
                        path.display()
                    )));
                    break;
                }
                if stopping {
                    break;
                }
            }
        })
        .map_err(|source| PiperError::SpawnFailed {
            worker: "piper-telemetry-log".to_string(),
            source,
        })?;

    Ok(Some(TelemetryLogRecorder { stop, handle }))
}

fn link_object_id(index: usize) -> String {
    format!("link{index}")
}

fn node_object_id(index: usize) -> String {
    format!("node{index}")
}

fn anchor_object_id(index: usize) -> String {
    format!("anchor{index}")
}

fn manifest_node_name<'a>(stage_id: &'a str, stages: &'a [ManifestNode]) -> &'a str {
    if stage_id == "input" {
        return "input";
    }
    if stage_id == "output" {
        return "output";
    }
    stages
        .iter()
        .find(|stage| stage.id == stage_id)
        .map(|stage| stage.name.as_str())
        .unwrap_or(stage_id)
}

fn format_manifest_node_list(ids: &[String], stages: &[ManifestNode]) -> String {
    let names: Vec<&str> = ids
        .iter()
        .map(|id| manifest_node_name(id, stages))
        .collect();
    if names.len() == 1 {
        names[0].to_string()
    } else {
        format!("{{{}}}", names.join(", "))
    }
}

fn manifest_link_kind(
    link_index: usize,
    input_link: usize,
    output_link: usize,
    producers: &[String],
    consumers: &[String],
) -> &'static str {
    if link_index == input_link {
        return "input";
    }
    if link_index == output_link {
        return "output";
    }
    match (producers.len(), consumers.len()) {
        (1, 1) => "internal",
        (1, _) if consumers.len() > 1 => "fork",
        (_, 1) if producers.len() > 1 => "join",
        (producer_count, consumer_count) if producer_count > 1 && consumer_count > 1 => "fork_join",
        _ => "internal",
    }
}

fn manifest_link_label(
    kind: &str,
    producers: &[String],
    consumers: &[String],
    stages: &[ManifestNode],
) -> String {
    match kind {
        "input" => format!("input -> {}", format_manifest_node_list(consumers, stages)),
        "output" => format!("{} -> output", format_manifest_node_list(producers, stages)),
        "fork" => format!(
            "{} -> {}",
            manifest_node_name(&producers[0], stages),
            format_manifest_node_list(consumers, stages)
        ),
        "join" => format!(
            "{} -> {}",
            format_manifest_node_list(producers, stages),
            manifest_node_name(&consumers[0], stages)
        ),
        "fork_join" => format!(
            "{} -> {}",
            format_manifest_node_list(producers, stages),
            format_manifest_node_list(consumers, stages)
        ),
        _ => format!(
            "{} -> {}",
            manifest_node_name(&producers[0], stages),
            manifest_node_name(&consumers[0], stages)
        ),
    }
}

fn push_manifest_metric(
    metrics: &mut Vec<ManifestMetric>,
    column: impl Into<String>,
    object_id: impl Into<String>,
    object_kind: impl Into<String>,
    category: impl Into<String>,
    metric: impl Into<String>,
    label: impl Into<String>,
    unit: Option<&str>,
) {
    metrics.push(ManifestMetric {
        column: column.into(),
        object_id: object_id.into(),
        object_kind: object_kind.into(),
        category: category.into(),
        metric: metric.into(),
        label: label.into(),
        unit: unit.map(str::to_string),
    });
}

fn build_telemetry_manifest(
    snapshot: &PiperSnapshot,
    input_link: usize,
    output_link: usize,
) -> TelemetryLogManifest {
    let link_count = snapshot.links.len();
    let mut producers_by_link = vec![Vec::<String>::new(); link_count];
    let mut consumers_by_link = vec![Vec::<String>::new(); link_count];

    let stages: Vec<ManifestNode> = snapshot
        .nodes
        .iter()
        .map(|stage| ManifestNode {
            id: node_object_id(stage.index),
            index: stage.index,
            name: stage.name.clone(),
            input_link: link_object_id(stage.input_link),
            output_link: link_object_id(stage.output_link),
            output_links: stage
                .output_links
                .iter()
                .map(|&index| link_object_id(index))
                .collect(),
            is_anchor: stage.is_anchor,
            thread_policy_kind: format!("{:?}", stage.thread_policy_kind),
            is_external: stage.is_external,
        })
        .collect();

    for stage in &snapshot.nodes {
        let stage_id = node_object_id(stage.index);
        consumers_by_link[stage.input_link].push(stage_id.clone());
        for &output_link in &stage.output_links {
            producers_by_link[output_link].push(stage_id.clone());
        }
    }
    producers_by_link[input_link] = vec!["input".to_string()];
    for link_index in 0..link_count {
        if link_index == output_link {
            consumers_by_link[link_index] = vec!["output".to_string()];
            continue;
        }
        if producers_by_link[link_index].len() > 1 && consumers_by_link[link_index].len() == 1 {
            let consumer_id = &consumers_by_link[link_index][0];
            if let Some(stage) = snapshot
                .nodes
                .iter()
                .find(|stage| node_object_id(stage.index) == *consumer_id)
            {
                if stage.output_links.iter().any(|&link| link == output_link) {
                    consumers_by_link[link_index] = vec!["output".to_string()];
                }
            }
        }
    }

    let links: Vec<ManifestLink> = snapshot
        .links
        .iter()
        .map(|link| {
            let producers = producers_by_link[link.index].clone();
            let consumers = consumers_by_link[link.index].clone();
            let kind =
                manifest_link_kind(link.index, input_link, output_link, &producers, &consumers)
                    .to_string();
            let label = manifest_link_label(&kind, &producers, &consumers, &stages);
            ManifestLink {
                id: link_object_id(link.index),
                index: link.index,
                kind,
                label,
                producers,
                consumers,
            }
        })
        .collect();

    let anchors: Vec<ManifestAnchor> = snapshot
        .anchors
        .iter()
        .enumerate()
        .map(|(index, anchor)| ManifestAnchor {
            id: anchor_object_id(index),
            index,
            name: anchor.node_name.clone(),
        })
        .collect();

    let mut metrics = Vec::new();
    push_manifest_metric(
        &mut metrics,
        "shutdown_requested",
        "pipeline",
        "pipeline",
        "Pipeline",
        "shutdown_requested",
        "shutdown requested",
        None,
    );
    push_manifest_metric(
        &mut metrics,
        "abort_requested",
        "pipeline",
        "pipeline",
        "Pipeline",
        "abort_requested",
        "abort requested",
        None,
    );
    push_manifest_metric(
        &mut metrics,
        "pending_scale_operation",
        "pipeline",
        "pipeline",
        "Pipeline",
        "pending_scale_operation",
        "pending scale operation",
        None,
    );
    push_manifest_metric(
        &mut metrics,
        "parked_threads",
        "pipeline",
        "pipeline",
        "Pipeline",
        "parked_threads",
        "parked threads",
        None,
    );
    push_manifest_metric(
        &mut metrics,
        "total_active_workers",
        "pipeline",
        "pipeline",
        "Pipeline",
        "total_active_workers",
        "active workers",
        None,
    );
    push_manifest_metric(
        &mut metrics,
        "global_worker_cap",
        "pipeline",
        "pipeline",
        "Pipeline",
        "global_worker_cap",
        "global worker cap",
        None,
    );
    push_manifest_metric(
        &mut metrics,
        "budget_pressure",
        "pipeline",
        "pipeline",
        "Pipeline",
        "budget_pressure",
        "budget pressure",
        None,
    );
    push_manifest_metric(
        &mut metrics,
        "output_backpressure",
        "pipeline",
        "pipeline",
        "Pipeline",
        "output_backpressure",
        "output backpressure",
        None,
    );

    for link in &snapshot.links {
        let object_id = link_object_id(link.index);
        for (metric, label, unit) in [
            ("len", "queue length", None),
            ("trend", "queue trend", None),
            ("arrival_rate", "arrival rate", Some("items/s")),
            ("drain_rate", "drain rate", Some("items/s")),
            ("net_rate", "net rate", Some("items/s")),
            ("smoothed_len", "smoothed length", None),
        ] {
            push_manifest_metric(
                &mut metrics,
                format!("link{}_{metric}", link.index),
                &object_id,
                "link",
                "Links",
                metric,
                label,
                unit,
            );
        }
    }

    for stage in &snapshot.nodes {
        let object_id = node_object_id(stage.index);
        for (metric, label, unit) in [
            ("active_threads", "active threads", None),
            ("processed_count", "processed count", None),
            ("busy_ratio", "busy ratio", None),
            ("service_time_ms", "service time", Some("ms")),
            (
                "per_worker_throughput",
                "per-worker throughput",
                Some("items/s"),
            ),
            ("desired_workers", "desired workers", None),
            ("scaling_state", "scaling state", None),
            ("thread_policy_kind", "thread policy", None),
            ("fixed_thread_count", "fixed threads", None),
            ("max_thread_count", "max threads", None),
            ("target_queue_seconds", "target queue seconds", Some("s")),
            ("low_queue_seconds", "low queue seconds", Some("s")),
            ("backlog_seconds", "backlog seconds", Some("s")),
        ] {
            push_manifest_metric(
                &mut metrics,
                format!("node{}_{metric}", stage.index),
                &object_id,
                "node",
                "Nodes",
                metric,
                label,
                unit,
            );
        }
        if stage.is_external {
            for (metric, label) in [
                ("external_input_rate", "external input rate"),
                ("external_output_rate", "external output rate"),
            ] {
                push_manifest_metric(
                    &mut metrics,
                    format!("node{}_{metric}", stage.index),
                    &object_id,
                    "node",
                    "External",
                    metric,
                    label,
                    Some("items/s"),
                );
            }
        }
    }

    for (index, _anchor) in snapshot.anchors.iter().enumerate() {
        let object_id = anchor_object_id(index);
        for (metric, label) in [
            ("active_threads", "active threads"),
            ("last_pressure_reason", "last pressure reason"),
        ] {
            push_manifest_metric(
                &mut metrics,
                format!("anchor{index}_{metric}"),
                &object_id,
                "anchor",
                "Anchors",
                metric,
                label,
                None,
            );
        }
    }

    TelemetryLogManifest {
        version: 2,
        format: "piper-telemetry-log-v2",
        links,
        nodes: stages,
        anchors,
        metrics,
    }
}

fn telemetry_log_header(manifest: &TelemetryLogManifest) -> String {
    let mut fields = vec!["elapsed_ms".to_string()];
    fields.extend(manifest.metrics.iter().map(|metric| metric.column.clone()));
    fields.join(",")
}

fn telemetry_log_metric_value(column: &str, snapshot: &PiperSnapshot) -> String {
    match column {
        "shutdown_requested" => snapshot.shutdown_requested.to_string(),
        "abort_requested" => snapshot.abort_requested.to_string(),
        "pending_scale_operation" => snapshot.pending_scale_operation.to_string(),
        "parked_threads" => snapshot.parked_threads.to_string(),
        "total_active_workers" => snapshot.total_active_workers.to_string(),
        "global_worker_cap" => snapshot.global_worker_cap.to_string(),
        "budget_pressure" => snapshot.budget_pressure.to_string(),
        "output_backpressure" => snapshot.output_backpressure.to_string(),
        _ => {
            if let Some(rest) = column.strip_prefix("link") {
                if let Some((index, metric)) = rest.split_once('_') {
                    if let Ok(link_index) = index.parse::<usize>() {
                        if let Some(link) =
                            snapshot.links.iter().find(|link| link.index == link_index)
                        {
                            return match metric {
                                "len" => link.len.to_string(),
                                "trend" => link.trend.code().to_string(),
                                "arrival_rate" => format!("{:.6}", link.arrival_rate),
                                "drain_rate" => format!("{:.6}", link.drain_rate),
                                "net_rate" => format!("{:.6}", link.net_rate),
                                "smoothed_len" => format!("{:.6}", link.smoothed_len),
                                _ => String::new(),
                            };
                        }
                    }
                }
            }
            if let Some(rest) = column.strip_prefix("node") {
                if let Some((index, metric)) = rest.split_once('_') {
                    if let Ok(node_index) = index.parse::<usize>() {
                        if let Some(stage) = snapshot
                            .nodes
                            .iter()
                            .find(|stage| stage.index == node_index)
                        {
                            return match metric {
                                "active_threads" => stage.active_threads.to_string(),
                                "processed_count" => stage.processed_count.to_string(),
                                "busy_ratio" => format!("{:.6}", stage.busy_ratio),
                                "service_time_ms" => {
                                    format!("{:.3}", stage.service_time.as_secs_f64() * 1000.0)
                                }
                                "per_worker_throughput" => {
                                    format!("{:.6}", stage.per_worker_throughput)
                                }
                                "desired_workers" => stage.desired_workers.to_string(),
                                "scaling_state" => format!("{:?}", stage.scaling_state),
                                "thread_policy_kind" => {
                                    format!("{:?}", stage.thread_policy_kind)
                                }
                                "fixed_thread_count" => stage
                                    .fixed_thread_count
                                    .map(|count| count.to_string())
                                    .unwrap_or_default(),
                                "max_thread_count" => stage
                                    .max_thread_count
                                    .map(|count| count.to_string())
                                    .unwrap_or_default(),
                                "target_queue_seconds" => stage
                                    .target_queue_seconds
                                    .map(|value| format!("{value:.6}"))
                                    .unwrap_or_default(),
                                "low_queue_seconds" => stage
                                    .low_queue_seconds
                                    .map(|value| format!("{value:.6}"))
                                    .unwrap_or_default(),
                                "backlog_seconds" => format!("{:.6}", stage.backlog_seconds),
                                "external_input_rate" => {
                                    format!("{:.6}", stage.external_input_rate)
                                }
                                "external_output_rate" => {
                                    format!("{:.6}", stage.external_output_rate)
                                }
                                _ => String::new(),
                            };
                        }
                    }
                }
            }
            if let Some(rest) = column.strip_prefix("anchor") {
                if let Some((index, metric)) = rest.split_once('_') {
                    if let Ok(anchor_index) = index.parse::<usize>() {
                        if let Some(anchor) = snapshot.anchors.get(anchor_index) {
                            return match metric {
                                "active_threads" => anchor.active_threads.to_string(),
                                "last_pressure_reason" => anchor
                                    .last_pressure_reason
                                    .map(|reason| format!("{reason:?}"))
                                    .unwrap_or_default(),
                                _ => String::new(),
                            };
                        }
                    }
                }
            }
            String::new()
        }
    }
}

fn telemetry_log_row(
    elapsed: Duration,
    snapshot: &PiperSnapshot,
    manifest: &TelemetryLogManifest,
) -> String {
    let mut fields = vec![format!("{:.3}", elapsed.as_secs_f64() * 1000.0)];
    fields.extend(
        manifest
            .metrics
            .iter()
            .map(|metric| telemetry_log_metric_value(&metric.column, snapshot)),
    );
    fields.join(",")
}

fn run_supervisor<E>(
    config: PiperConfig,
    stages: Vec<RuntimeNode<E>>,
    mut links: Vec<Link>,
    input_link: usize,
    output_link: usize,
    shutdown: Arc<AtomicBool>,
    abort: Arc<AtomicBool>,
    snapshot: Arc<RwLock<PiperSnapshot>>,
    internal_failure_sender: channel::Sender<InternalFailure>,
    internal_failure_receiver: channel::Receiver<InternalFailure>,
    mut csv_recorder: Option<TelemetryLogRecorder>,
    startup_ready_tx: channel::Sender<()>,
) -> Result<SupervisorResult, E>
where
    E: Debug + Display + Send + 'static,
{
    let (worker_event_sender, worker_event_receiver) = channel::unbounded::<WorkerEvent<E>>();
    let mut workers = Vec::new();
    let mut active_by_node = vec![Vec::<usize>::new(); stages.len()];
    let mut parked = Vec::new();
    let mut link_controls = vec![LinkControl::default(); links.len()];
    let link_queue_lens: Vec<Arc<AtomicUsize>> = (0..links.len())
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();
    let mut controls = build_node_controls(&stages);
    let required_base = required_base_workers(&stages);
    let global_worker_cap =
        resolve_global_worker_cap(config.global_worker_cap, stages.len(), required_base);
    let mut pending_scale = None;
    let mut stored_failure = None;
    let mut returned_states = Vec::new();
    let mut supervisor_holds_links = true;
    let mut last_sample_at = Instant::now();

    let mut scalable_extra: Vec<(usize, usize)> = Vec::new();
    for node_index in 0..stages.len() {
        if stages[node_index].is_external {
            continue;
        }
        let (base, extra) = if stages[node_index].weighted_branch_config.is_some() {
            (1, 0)
        } else {
            startup_worker_count(stages[node_index].thread_policy)
        };
        if extra > 0 {
            scalable_extra.push((node_index, extra));
        }
        for _ in 0..base {
            let name = format!("piper-worker-{}", workers.len());
            let worker_id = spawn_worker(&mut workers, worker_event_sender.clone(), &name)?;
            assign_worker(
                worker_id,
                node_index,
                &stages,
                &links,
                &config,
                input_link,
                &shutdown,
                &abort,
                &internal_failure_sender,
                &link_queue_lens,
                &mut workers,
                &mut active_by_node,
            )?;
        }
    }

    for (node_index, mut remaining) in scalable_extra {
        while remaining > 0 && active_worker_count(&active_by_node) < global_worker_cap {
            let name = format!("piper-worker-{}", workers.len());
            let worker_id = spawn_worker(&mut workers, worker_event_sender.clone(), &name)?;
            assign_worker(
                worker_id,
                node_index,
                &stages,
                &links,
                &config,
                input_link,
                &shutdown,
                &abort,
                &internal_failure_sender,
                &link_queue_lens,
                &mut workers,
                &mut active_by_node,
            )?;
            remaining -= 1;
        }
    }

    let parked_target = parked_worker_target(&stages);
    for _ in 0..parked_target {
        let name = format!("piper-worker-{}", workers.len());
        let worker_id = spawn_worker(&mut workers, worker_event_sender.clone(), &name)?;
        parked.push(worker_id);
    }

    let _ = startup_ready_tx.send(());

    update_snapshot(
        &snapshot,
        &links,
        &stages,
        &active_by_node,
        parked.len(),
        &link_controls,
        &controls,
        output_link,
        global_worker_cap,
        shutdown.load(Ordering::Acquire),
        abort.load(Ordering::Acquire),
        pending_scale.is_some(),
    );

    loop {
        drain_worker_events(
            &worker_event_receiver,
            &mut workers,
            &mut active_by_node,
            &mut parked,
            &mut controls,
            &mut pending_scale,
            &abort,
            &mut stored_failure,
            &mut returned_states,
        );

        drain_internal_failures(&internal_failure_receiver, &abort, &mut stored_failure);

        if shutdown.load(Ordering::Acquire)
            || abort.load(Ordering::Acquire)
            || stored_failure.is_some()
        {
            if supervisor_holds_links {
                let aborting = abort.load(Ordering::Acquire) || stored_failure.is_some();
                for link in &mut links {
                    if aborting {
                        link.abort();
                    } else {
                        link.close();
                    }
                }
                supervisor_holds_links = false;
            }
        }

        let now = Instant::now();
        let sample_elapsed = now
            .duration_since(last_sample_at)
            .max(Duration::from_micros(1));
        last_sample_at = now;
        sample_links(&links, sample_elapsed, &mut link_controls);
        for (index, link) in links.iter().enumerate() {
            link_queue_lens[index].store(link.queue_len().unwrap_or(0), Ordering::Relaxed);
        }
        collect_node_samples(
            &workers,
            &active_by_node,
            &link_controls,
            &stages,
            &mut controls,
            sample_elapsed,
        );
        update_desired_workers(&link_controls, &active_by_node, &stages, &mut controls);

        if !shutdown.load(Ordering::Acquire)
            && !abort.load(Ordering::Acquire)
            && stored_failure.is_none()
            && pending_scale.is_none()
        {
            if let Some(operation) = choose_scale_operation(
                &link_controls,
                &active_by_node,
                &mut controls,
                &stages,
                output_link,
                global_worker_cap,
            ) {
                match operation {
                    ScaleOperation::Add { node_index } => {
                        let worker_id = match parked.pop() {
                            Some(worker_id) => worker_id,
                            None => {
                                let name = format!("piper-worker-{}", workers.len());
                                spawn_worker(&mut workers, worker_event_sender.clone(), &name)?
                            }
                        };
                        assign_worker(
                            worker_id,
                            node_index,
                            &stages,
                            &links,
                            &config,
                            input_link,
                            &shutdown,
                            &abort,
                            &internal_failure_sender,
                            &link_queue_lens,
                            &mut workers,
                            &mut active_by_node,
                        )?;
                        pending_scale = Some(PendingScale::Add {
                            worker_id,
                            node_index,
                        });

                        while parked.len() < parked_target {
                            let name = format!("piper-worker-{}", workers.len());
                            let worker_id =
                                spawn_worker(&mut workers, worker_event_sender.clone(), &name)?;
                            parked.push(worker_id);
                        }
                    }
                    ScaleOperation::Remove {
                        node_index,
                        worker_id,
                    } => {
                        if let Some(worker_id) =
                            worker_id.or_else(|| active_by_node[node_index].first().copied())
                        {
                            if node_has_merge_state(&stages[node_index])
                                && active_by_node[node_index].len() > 1
                            {
                                if let Some(target_worker_id) = active_by_node[node_index]
                                    .iter()
                                    .copied()
                                    .find(|candidate| *candidate != worker_id)
                                {
                                    if let Some(retire) = workers[worker_id].retire.as_ref() {
                                        let (state_sender, state_receiver) =
                                            channel::unbounded::<Message>();
                                        retire.request_transfer(state_sender);
                                        pending_scale = Some(PendingScale::MergeDown {
                                            source_worker_id: worker_id,
                                            target_worker_id,
                                            node_index,
                                            state_receiver: Some(state_receiver),
                                            waiting_for_target: false,
                                        });
                                        controls[node_index].scaling_state =
                                            NodeScalingState::Merging;
                                    }
                                }
                            } else if let Some(retire) = workers[worker_id].retire.as_ref() {
                                retire.request_cleanup();
                                pending_scale = Some(PendingScale::Remove {
                                    worker_id,
                                    node_index,
                                });
                            }
                        }
                    }
                }
            }
        }

        update_snapshot(
            &snapshot,
            &links,
            &stages,
            &active_by_node,
            parked.len(),
            &link_controls,
            &controls,
            output_link,
            global_worker_cap,
            shutdown.load(Ordering::Acquire),
            abort.load(Ordering::Acquire),
            pending_scale.is_some(),
        );

        let active_count: usize = active_by_node.iter().map(Vec::len).sum();
        if (shutdown.load(Ordering::Acquire)
            || abort.load(Ordering::Acquire)
            || stored_failure.is_some())
            && active_count == 0
        {
            drain_internal_failures(&internal_failure_receiver, &abort, &mut stored_failure);
            break;
        }

        thread::sleep(config.sample_interval);
    }

    for worker_id in parked.drain(..) {
        let _ = workers[worker_id].command.send(WorkerCommand::Stop);
    }

    for worker in &mut workers {
        if let Some(handle) = worker.handle.take() {
            handle
                .join()
                .map_err(|payload| PiperError::WorkerPanicked {
                    worker: worker.name.clone(),
                    message: panic_payload_to_string(payload),
                })?;
        }
    }

    update_snapshot(
        &snapshot,
        &links,
        &stages,
        &active_by_node,
        0,
        &link_controls,
        &controls,
        output_link,
        global_worker_cap,
        shutdown.load(Ordering::Acquire),
        abort.load(Ordering::Acquire),
        false,
    );

    if let Some(recorder) = csv_recorder.take() {
        recorder.stop()?;
    }

    if let Some(failure) = stored_failure {
        Err(failure)
    } else {
        Ok(SupervisorResult { returned_states })
    }
}

fn drain_internal_failures<E>(
    receiver: &channel::Receiver<InternalFailure>,
    abort: &Arc<AtomicBool>,
    stored_failure: &mut Option<PiperError<E>>,
) where
    E: Debug + Display + Send + 'static,
{
    while let Ok(failure) = receiver.try_recv() {
        *stored_failure = Some(match failure {
            InternalFailure::Internal { message } => PiperError::Internal {
                worker: "piper-supervisor".to_string(),
                message,
            },
            InternalFailure::Telemetry { message } => PiperError::Telemetry { message },
            InternalFailure::External { node, error } => match error.downcast::<E>() {
                Ok(error) => PiperError::ExternalNode {
                    node,
                    error: *error,
                },
                Err(_) => PiperError::Internal {
                    worker: "piper-supervisor".to_string(),
                    message: format!("external node `{node}` reported an invalid error type"),
                },
            },
        });
        abort.store(true, Ordering::Release);
    }
}

struct SupervisorResult {
    returned_states: Vec<Message>,
}

fn spawn_worker<E>(
    workers: &mut Vec<WorkerSlot<E>>,
    event_sender: channel::Sender<WorkerEvent<E>>,
    name: &str,
) -> Result<usize, E>
where
    E: Debug + Display + Send + 'static,
{
    let worker_id = workers.len();
    let (command_sender, command_receiver) = channel::unbounded::<WorkerCommand<E>>();
    let thread_name = name.to_string();
    let worker_thread_name = thread_name.clone();
    let handle = thread::Builder::new()
        .name(thread_name.clone())
        .spawn(move || {
            worker_loop(
                worker_id,
                worker_thread_name,
                command_receiver,
                event_sender,
            )
        })
        .map_err(|source| PiperError::SpawnFailed {
            worker: name.to_string(),
            source,
        })?;
    workers.push(WorkerSlot {
        name: name.to_string(),
        command: command_sender,
        handle: Some(handle),
        active_node: None,
        retire: None,
        merge_pending: None,
        merge_sender: None,
        stats: Arc::new(WorkerStats::default()),
    });
    Ok(worker_id)
}

fn parked_worker_target<E>(nodes: &[RuntimeNode<E>]) -> usize
where
    E: Debug + Display + Send + 'static,
{
    nodes
        .iter()
        .filter(|node| {
            !node.is_external
                && node.weighted_branch_config.is_none()
                && !matches!(node.thread_policy, ResolvedThreadPolicy::Fixed(_))
        })
        .count()
}

#[allow(clippy::too_many_arguments)]
fn assign_worker<E>(
    worker_id: usize,
    node_index: usize,
    stages: &[RuntimeNode<E>],
    links: &[Link],
    config: &PiperConfig,
    input_link: usize,
    shutdown: &Arc<AtomicBool>,
    abort: &Arc<AtomicBool>,
    internal_failure: &channel::Sender<InternalFailure>,
    link_queue_lens: &[Arc<AtomicUsize>],
    workers: &mut [WorkerSlot<E>],
    active_by_node: &mut [Vec<usize>],
) -> Result<(), E>
where
    E: Debug + Display + Send + 'static,
{
    let retire = Arc::new(RetireControl::new());
    workers[worker_id].stats.reset();
    let stage = &stages[node_index];
    if let Some(branch_config) = stage.weighted_branch_config {
        let left_link = stage.output_links[0];
        let right_link = stage.output_links[1];
        let assignment = WeightedBranchAssignment {
            node_index,
            input: links[stage.input_link].make_input(),
            input_stats: Arc::clone(&links[stage.input_link].stats),
            left_output: links[left_link].make_output(),
            left_output_stats: Arc::clone(&links[left_link].stats),
            right_output: links[right_link].make_output(),
            right_output_stats: Arc::clone(&links[right_link].stats),
            left_queue_len: Arc::clone(&link_queue_lens[left_link]),
            right_queue_len: Arc::clone(&link_queue_lens[right_link]),
            controller: WeightedBranchController::new(branch_config),
            sample_interval: config.sample_interval,
            is_input_stage: stage.input_link == input_link,
            retire: Arc::clone(&retire),
            shutdown: Arc::clone(shutdown),
            abort: Arc::clone(abort),
            poll_interval: config.poll_interval,
            internal_failure: internal_failure.clone(),
            stats: Arc::clone(&workers[worker_id].stats),
        };
        workers[worker_id].active_node = Some(node_index);
        workers[worker_id].retire = Some(retire);
        workers[worker_id].merge_pending = None;
        workers[worker_id].merge_sender = None;
        active_by_node[node_index].push(worker_id);
        return workers[worker_id]
            .command
            .send(WorkerCommand::RunWeightedBranch(assignment))
            .map_err(|_| PiperError::Internal {
                worker: workers[worker_id].name.clone(),
                message: "worker command channel closed".to_string(),
            });
    }

    let stage_impl = stage.stage.as_ref().ok_or_else(|| PiperError::Internal {
        worker: workers[worker_id].name.clone(),
        message: "cannot assign a managed worker to an external node".to_string(),
    })?;
    let output_link = stage.output_links[0];
    let output = links[output_link].make_output();
    let (merge_sender, merge_receiver) = channel::unbounded::<MergeRequest>();
    let merge_pending = Arc::new(AtomicBool::new(false));
    let assignment = WorkerAssignment {
        node_index,
        stage: Arc::clone(stage_impl),
        initial_state: None,
        input: links[stage.input_link].make_input(),
        input_stats: Arc::clone(&links[stage.input_link].stats),
        output,
        output_stats: Arc::clone(&links[output_link].stats),
        output_acquire: stage.output_acquire.clone(),
        is_input_stage: stage.input_link == input_link,
        retire: Arc::clone(&retire),
        merge_pending: Arc::clone(&merge_pending),
        merge_requests: merge_receiver,
        shutdown: Arc::clone(shutdown),
        abort: Arc::clone(abort),
        poll_interval: config.poll_interval,
        internal_failure: internal_failure.clone(),
        stats: Arc::clone(&workers[worker_id].stats),
        return_state_on_exit: stage.return_state_on_exit,
    };
    workers[worker_id].active_node = Some(node_index);
    workers[worker_id].retire = Some(retire);
    workers[worker_id].merge_pending = Some(merge_pending);
    workers[worker_id].merge_sender = Some(merge_sender);
    active_by_node[node_index].push(worker_id);
    workers[worker_id]
        .command
        .send(WorkerCommand::Run(assignment))
        .map_err(|_| PiperError::Internal {
            worker: workers[worker_id].name.clone(),
            message: "worker command channel closed".to_string(),
        })
}

fn worker_loop<E>(
    worker_id: usize,
    worker_name: String,
    command_receiver: channel::Receiver<WorkerCommand<E>>,
    event_sender: channel::Sender<WorkerEvent<E>>,
) where
    E: Debug + Display + Send + 'static,
{
    while let Ok(command) = command_receiver.recv() {
        match command {
            WorkerCommand::Run(assignment) => {
                run_assignment(worker_id, &worker_name, assignment, &event_sender);
            }
            WorkerCommand::RunWeightedBranch(assignment) => {
                run_weighted_branch_assignment(worker_id, &worker_name, assignment, &event_sender);
            }
            WorkerCommand::Stop => break,
        }
    }
    let _ = event_sender.send(WorkerEvent::Stopped);
}

fn run_assignment<E>(
    worker_id: usize,
    worker_name: &str,
    assignment: WorkerAssignment<E>,
    event_sender: &channel::Sender<WorkerEvent<E>>,
) where
    E: Debug + Display + Send + 'static,
{
    let node_index = assignment.node_index;
    let mut state = match assignment.initial_state {
        Some(state) => state,
        None => match assignment.stage.init_box() {
            Ok(state) => state,
            Err(failure) => {
                let _ = event_sender.send(WorkerEvent::Failed {
                    worker_id,
                    node_index,
                    worker: worker_name.to_string(),
                    failure,
                });
                return;
            }
        },
    };

    let _ = event_sender.send(WorkerEvent::Started { worker_id });

    let mut exit = WorkerExit::Graceful;

    loop {
        if assignment.abort.load(Ordering::Acquire) {
            exit = WorkerExit::Abort;
            break;
        }

        if let Err(failure) = drain_merge_requests(
            worker_id,
            node_index,
            assignment.stage.as_ref(),
            state.as_mut(),
            &assignment.merge_pending,
            &assignment.merge_requests,
            event_sender,
        ) {
            let _ = event_sender.send(WorkerEvent::Failed {
                worker_id,
                node_index,
                worker: worker_name.to_string(),
                failure,
            });
            return;
        }

        if let Some(retire) = assignment.retire.take_request() {
            exit = WorkerExit::Retire(retire);
            break;
        }
        let wait_started = Instant::now();
        match assignment.input.recv_poll(assignment.poll_interval) {
            RecvPoll::Item(input) => {
                assignment.stats.wait_nanos.fetch_add(
                    duration_nanos_u64(wait_started.elapsed()),
                    Ordering::Relaxed,
                );
                assignment
                    .input_stats
                    .drains
                    .fetch_add(1, Ordering::Relaxed);
                let ctx = RuntimeNodeContext {
                    output: assignment.output.clone(),
                    output_stats: Arc::clone(&assignment.output_stats),
                    output_acquire: assignment.output_acquire.clone(),
                    shutdown: Arc::clone(&assignment.shutdown),
                    abort: Arc::clone(&assignment.abort),
                    internal_failure: assignment.internal_failure.clone(),
                };
                let process_started = Instant::now();
                let result = assignment.stage.process_box(state.as_mut(), input, ctx);
                assignment.stats.process_nanos.fetch_add(
                    duration_nanos_u64(process_started.elapsed()),
                    Ordering::Relaxed,
                );
                if let Err(failure) = result {
                    let _ = event_sender.send(WorkerEvent::Failed {
                        worker_id,
                        node_index,
                        worker: worker_name.to_string(),
                        failure,
                    });
                    return;
                }
                assignment
                    .stats
                    .processed_items
                    .fetch_add(1, Ordering::Relaxed);
            }
            RecvPoll::Timeout => {
                assignment.stats.wait_nanos.fetch_add(
                    duration_nanos_u64(wait_started.elapsed()),
                    Ordering::Relaxed,
                );
                if assignment.abort.load(Ordering::Acquire) {
                    exit = WorkerExit::Abort;
                    break;
                }
                if assignment.input.is_terminated()
                    || (assignment.shutdown.load(Ordering::Acquire)
                        && assignment
                            .input
                            .should_stop_on_shutdown_timeout(assignment.is_input_stage))
                {
                    break;
                }
            }
            RecvPoll::Disconnected => {
                assignment.stats.wait_nanos.fetch_add(
                    duration_nanos_u64(wait_started.elapsed()),
                    Ordering::Relaxed,
                );
                if assignment.abort.load(Ordering::Acquire) {
                    exit = WorkerExit::Abort;
                }
                break;
            }
        }
    }

    match exit {
        WorkerExit::Abort => {}
        WorkerExit::Graceful if assignment.return_state_on_exit => {
            let _ = event_sender.send(WorkerEvent::Parked {
                worker_id,
                node_index,
                returned_state: Some(state),
            });
            return;
        }
        WorkerExit::Retire(RetireAction::Transfer(sender)) => {
            if sender.send(state).is_err() {
                let _ = event_sender.send(WorkerEvent::Failed {
                    worker_id,
                    node_index,
                    worker: worker_name.to_string(),
                    failure: NodeFailure::Internal("state transfer receiver closed".to_string()),
                });
                return;
            }
        }
        WorkerExit::Graceful | WorkerExit::Retire(RetireAction::Cleanup) => {
            if let Err(failure) = assignment.stage.cleanup_box(state) {
                let _ = event_sender.send(WorkerEvent::Failed {
                    worker_id,
                    node_index,
                    worker: worker_name.to_string(),
                    failure,
                });
                return;
            }
        }
    }

    let _ = event_sender.send(WorkerEvent::Parked {
        worker_id,
        node_index,
        returned_state: None,
    });
}

enum WorkerExit {
    Graceful,
    Abort,
    Retire(RetireAction),
}

fn drain_merge_requests<E>(
    worker_id: usize,
    node_index: usize,
    stage: &dyn DynNode<E>,
    state: &mut dyn Any,
    merge_pending: &AtomicBool,
    merge_requests: &channel::Receiver<MergeRequest>,
    event_sender: &channel::Sender<WorkerEvent<E>>,
) -> std::result::Result<(), NodeFailure<E>>
where
    E: Debug + Display + Send + 'static,
{
    if !merge_pending.swap(false, Ordering::AcqRel) {
        return Ok(());
    }

    loop {
        let request = match merge_requests.try_recv() {
            Ok(request) => request,
            Err(channel::TryRecvError::Empty) => return Ok(()),
            Err(channel::TryRecvError::Closed) => {
                return Err(NodeFailure::Internal(
                    "worker merge request channel closed".to_string(),
                ));
            }
        };
        let source_worker_id = request.source_worker_id;
        let source_state = request.state_receiver.recv().map_err(|_| {
            NodeFailure::Internal("worker state transfer channel closed".to_string())
        })?;
        stage.merge_box(state, source_state)?;
        let _ = event_sender.send(WorkerEvent::MergeCompleted {
            worker_id,
            node_index,
            source_worker_id,
        });
    }
}

fn run_weighted_branch_assignment<E>(
    worker_id: usize,
    _worker_name: &str,
    mut assignment: WeightedBranchAssignment,
    event_sender: &channel::Sender<WorkerEvent<E>>,
) where
    E: Debug + Display + Send + 'static,
{
    let node_index = assignment.node_index;
    let _ = event_sender.send(WorkerEvent::Started { worker_id });
    let mut last_controller_sample =
        Instant::now() - assignment.sample_interval - Duration::from_micros(1);

    loop {
        if assignment.abort.load(Ordering::Acquire) {
            break;
        }
        if assignment.retire.take_request().is_some() {
            break;
        }
        let wait_started = Instant::now();
        match assignment.input.recv_poll(assignment.poll_interval) {
            RecvPoll::Item(message) => {
                assignment.stats.wait_nanos.fetch_add(
                    duration_nanos_u64(wait_started.elapsed()),
                    Ordering::Relaxed,
                );
                assignment
                    .input_stats
                    .drains
                    .fetch_add(1, Ordering::Relaxed);

                let now = Instant::now();
                let elapsed = now.duration_since(last_controller_sample);
                if !assignment.controller.arms_ready() || elapsed >= assignment.sample_interval {
                    last_controller_sample = now;
                    assignment.controller.sample_arms(
                        elapsed.as_secs_f64().max(0.000_001),
                        &assignment.left_output_stats,
                        &assignment.right_output_stats,
                        assignment.left_queue_len.load(Ordering::Relaxed),
                        assignment.right_queue_len.load(Ordering::Relaxed),
                    );
                }

                let route_left = assignment.controller.choose_route(
                    assignment.left_queue_len.load(Ordering::Relaxed),
                    assignment.right_queue_len.load(Ordering::Relaxed),
                );
                let (output, output_stats) = if route_left {
                    (&assignment.left_output, &assignment.left_output_stats)
                } else {
                    (&assignment.right_output, &assignment.right_output_stats)
                };

                match output.send(message) {
                    Ok(()) => {
                        output_stats.arrivals.fetch_add(1, Ordering::Relaxed);
                        assignment
                            .stats
                            .processed_items
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_)
                        if !assignment.shutdown.load(Ordering::Acquire)
                            && !assignment.abort.load(Ordering::Acquire) =>
                    {
                        assignment.abort.store(true, Ordering::Release);
                        let _ = assignment.internal_failure.send(InternalFailure::internal(
                            "node output channel closed unexpectedly",
                        ));
                    }
                    Err(_) => {}
                }
            }
            RecvPoll::Timeout => {
                assignment.stats.wait_nanos.fetch_add(
                    duration_nanos_u64(wait_started.elapsed()),
                    Ordering::Relaxed,
                );
                if assignment.input.is_terminated()
                    || (assignment.shutdown.load(Ordering::Acquire)
                        && assignment
                            .input
                            .should_stop_on_shutdown_timeout(assignment.is_input_stage))
                {
                    break;
                }
            }
            RecvPoll::Disconnected => {
                assignment.stats.wait_nanos.fetch_add(
                    duration_nanos_u64(wait_started.elapsed()),
                    Ordering::Relaxed,
                );
                break;
            }
        }
    }

    let _ = event_sender.send(WorkerEvent::Parked {
        worker_id,
        node_index,
        returned_state: None,
    });
}

#[allow(clippy::too_many_arguments)]
fn drain_worker_events<E>(
    receiver: &channel::Receiver<WorkerEvent<E>>,
    workers: &mut [WorkerSlot<E>],
    active_by_node: &mut [Vec<usize>],
    parked: &mut Vec<usize>,
    controls: &mut [NodeControl],
    pending_scale: &mut Option<PendingScale>,
    abort: &Arc<AtomicBool>,
    stored_failure: &mut Option<PiperError<E>>,
    returned_states: &mut Vec<Message>,
) where
    E: Debug + Display + Send + 'static,
{
    while let Ok(event) = receiver.try_recv() {
        match event {
            WorkerEvent::Started { worker_id, .. } => {
                if matches!(
                    pending_scale,
                    Some(PendingScale::Add {
                        worker_id: pending_worker,
                        ..
                    }) if *pending_worker == worker_id
                ) {
                    if let Some(PendingScale::Add { node_index, .. }) = pending_scale.take() {
                        record_node_operation(controls, node_index, ScaleDirection::Add);
                        mark_node_settling(controls, node_index);
                    }
                }
            }
            WorkerEvent::Parked {
                worker_id,
                node_index,
                returned_state,
            } => {
                remove_worker_from_node(active_by_node, node_index, worker_id);
                workers[worker_id].active_node = None;
                workers[worker_id].retire = None;
                workers[worker_id].merge_pending = None;
                workers[worker_id].merge_sender = None;
                parked.push(worker_id);
                if let Some(state) = returned_state {
                    returned_states.push(state);
                }

                match pending_scale {
                    Some(PendingScale::Remove {
                        worker_id: pending_worker,
                        ..
                    }) if *pending_worker == worker_id => {
                        if let Some(PendingScale::Remove { node_index, .. }) = pending_scale.take()
                        {
                            record_node_operation(controls, node_index, ScaleDirection::Remove);
                            mark_node_settling(controls, node_index);
                        }
                    }
                    Some(PendingScale::MergeDown {
                        source_worker_id,
                        target_worker_id,
                        node_index,
                        state_receiver,
                        waiting_for_target,
                        ..
                    }) if *source_worker_id == worker_id && !*waiting_for_target => {
                        let Some(receiver) = state_receiver.take() else {
                            abort.store(true, Ordering::Release);
                            *stored_failure = Some(PiperError::Internal {
                                worker: "piper-supervisor".to_string(),
                                message: "missing merge state receiver".to_string(),
                            });
                            continue;
                        };
                        let Some(sender) = workers[*target_worker_id].merge_sender.as_ref() else {
                            abort.store(true, Ordering::Release);
                            *stored_failure = Some(PiperError::Internal {
                                worker: "piper-supervisor".to_string(),
                                message: "merge target worker is not active".to_string(),
                            });
                            continue;
                        };
                        if sender
                            .send(MergeRequest {
                                source_worker_id: worker_id,
                                state_receiver: receiver,
                            })
                            .is_err()
                        {
                            abort.store(true, Ordering::Release);
                            *stored_failure = Some(PiperError::Internal {
                                worker: "piper-supervisor".to_string(),
                                message: "merge target request channel closed".to_string(),
                            });
                            continue;
                        }
                        if let Some(flag) = workers[*target_worker_id].merge_pending.as_ref() {
                            flag.store(true, Ordering::Release);
                            *waiting_for_target = true;
                            controls[*node_index].scaling_state = NodeScalingState::Merging;
                        } else {
                            abort.store(true, Ordering::Release);
                            *stored_failure = Some(PiperError::Internal {
                                worker: "piper-supervisor".to_string(),
                                message: "merge target pending flag is unavailable".to_string(),
                            });
                        }
                    }
                    _ => {}
                }
            }
            WorkerEvent::MergeCompleted {
                worker_id,
                node_index,
                source_worker_id,
            } => {
                if matches!(
                    pending_scale,
                    Some(PendingScale::MergeDown {
                        source_worker_id: pending_source,
                        target_worker_id,
                        waiting_for_target,
                        ..
                    }) if *pending_source == source_worker_id
                        && *target_worker_id == worker_id
                        && *waiting_for_target
                ) {
                    let _ = pending_scale.take();
                    record_node_operation(controls, node_index, ScaleDirection::Merge);
                    mark_node_settling(controls, node_index);
                }
            }
            WorkerEvent::Failed {
                worker_id,
                node_index,
                worker,
                failure,
            } => {
                remove_worker_from_node(active_by_node, node_index, worker_id);
                workers[worker_id].active_node = None;
                workers[worker_id].retire = None;
                workers[worker_id].merge_pending = None;
                workers[worker_id].merge_sender = None;
                if !parked.contains(&worker_id) {
                    parked.push(worker_id);
                }
                if pending_scale_involves_worker(pending_scale.as_ref(), worker_id) {
                    *pending_scale = None;
                }
                abort.store(true, Ordering::Release);
                *stored_failure = Some(match failure {
                    NodeFailure::Init(error) => PiperError::UserInit { worker, error },
                    NodeFailure::Process(error) => PiperError::UserProcess { worker, error },
                    NodeFailure::Cleanup(error) => PiperError::UserCleanup { worker, error },
                    NodeFailure::Merge(error) => PiperError::UserMerge { worker, error },
                    NodeFailure::Internal(message) => PiperError::Internal { worker, message },
                });
            }
            WorkerEvent::Stopped => {}
        }
    }
}

fn mark_node_settling(controls: &mut [NodeControl], node_index: usize) {
    controls[node_index].settling = true;
    controls[node_index].settle_samples = 0;
    controls[node_index].settle_observed_work = false;
    controls[node_index].scaling_state = NodeScalingState::Settling;
}

fn pending_scale_involves_worker(pending: Option<&PendingScale>, worker_id: usize) -> bool {
    match pending {
        Some(PendingScale::Add {
            worker_id: pending, ..
        })
        | Some(PendingScale::Remove {
            worker_id: pending, ..
        }) => *pending == worker_id,
        Some(PendingScale::MergeDown {
            source_worker_id,
            target_worker_id,
            ..
        }) => *source_worker_id == worker_id || *target_worker_id == worker_id,
        None => false,
    }
}

fn remove_worker_from_node(
    active_by_node: &mut [Vec<usize>],
    node_index: usize,
    worker_id: usize,
) {
    if let Some(position) = active_by_node[node_index]
        .iter()
        .position(|id| *id == worker_id)
    {
        active_by_node[node_index].swap_remove(position);
    }
}

fn record_node_operation(
    controls: &mut [NodeControl],
    node_index: usize,
    direction: ScaleDirection,
) {
    controls[node_index].last_operation = Some((direction, Instant::now()));
}

const RATE_EWMA_ALPHA: f64 = 0.35;
const SERVICE_EWMA_ALPHA: f64 = 0.30;
const STABLE_RATE_RATIO: f64 = 0.12;
const FAST_RATE_RATIO: f64 = 0.45;
const RUNAWAY_RATE_RATIO: f64 = 0.85;
const SETTLE_SAMPLES: u32 = 2;
const DEFAULT_BACKLOG_DRAIN_SECS: f64 = 1.0;

fn sample_links(links: &[Link], elapsed: Duration, controls: &mut [LinkControl]) {
    let seconds = elapsed.as_secs_f64().max(0.000_001);
    for (index, link) in links.iter().enumerate() {
        let arrivals = link.stats.arrivals.load(Ordering::Relaxed);
        let drains = link.stats.drains.load(Ordering::Relaxed);
        let delta_arrivals = arrivals.saturating_sub(controls[index].last_arrivals);
        let delta_drains = drains.saturating_sub(controls[index].last_drains);
        let len = link.queue_len().unwrap_or(0);
        let previous_len = controls[index].len;
        let arrival_rate = delta_arrivals as f64 / seconds;
        let drain_rate = delta_drains as f64 / seconds;
        let net_rate = arrival_rate - drain_rate;

        controls[index].last_arrivals = arrivals;
        controls[index].last_drains = drains;
        controls[index].previous_len = previous_len;
        controls[index].len = len;
        controls[index].arrival_rate =
            ewma(controls[index].arrival_rate, arrival_rate, RATE_EWMA_ALPHA);
        controls[index].drain_rate = ewma(controls[index].drain_rate, drain_rate, RATE_EWMA_ALPHA);
        controls[index].net_rate = ewma(controls[index].net_rate, net_rate, RATE_EWMA_ALPHA);
        controls[index].smoothed_len = ewma(controls[index].smoothed_len, len as f64, 0.25);
        controls[index].trend = classify_queue_trend(
            len,
            previous_len,
            controls[index].arrival_rate,
            controls[index].drain_rate,
            controls[index].net_rate,
        );
    }
}

fn ewma(previous: f64, sample: f64, alpha: f64) -> f64 {
    if previous == 0.0 {
        sample
    } else {
        (previous * (1.0 - alpha)) + (sample * alpha)
    }
}

fn classify_queue_trend(
    len: usize,
    previous_len: usize,
    arrival_rate: f64,
    drain_rate: f64,
    net_rate: f64,
) -> QueueTrend {
    let total_rate = arrival_rate + drain_rate;
    if len == 0 && total_rate < 0.01 {
        return QueueTrend::Starved;
    }
    if len == 0 && previous_len == 0 {
        return QueueTrend::Stable;
    }

    let basis = arrival_rate.max(drain_rate).max(1.0);
    let ratio = net_rate / basis;
    let length_delta = len as isize - previous_len as isize;

    if ratio <= -FAST_RATE_RATIO {
        QueueTrend::FastDraining
    } else if ratio <= -STABLE_RATE_RATIO {
        QueueTrend::Draining
    } else if ratio >= RUNAWAY_RATE_RATIO && length_delta > 0 {
        QueueTrend::Runaway
    } else if ratio >= FAST_RATE_RATIO {
        QueueTrend::FastGrowing
    } else if ratio >= STABLE_RATE_RATIO || length_delta > 2 {
        QueueTrend::Growing
    } else {
        QueueTrend::Stable
    }
}

fn effective_backlog_len(link: &LinkControl) -> f64 {
    if link.len == 0 {
        return 0.0;
    }
    let smoothed = if link.smoothed_len.is_finite() && link.smoothed_len > 0.0 {
        link.smoothed_len.ceil()
    } else {
        0.0
    };
    link.len.max(smoothed as usize) as f64
}

fn collect_node_samples<E>(
    workers: &[WorkerSlot<E>],
    active_by_node: &[Vec<usize>],
    links: &[LinkControl],
    nodes: &[RuntimeNode<E>],
    controls: &mut [NodeControl],
    sample_elapsed: Duration,
) where
    E: Debug + Display + Send + 'static,
{
    let now = Instant::now();
    for (node_index, worker_ids) in active_by_node.iter().enumerate() {
        let mut sample = NodeSample::default();
        for worker_id in worker_ids {
            let stats = &workers[*worker_id].stats;
            sample.process_nanos += stats.process_nanos.swap(0, Ordering::Relaxed);
            sample.wait_nanos += stats.wait_nanos.swap(0, Ordering::Relaxed);
            sample.processed_items += stats.processed_items.swap(0, Ordering::Relaxed);
        }

        let control = &mut controls[node_index];
        control.last_sample_processed = sample.processed_items;
        control.processed_count = control
            .processed_count
            .saturating_add(sample.processed_items);

        let total_nanos = sample.process_nanos.saturating_add(sample.wait_nanos);
        if total_nanos > 0 {
            control.busy_ratio = sample.process_nanos as f64 / total_nanos as f64;
        }

        if sample.processed_items > 0 && sample.process_nanos > 0 {
            let service_time = sample.process_nanos as f64 / sample.processed_items as f64;
            control.service_time_ewma =
                ewma(control.service_time_ewma, service_time, SERVICE_EWMA_ALPHA);
            control.per_worker_throughput = 1_000_000_000.0 / control.service_time_ewma.max(1.0);
        }

        if control.settling {
            control.settle_samples = control.settle_samples.saturating_add(1);
            control.settle_observed_work |= sample.processed_items > 0;
            if control.settle_samples >= SETTLE_SAMPLES && control.settle_observed_work {
                control.settling = false;
                control.scaling_state = NodeScalingState::Eligible;
            }
        }

        if let ResolvedThreadPolicy::Scalable(policy) = control.thread_policy {
            let throughput = control.per_worker_throughput;
            let input = &links[nodes[node_index].input_link];
            control.backlog_seconds = if throughput > 0.0 {
                effective_backlog_len(input) / throughput
            } else {
                0.0
            };
            let underutilized = control.backlog_seconds <= policy.low_queue_seconds
                && control.busy_ratio < policy.underutilized_busy_ratio;
            if underutilized {
                let entry = control.scale_down_eligible_since.get_or_insert(now);
                if now.duration_since(*entry) < policy.scale_down_after {
                    let _ = sample_elapsed;
                }
            } else {
                control.scale_down_eligible_since = None;
            }
        }
    }
}

fn update_desired_workers<E>(
    links: &[LinkControl],
    active_by_node: &[Vec<usize>],
    nodes: &[RuntimeNode<E>],
    controls: &mut [NodeControl],
) where
    E: Debug + Display + Send + 'static,
{
    for node_index in 0..controls.len() {
        if controls[node_index].is_external {
            controls[node_index].desired_workers = 0;
            continue;
        }
        if controls[node_index].is_weighted_branch {
            controls[node_index].desired_workers = 1;
            continue;
        }

        let active = active_by_node[node_index].len().max(1);
        let input = &links[nodes[node_index].input_link];
        let throughput = controls[node_index].per_worker_throughput;

        controls[node_index].desired_workers = match controls[node_index].thread_policy {
            ResolvedThreadPolicy::Fixed(count) => count,
            ResolvedThreadPolicy::Scalable(policy) => {
                if throughput > 0.0 {
                    let backlog_len = effective_backlog_len(input);
                    controls[node_index].backlog_seconds = backlog_len / throughput;
                    let workers_for_arrivals =
                        (input.arrival_rate / throughput).ceil().max(1.0) as usize;
                    let workers_for_backlog = (backlog_len
                        / (policy.target_queue_seconds * throughput))
                        .ceil()
                        .max(1.0) as usize;
                    workers_for_arrivals
                        .max(workers_for_backlog)
                        .clamp(1, policy.max_threads)
                } else if input.trend.is_growing() {
                    active.saturating_add(1).clamp(1, policy.max_threads)
                } else {
                    active.max(1)
                }
            }
            ResolvedThreadPolicy::ImplicitSupport => {
                if throughput > 0.0 {
                    let mut required_rate = input.arrival_rate.max(0.0);
                    if input.trend.is_growing() {
                        required_rate += input.net_rate.max(0.0);
                        required_rate +=
                            effective_backlog_len(input) / DEFAULT_BACKLOG_DRAIN_SECS;
                    }
                    (required_rate / throughput).ceil().max(1.0) as usize
                } else if input.trend.is_growing() {
                    active.saturating_add(1)
                } else {
                    active
                }
                .max(1)
            }
        };
    }
}

enum ScaleOperation {
    Add {
        node_index: usize,
    },
    Remove {
        node_index: usize,
        worker_id: Option<usize>,
    },
}

fn choose_scale_operation<E>(
    links: &[LinkControl],
    active_by_node: &[Vec<usize>],
    controls: &mut [NodeControl],
    nodes: &[RuntimeNode<E>],
    output_link: usize,
    global_worker_cap: usize,
) -> Option<ScaleOperation>
where
    E: Debug + Display + Send + 'static,
{
    if let Some(operation) = choose_scalable_scale_up(
        links,
        active_by_node,
        controls,
        nodes,
        output_link,
        global_worker_cap,
    ) {
        return Some(operation);
    }

    if let Some(operation) = choose_support_operation(
        links,
        active_by_node,
        controls,
        nodes,
        output_link,
        global_worker_cap,
    ) {
        return Some(operation);
    }

    if let Some(operation) = choose_anchor_pressure_operation(
        links,
        active_by_node,
        controls,
        nodes,
        output_link,
        global_worker_cap,
    ) {
        return Some(operation);
    }

    choose_scale_down_operation(links, active_by_node, controls, nodes, output_link)
}

fn downstream_blocks_scale_up<E>(
    links: &[LinkControl],
    nodes: &[RuntimeNode<E>],
    node_index: usize,
    output_link: usize,
) -> bool
where
    E: Debug + Display + Send + 'static,
{
    links[output_link].trend.is_growing()
        || links[nodes[node_index].output_links[0]].trend.is_growing()
}

fn choose_scalable_scale_up<E>(
    links: &[LinkControl],
    active_by_node: &[Vec<usize>],
    controls: &mut [NodeControl],
    nodes: &[RuntimeNode<E>],
    output_link: usize,
    global_worker_cap: usize,
) -> Option<ScaleOperation>
where
    E: Debug + Display + Send + 'static,
{
    if active_worker_count(active_by_node) >= global_worker_cap {
        return None;
    }

    for node_index in 0..controls.len() {
        let ResolvedThreadPolicy::Scalable(policy) = controls[node_index].thread_policy else {
            continue;
        };
        if !node_can_scale(&controls[node_index]) {
            continue;
        }
        if downstream_blocks_scale_up(links, nodes, node_index, output_link) {
            continue;
        }
        let active = active_by_node[node_index].len();
        if active >= controls[node_index].desired_workers || active >= policy.max_threads {
            continue;
        }
        let input = &links[nodes[node_index].input_link];
        if controls[node_index].backlog_seconds <= policy.target_queue_seconds
            && !input.trend.is_growing()
        {
            continue;
        }
        return Some(ScaleOperation::Add {
            node_index,
        });
    }

    None
}

fn choose_support_operation<E>(
    links: &[LinkControl],
    active_by_node: &[Vec<usize>],
    controls: &mut [NodeControl],
    nodes: &[RuntimeNode<E>],
    output_link: usize,
    global_worker_cap: usize,
) -> Option<ScaleOperation>
where
    E: Debug + Display + Send + 'static,
{
    if links[output_link].trend.is_growing() {
        return None;
    }

    for link_index in 0..links.len() {
        if !links[link_index].trend.is_growing() {
            continue;
        }
        for consumer_index in consumer_nodes(nodes, link_index) {
            if !matches!(
                controls[consumer_index].thread_policy,
                ResolvedThreadPolicy::ImplicitSupport
            ) {
                continue;
            }
            if active_by_node[consumer_index].len() < controls[consumer_index].desired_workers
                && node_can_scale(&controls[consumer_index])
                && !downstream_blocks_scale_up(links, nodes, consumer_index, output_link)
            {
                return add_or_rebalance_for_node(
                    consumer_index,
                    active_by_node,
                    controls,
                    global_worker_cap,
                );
            }
        }
    }

    None
}

fn choose_anchor_pressure_operation<E>(
    links: &[LinkControl],
    active_by_node: &[Vec<usize>],
    controls: &mut [NodeControl],
    nodes: &[RuntimeNode<E>],
    output_link: usize,
    global_worker_cap: usize,
) -> Option<ScaleOperation>
where
    E: Debug + Display + Send + 'static,
{
    if links[output_link].trend.is_growing() {
        return None;
    }

    for anchor_index in anchor_node_indices(controls).collect::<Vec<_>>() {
        let input_link = nodes[anchor_index].input_link;
        let target_queue = match controls[anchor_index].thread_policy {
            ResolvedThreadPolicy::Scalable(policy) => policy.target_queue_seconds,
            _ => 1.0,
        };
        if controls[anchor_index].busy_ratio <= 0.60
            || !link_underfeeds_node(
                &links[input_link],
                active_by_node[anchor_index].len(),
                controls[anchor_index].backlog_seconds,
                target_queue,
            )
        {
            continue;
        }

        for producer_index in producer_nodes(nodes, input_link) {
            if matches!(
                controls[producer_index].thread_policy,
                ResolvedThreadPolicy::Fixed(_)
            ) {
                continue;
            }
            if links[nodes[producer_index].input_link].trend == QueueTrend::Starved {
                continue;
            }
            if !node_can_scale(&controls[producer_index])
                || downstream_blocks_scale_up(links, nodes, producer_index, output_link)
            {
                continue;
            }
            if active_by_node[producer_index].len() < controls[producer_index].desired_workers {
                controls[anchor_index].last_anchor_pressure_reason =
                    Some(AnchorPressureReason::InputUnderfed);
                return add_or_rebalance_for_node(
                    producer_index,
                    active_by_node,
                    controls,
                    global_worker_cap,
                );
            }
        }
    }

    None
}

fn choose_scale_down_operation<E>(
    links: &[LinkControl],
    active_by_node: &[Vec<usize>],
    controls: &mut [NodeControl],
    nodes: &[RuntimeNode<E>],
    output_link: usize,
) -> Option<ScaleOperation>
where
    E: Debug + Display + Send + 'static,
{
    let now = Instant::now();
    for node_index in 0..controls.len() {
        if active_by_node[node_index].len() <= 1 || !node_can_scale(&controls[node_index]) {
            continue;
        }
        if downstream_blocks_scale_up(links, nodes, node_index, output_link) {
            continue;
        }

        match controls[node_index].thread_policy {
            ResolvedThreadPolicy::Scalable(policy) => {
                let Some(since) = controls[node_index].scale_down_eligible_since else {
                    continue;
                };
                if now.duration_since(since) < policy.scale_down_after {
                    continue;
                }
                if controls[node_index].backlog_seconds > policy.low_queue_seconds {
                    continue;
                }
                return Some(ScaleOperation::Remove {
                    node_index,
                    worker_id: None,
                });
            }
            ResolvedThreadPolicy::ImplicitSupport => {
                if active_by_node[node_index].len()
                    <= controls[node_index].desired_workers.max(1)
                {
                    continue;
                }
                if links[nodes[node_index].input_link].trend.is_growing() {
                    continue;
                }
                return Some(ScaleOperation::Remove {
                    node_index,
                    worker_id: None,
                });
            }
            ResolvedThreadPolicy::Fixed(_) => {}
        }
    }

    None
}

fn link_underfeeds_node(
    link: &LinkControl,
    active_threads: usize,
    backlog_seconds: f64,
    target_queue_seconds: f64,
) -> bool {
    if backlog_seconds > target_queue_seconds {
        return false;
    }
    link.trend == QueueTrend::Starved
        || (link.trend == QueueTrend::FastDraining
            && backlog_seconds <= target_queue_seconds
            && link.len <= active_threads.saturating_mul(2).max(1))
        || (link.trend.is_draining()
            && link.trend != QueueTrend::FastDraining
            && link.len <= active_threads.saturating_mul(2).max(1))
}

fn node_can_scale(control: &NodeControl) -> bool {
    !control.is_external
        && !control.is_weighted_branch
        && !matches!(control.thread_policy, ResolvedThreadPolicy::Fixed(_))
        && !control.settling
        && control.scaling_state != NodeScalingState::Settling
        && control.scaling_state != NodeScalingState::Merging
}

fn node_has_merge_state<E>(node: &RuntimeNode<E>) -> bool
where
    E: Debug + Display + Send + 'static,
{
    node.stage
        .as_ref()
        .is_some_and(|stage| stage.can_merge_state())
}

fn add_or_rebalance_for_node(
    node_index: usize,
    active_by_node: &[Vec<usize>],
    controls: &mut [NodeControl],
    global_worker_cap: usize,
) -> Option<ScaleOperation> {
    if active_worker_count(active_by_node) < global_worker_cap {
        return Some(ScaleOperation::Add {
            node_index,
        });
    }

    for anchor_index in anchor_node_indices(controls).collect::<Vec<_>>() {
        if node_index != anchor_index
            && matches!(
                controls[anchor_index].thread_policy,
                ResolvedThreadPolicy::Scalable(_)
            )
            && active_by_node[anchor_index].len() > 1
            && node_can_scale(&controls[anchor_index])
        {
            controls[anchor_index].last_anchor_pressure_reason =
                Some(AnchorPressureReason::BudgetPressure);
            return Some(ScaleOperation::Remove {
                node_index: anchor_index,
                worker_id: None,
            });
        }
    }

    None
}

fn active_worker_count(active_by_node: &[Vec<usize>]) -> usize {
    active_by_node.iter().map(Vec::len).sum()
}

fn anchor_node_indices(controls: &[NodeControl]) -> impl Iterator<Item = usize> + '_ {
    controls
        .iter()
        .enumerate()
        .filter_map(|(index, control)| control.is_anchor.then_some(index))
}

fn consumer_nodes<E>(
    nodes: &[RuntimeNode<E>],
    link_index: usize,
) -> impl Iterator<Item = usize> + '_
where
    E: Debug + Display + Send + 'static,
{
    nodes
        .iter()
        .enumerate()
        .filter_map(move |(index, node)| (node.input_link == link_index).then_some(index))
}

fn producer_nodes<E>(
    nodes: &[RuntimeNode<E>],
    link_index: usize,
) -> impl Iterator<Item = usize> + '_
where
    E: Debug + Display + Send + 'static,
{
    nodes.iter().enumerate().filter_map(move |(index, node)| {
        node.output_links
            .iter()
            .any(|&output| output == link_index)
            .then_some(index)
    })
}

fn duration_nanos_u64(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}
#[allow(clippy::too_many_arguments)]
fn update_snapshot<E>(
    snapshot: &Arc<RwLock<PiperSnapshot>>,
    links: &[Link],
    stages: &[RuntimeNode<E>],
    active_by_node: &[Vec<usize>],
    parked_threads: usize,
    link_controls: &[LinkControl],
    controls: &[NodeControl],
    output_link: usize,
    global_worker_cap: usize,
    shutdown_requested: bool,
    abort_requested: bool,
    pending_scale_operation: bool,
) where
    E: Debug + Display + Send + 'static,
{
    let mut snapshot = snapshot.write();
    snapshot.links = link_controls
        .iter()
        .enumerate()
        .map(|(index, control)| LinkSnapshot {
            index,
            len: links[index].queue_len().unwrap_or(0),
            trend: control.trend,
            arrival_rate: control.arrival_rate,
            drain_rate: control.drain_rate,
            net_rate: control.net_rate,
            smoothed_len: control.smoothed_len,
        })
        .collect();
    snapshot.nodes = stages
        .iter()
        .enumerate()
        .map(|(index, stage)| {
            let desired_workers = if controls[index].is_weighted_branch {
                1
            } else {
                controls[index].desired_workers
            };
            let active_threads = if controls[index].is_weighted_branch {
                active_by_node[index].len().max(1)
            } else {
                active_by_node[index].len()
            };
            NodeSnapshot {
                index,
                name: stage.name.clone(),
                input_link: stage.input_link,
                output_link: stage.output_links[0],
                output_links: stage.output_links.clone(),
                active_threads,
                processed_count: controls[index].processed_count,
                busy_ratio: controls[index].busy_ratio,
                service_time: Duration::from_nanos(
                    controls[index]
                        .service_time_ewma
                        .max(0.0)
                        .min(u64::MAX as f64) as u64,
                ),
                per_worker_throughput: controls[index].per_worker_throughput,
                desired_workers,
                scaling_state: controls[index].scaling_state,
                is_anchor: controls[index].is_anchor,
                thread_policy_kind: thread_policy_kind(controls[index].thread_policy),
                fixed_thread_count: fixed_thread_count(controls[index].thread_policy),
                max_thread_count: max_thread_count(controls[index].thread_policy),
                target_queue_seconds: target_queue_seconds(controls[index].thread_policy),
                low_queue_seconds: low_queue_seconds(controls[index].thread_policy),
                backlog_seconds: controls[index].backlog_seconds,
                is_external: stage.is_external,
                external_input_rate: if stage.is_external {
                    link_controls[stage.input_link].drain_rate
                } else {
                    0.0
                },
                external_output_rate: if stage.is_external {
                    link_controls[stage.output_links[0]].arrival_rate
                } else {
                    0.0
                },
            }
        })
        .collect();
    snapshot.anchors = controls
        .iter()
        .enumerate()
        .filter_map(|(index, control)| {
            control.is_anchor.then(|| AnchorSnapshot {
                node_index: index,
                node_name: stages[index].name.clone(),
                active_threads: active_by_node[index].len(),
                last_pressure_reason: control.last_anchor_pressure_reason,
            })
        })
        .collect();
    snapshot.parked_threads = parked_threads;
    snapshot.total_active_workers = active_worker_count(active_by_node);
    snapshot.global_worker_cap = global_worker_cap;
    snapshot.budget_pressure = snapshot.total_active_workers >= global_worker_cap;
    snapshot.output_backpressure = link_controls[output_link].trend.is_growing();
    snapshot.shutdown_requested = shutdown_requested;
    snapshot.abort_requested = abort_requested;
    snapshot.pending_scale_operation = pending_scale_operation;
}

pub fn panic_payload_to_string(payload: Box<dyn Any + Send + 'static>) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else {
        String::from("<non-string panic payload>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Error)]
    enum TestError {
        #[error("boom")]
        Boom,
    }

    struct TestNode;

    impl Node for TestNode {
        type Input = u8;
        type Output = u8;
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

    fn test_config() -> PiperConfig {
        PiperConfig {
            sample_interval: Duration::from_millis(1),
            poll_interval: Duration::from_millis(1),
            global_worker_cap: Some(8),
            csv_telemetry: None,
        }
    }

    fn support_control() -> NodeControl {
        NodeControl {
            is_external: false,
            is_weighted_branch: false,
            is_anchor: false,
            thread_policy: ResolvedThreadPolicy::ImplicitSupport,
            processed_count: 0,
            busy_ratio: 0.0,
            service_time_ewma: 1_000_000.0,
            per_worker_throughput: 1_000.0,
            desired_workers: 1,
            backlog_seconds: 0.0,
            last_sample_processed: 0,
            scaling_state: NodeScalingState::Eligible,
            settling: false,
            settle_samples: 0,
            settle_observed_work: false,
            scale_down_eligible_since: None,
            last_anchor_pressure_reason: None,
            last_operation: None,
        }
    }

    fn scalable_control(max_threads: usize) -> NodeControl {
        NodeControl {
            is_anchor: true,
            thread_policy: ResolvedThreadPolicy::Scalable(ResolvedScalePolicy {
                initial_threads: 1,
                max_threads,
                target_queue_seconds: 1.0,
                low_queue_seconds: 0.25,
                scale_down_after: Duration::from_millis(500),
                underutilized_busy_ratio: 0.35,
            }),
            busy_ratio: 0.8,
            desired_workers: max_threads,
            ..support_control()
        }
    }

    fn link(trend: QueueTrend) -> LinkControl {
        LinkControl {
            trend,
            len: if trend.is_growing() { 8 } else { 0 },
            previous_len: 4,
            arrival_rate: if trend.is_growing() { 100.0 } else { 10.0 },
            drain_rate: if trend.is_draining() { 100.0 } else { 10.0 },
            net_rate: if trend.is_growing() {
                90.0
            } else if trend.is_draining() {
                -90.0
            } else {
                0.0
            },
            ..LinkControl::default()
        }
    }

    fn linear_runtime_nodes(count: usize) -> Vec<RuntimeNode<TestError>> {
        (0..count)
            .map(|index| RuntimeNode {
                name: format!("node{index}"),
                stage: Some(Arc::new(NodeAdapter { stage: TestNode })),
                output_acquire: None,
                is_anchor: false,
                thread_policy: ResolvedThreadPolicy::ImplicitSupport,
                input_link: index,
                output_links: vec![index + 1],
                weighted_branch_config: None,
                is_external: false,
                external_index: None,
                return_state_on_exit: false,
            })
            .collect()
    }

    fn linear_runtime_nodes_with_anchor(count: usize, anchor_index: usize) -> Vec<RuntimeNode<TestError>> {
        let mut nodes = linear_runtime_nodes(count);
        nodes[anchor_index].is_anchor = true;
        nodes[anchor_index].thread_policy = ResolvedThreadPolicy::Scalable(ResolvedScalePolicy {
            initial_threads: 1,
            max_threads: 4,
            target_queue_seconds: 1.0,
            low_queue_seconds: 0.25,
            scale_down_after: Duration::from_millis(500),
            underutilized_busy_ratio: 0.35,
        });
        nodes
    }

    #[test]
    fn queue_trend_classification_distinguishes_stable_empty_from_starved() {
        assert_eq!(
            classify_queue_trend(0, 0, 0.0, 0.0, 0.0),
            QueueTrend::Starved
        );
        assert_eq!(
            classify_queue_trend(0, 0, 100.0, 100.0, 0.0),
            QueueTrend::Stable
        );
        assert_eq!(
            classify_queue_trend(20, 10, 100.0, 10.0, 90.0),
            QueueTrend::Runaway
        );
        assert_eq!(
            classify_queue_trend(2, 20, 10.0, 100.0, -90.0),
            QueueTrend::FastDraining
        );
    }

    #[test]
    fn merging_state_blocks_more_scale_decisions() {
        let mut control = scalable_control(4);
        assert!(node_can_scale(&control));
        control.scaling_state = NodeScalingState::Merging;
        assert!(!node_can_scale(&control));
    }

    fn branch_controller(config: SingleThreadWeightedBranchConfig) -> WeightedBranchController {
        WeightedBranchController::new(config)
    }

    fn default_branch_config() -> SingleThreadWeightedBranchConfig {
        SingleThreadWeightedBranchConfig::default()
    }

    #[test]
    fn weighted_branch_warmup_alternates_left_and_right() {
        let mut controller = branch_controller(default_branch_config());
        assert!(controller.choose_route(0, 0));
        assert!(!controller.choose_route(0, 0));
        assert!(controller.choose_route(0, 0));
    }

    #[test]
    fn weighted_branch_one_ready_arm_stays_in_warmup() {
        let mut controller = branch_controller(default_branch_config());
        controller.left.ready = true;
        assert!(controller.choose_route(0, 0));
        assert!(!controller.choose_route(0, 0));
    }

    #[test]
    fn weighted_branch_both_ready_uses_fill_ratio() {
        let mut controller = branch_controller(default_branch_config());
        controller.left.ready = true;
        controller.right.ready = true;
        controller.left.drain_rate_ewma = 10.0;
        controller.right.drain_rate_ewma = 1.0;
        assert!(controller.choose_route(0, 10));
        assert!(!controller.choose_route(10, 0));
    }

    #[test]
    fn weighted_branch_empty_observed_queue_ignores_stale_smoothed_len() {
        let mut controller = branch_controller(default_branch_config());
        controller.left.ready = true;
        controller.right.ready = true;
        controller.left.drain_rate_ewma = 10.0;
        controller.right.drain_rate_ewma = 10.0;
        controller.left.smoothed_len = 0.0;
        controller.right.smoothed_len = 10.0;
        controller.next_alternate_left = false;

        assert!(!controller.choose_route(0, 0));
    }

    #[test]
    fn weighted_branch_nonempty_observed_queue_keeps_smoothed_backlog_penalty() {
        let mut controller = branch_controller(default_branch_config());
        controller.left.ready = true;
        controller.right.ready = true;
        controller.left.drain_rate_ewma = 10.0;
        controller.right.drain_rate_ewma = 10.0;
        controller.left.smoothed_len = 0.0;
        controller.right.smoothed_len = 10.0;

        assert!(controller.choose_route(0, 1));
    }

    #[test]
    fn weighted_branch_higher_drain_rate_ewma_increases_virtual_capacity() {
        let config = default_branch_config();
        let low = WeightedBranchController::virtual_capacity(1.0, config.target_queue_seconds);
        let high = WeightedBranchController::virtual_capacity(10.0, config.target_queue_seconds);
        assert!(high > low);
    }

    #[test]
    fn weighted_branch_lower_fill_ratio_is_chosen() {
        let mut controller = branch_controller(default_branch_config());
        controller.left.ready = true;
        controller.right.ready = true;
        controller.left.drain_rate_ewma = 10.0;
        controller.right.drain_rate_ewma = 10.0;
        assert!(controller.choose_route(1, 8));
        assert!(!controller.choose_route(8, 1));
    }

    #[test]
    fn weighted_branch_drain_rate_ewma_affects_later_routing() {
        let mut controller = branch_controller(default_branch_config());
        controller.left.ready = true;
        controller.right.ready = true;
        controller.left.drain_rate_ewma = 10.0;
        controller.right.drain_rate_ewma = 10.0;
        assert!(!controller.choose_route(8, 1));
        controller.left.drain_rate_ewma = 100.0;
        assert!(controller.choose_route(8, 1));
    }

    #[test]
    fn link_sampling_tracks_rates_and_numeric_trends() {
        let (sender, receiver) = channel::unbounded::<Message>();
        let links = vec![Link {
            stats: Arc::new(LinkStats::default()),
            kind: LinkKind::Standard {
                sender: Some(sender.clone()),
                receiver,
            },
        }];
        let mut controls = vec![LinkControl::default()];

        for _ in 0..10 {
            sender.send(Box::new(1_u8)).unwrap();
            links[0].stats.arrivals.fetch_add(1, Ordering::Relaxed);
        }
        sample_links(&links, Duration::from_secs(1), &mut controls);

        assert_eq!(controls[0].arrival_rate, 10.0);
        assert_eq!(controls[0].drain_rate, 0.0);
        assert!(controls[0].trend.is_growing());
        assert_eq!(QueueTrend::Runaway.code(), 6);
    }

    #[test]
    fn growing_internal_link_adds_consumer() {
        let active = vec![vec![0], vec![1]];
        let links = vec![
            link(QueueTrend::Stable),
            link(QueueTrend::Growing),
            link(QueueTrend::Stable),
        ];
        let mut controls = vec![scalable_control(1), support_control()];
        controls[1].desired_workers = 2;
        let nodes = linear_runtime_nodes(active.len());

        assert!(matches!(
            choose_scale_operation(&links, &active, &mut controls, &nodes, 2, 4),
            Some(ScaleOperation::Add { node_index: 1 })
        ));
    }

    #[test]
    fn draining_anchor_input_adds_nearest_upstream_producer() {
        let active = vec![vec![0], vec![1], vec![2, 3]];
        let links = vec![
            link(QueueTrend::Stable),
            link(QueueTrend::Stable),
            link(QueueTrend::Draining),
            link(QueueTrend::Stable),
        ];
        let mut controls = vec![support_control(), support_control(), scalable_control(4)];
        controls[1].desired_workers = 2;
        controls[2].busy_ratio = 0.8;
        let nodes = linear_runtime_nodes_with_anchor(active.len(), 2);

        assert!(matches!(
            choose_scale_operation(&links, &active, &mut controls, &nodes, 3, 6),
            Some(ScaleOperation::Add { node_index: 1, .. })
        ));
        assert_eq!(
            controls[2].last_anchor_pressure_reason,
            Some(AnchorPressureReason::InputUnderfed)
        );
    }

    #[test]
    fn global_cap_full_reduces_anchor_before_support_growth() {
        let active = vec![vec![0, 1], vec![2]];
        let links = vec![
            link(QueueTrend::Stable),
            link(QueueTrend::Growing),
            link(QueueTrend::Stable),
        ];
        let mut controls = vec![scalable_control(4), support_control()];
        controls[1].desired_workers = 2;
        let nodes = linear_runtime_nodes(active.len());

        assert!(matches!(
            choose_scale_operation(&links, &active, &mut controls, &nodes, 2, 3),
            Some(ScaleOperation::Remove {
                node_index: 0,
                ..
            })
        ));
    }

    #[test]
    fn output_growth_blocks_scale_up() {
        let active = vec![vec![0], vec![1]];
        let links = vec![
            link(QueueTrend::Stable),
            link(QueueTrend::Stable),
            link(QueueTrend::Growing),
        ];
        let mut controls = vec![scalable_control(4)];
        controls[0].backlog_seconds = 2.0;
        controls[0].desired_workers = 3;
        let nodes = linear_runtime_nodes(active.len());

        assert!(choose_scale_operation(&links, &active, &mut controls, &nodes, 2, 8).is_none());
    }

    #[test]
    fn lease_returns_recycled_value_on_drop() {
        let (recycle_sender, recycle_receiver) = channel::unbounded();
        let (failure_sender, _failure_receiver) = channel::unbounded();
        let runtime = LeaseRuntime {
            shutdown: Arc::new(AtomicBool::new(false)),
            abort: Arc::new(AtomicBool::new(false)),
            internal_failure: failure_sender,
        };

        {
            let mut lease = BufferLease::new(vec![1, 2, 3], recycle_sender, runtime);
            lease.push(4);
        }

        let recycled = recycle_receiver.recv().unwrap();
        assert!(recycled.is_empty());
        assert!(recycled.capacity() >= 4);
    }

    #[test]
    fn lease_recycle_failure_is_fatal_outside_shutdown() {
        let (recycle_sender, recycle_receiver) = channel::unbounded();
        drop(recycle_receiver);
        let (failure_sender, failure_receiver) = channel::unbounded();
        let abort = Arc::new(AtomicBool::new(false));
        let runtime = LeaseRuntime {
            shutdown: Arc::new(AtomicBool::new(false)),
            abort: Arc::clone(&abort),
            internal_failure: failure_sender,
        };

        drop(BufferLease::new(vec![1], recycle_sender, runtime));

        assert!(abort.load(Ordering::Acquire));
        let InternalFailure::Internal { message } = failure_receiver.recv().unwrap() else {
            panic!("expected internal recycle failure");
        };
        assert!(message.contains("recycle"));
    }

    #[test]
    fn cleanup_failure_is_reported_from_join() {
        let config = test_config();
        let mut builder = PipelineGraphBuilder::<u8, TestError>::new();
        let input = builder.input();
        let output = builder.add_node(
            input,
            anchor(
                inline_node(
                    "cleanup",
                    || -> std::result::Result<(), TestError> { Ok(()) },
                    |_state: &mut (), input: u8, ctx: &mut NodeContext<u8, TestError>| {
                        ctx.emit(input);
                        Ok(())
                    },
                )
                .with_cleanup(|_state| Err(TestError::Boom)),
            )
            .max_threads(1),
        );
        let piper = Piper::<u8, u8, TestError>::start(config, builder.finish(output)).unwrap();

        piper.shutdown();
        let error = piper.join().expect_err("cleanup failure should fail join");
        assert!(matches!(error, PiperError::UserCleanup { .. }));
    }
}

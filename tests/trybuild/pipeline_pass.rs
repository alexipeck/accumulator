use piper::{
    BufferLease, PiperConfig, Node, NodeContext, anchor, inline_node, node,
    node_with_state_merge, pipeline,
};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
enum MacroError {}

struct Widen;

impl Node for Widen {
    type Input = u8;
    type Output = u16;
    type Error = MacroError;
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
        ctx.emit(input as u16);
        Ok(())
    }
}

struct Keep;

impl Node for Keep {
    type Input = u16;
    type Output = u16;
    type Error = MacroError;
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

struct CountingKeep;

impl Node for CountingKeep {
    type Input = u16;
    type Output = u16;
    type Error = MacroError;
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

fn merge_usize(target: &mut usize, source: usize) -> std::result::Result<(), MacroError> {
    *target += source;
    Ok(())
}

fn config() -> PiperConfig {
    PiperConfig {
        sample_interval: Duration::from_millis(1),
        poll_interval: Duration::from_millis(1),
        global_worker_cap: Some(4),
        csv_telemetry: None,
    }
}

pipeline! {
    pub struct DirectPipeline {
        type Input = u8;
        type Output = u16;
        type Error = MacroError;

        config = config();
        nodes = [anchor(Widen).max_threads(1), Keep];
    }
}

pipeline! {
    pub struct NamedPipeline {
        type Input = u8;
        type Output = u16;
        type Error = MacroError;

        config = config();
        nodes = [anchor(node("widen", Widen)).max_threads(1), node("keep", Keep)];
    }
}

pipeline! {
    pub struct InlinePipeline {
        type Input = u8;
        type Output = u16;
        type Error = MacroError;

        config = config();
        nodes = [
            anchor(inline_node(
                "widen",
                || -> std::result::Result<(), MacroError> { Ok(()) },
                |_state: &mut (), input: u8, ctx: &mut NodeContext<u16, MacroError>| {
                    ctx.emit(input as u16);
                    Ok(())
                },
            )).max_threads(1),
        ];
    }
}

pipeline! {
    pub struct GraphPipeline {
        type Input = u8;
        type Output = u16;
        type Error = MacroError;

        config = config();
        nodes = {
            widen = anchor(Widen).max_threads(1),
            left = node("left", Keep),
            right = anchor(node("right", Keep)).fixed_threads(1),
            out = node("out", Keep),
        };
        graph = {
            input -> widen;
            widen -> [left, right];
            [left, right] -> out;
            out -> output;
        };
    }
}

pipeline! {
    pub struct StatePipeline {
        type Input = u8;
        type Output = u16;
        type Error = MacroError;

        config = config();
        nodes = [Widen, node("count", CountingKeep).fixed_threads(1)];
        return_state = usize;
    }
}

pipeline! {
    pub struct MergedStatePipeline {
        type Input = u8;
        type Output = u16;
        type Error = MacroError;

        config = config();
        nodes = [
            Widen,
            node_with_state_merge("count", CountingKeep, merge_usize).fixed_threads(1),
        ];
        return_state = usize;
    }
}

pipeline! {
    pub struct ExternalPipeline {
        type Input = u8;
        type Output = BufferLease<Vec<u16>>;
        type Error = MacroError;

        config = config();
        nodes = {
            external = external_node(u8, BufferLease<Vec<u16>>)
                .with_reusable_output(|| Vec::<u16>::new()),
        };
        graph = {
            input -> external;
            external -> output;
        };
    }
}

fn external_run_shape(run: ExternalPipelineRun) {
    let _sender = run.sender();
    let _receiver = run.receiver();
    let _external = run.external.clone();
    let _lease = run.external.acquire_output();
    run.shutdown();
    run.abort();
    let _telemetry = run.get_telemetry();
    let _join = ExternalPipelineRun::join;
}

fn main() {
    let _ = DirectPipeline::start;
    let _ = NamedPipeline::start;
    let _ = InlinePipeline::start;
    let _ = GraphPipeline::start;
    let _ = StatePipeline::start;
    let _ = MergedStatePipeline::start;
    let _ = ExternalPipeline::start;
    let _ = external_run_shape;
}

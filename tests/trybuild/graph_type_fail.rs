use piper::{PiperConfig, Node, NodeContext, node, pipeline};
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

struct ToText;

impl Node for ToText {
    type Input = u8;
    type Output = String;
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
        ctx.emit(input.to_string());
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

fn config() -> PiperConfig {
    PiperConfig {
        sample_interval: Duration::from_millis(1),
        poll_interval: Duration::from_millis(1),
        global_worker_cap: Some(4),
        csv_telemetry: None,
    }
}

pipeline! {
    pub struct BadJoinPipeline {
        type Input = u8;
        type Output = u16;
        type Error = MacroError;

        config = config();
        nodes = {
            widen = node("widen", Widen),
            text = node("text", ToText),
            keep = node("keep", Keep),
        };
        graph = {
            input -> [widen, text];
            [widen, text] -> keep;
            keep -> output;
        };
    }
}

pipeline! {
    pub struct BadExternalPipeline {
        type Input = u8;
        type Output = u16;
        type Error = MacroError;

        config = config();
        nodes = {
            external = external_node(u8, String),
            keep = node("keep", Keep),
        };
        graph = {
            input -> external;
            external -> keep;
            keep -> output;
        };
    }
}

fn main() {}

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

static FIXTURE_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn fixture_dir(name: &str) -> PathBuf {
    let id = FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "piper_macro_test_{}_{name}_{}",
        std::process::id(),
        id
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_fixture(dir: &Path, source: &str) {
    fs::write(dir.join("src").join("lib.rs"), source).unwrap();
}

fn write_manifest(dir: &Path) {
    fs::create_dir_all(dir.join("src")).unwrap();
    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "piper_macro_fixture"
version = "0.1.0"
edition = "2024"

[dependencies]
piper = { path = "REPLACE_PIPER" }
thiserror = "2.0.18"

[features]
default = ["piper/channel-crossbeam"]
"#,
    )
    .unwrap();
    let manifest = fs::read_to_string(dir.join("Cargo.toml")).unwrap();
    let piper_path = env!("CARGO_MANIFEST_DIR").replace('\\', "/");
    let manifest = manifest.replace("REPLACE_PIPER", &piper_path);
    fs::write(dir.join("Cargo.toml"), manifest).unwrap();
}

fn cargo_check_release(dir: &Path) -> std::process::Output {
    Command::new("cargo")
        .current_dir(dir)
        .args(["check", "--release"])
        .output()
        .unwrap()
}

#[test]
fn pipeline_macro_compiles() {
    let pass_source = fs::read_to_string("tests/trybuild/pipeline_pass.rs").unwrap();
    let dir = fixture_dir("pass");
    write_manifest(&dir);
    write_fixture(&dir, &pass_source);
    let output = cargo_check_release(&dir);
    assert!(
        output.status.success(),
        "expected pass fixture to compile:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn pipeline_graph_type_mismatch_fails() {
    let fail_source = fs::read_to_string("tests/trybuild/graph_type_fail.rs").unwrap();
    let dir = fixture_dir("fail");
    write_manifest(&dir);
    write_fixture(&dir, &fail_source);
    let output = cargo_check_release(&dir);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("type mismatch") || stderr.contains("mismatched types"),
        "{stderr}"
    );
    assert!(stderr.contains("IntoNodeSpec"), "{stderr}");
}

#[test]
fn state_pipeline_without_merge_has_no_join_merged() {
    let source = r#"
use piper::{Node, NodeContext, PiperConfig, node, pipeline};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
enum MacroError {}

struct Count;

impl Node for Count {
    type Input = u32;
    type Output = u32;
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

fn config() -> PiperConfig {
    PiperConfig {
        sample_interval: Duration::from_millis(1),
        poll_interval: Duration::from_millis(1),
        global_worker_cap: Some(2),
        csv_telemetry: None,
    }
}

pipeline! {
    pub struct StatePipeline {
        type Input = u32;
        type Output = u32;
        type Error = MacroError;

        config = config();
        nodes = [node("count", Count).fixed_threads(1)];
        return_state = usize;
    }
}

pub fn check() {
    let run = StatePipeline::start().unwrap();
    let _ = run.join_merged();
}
"#;
    let dir = fixture_dir("no_join_merged");
    write_manifest(&dir);
    write_fixture(&dir, source);
    let output = cargo_check_release(&dir);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("join_merged"), "{stderr}");
}

#[test]
fn return_state_rejects_multiple_output_producers() {
    let source = r#"
use piper::{Node, NodeContext, PiperConfig, node, pipeline};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
enum MacroError {}

struct Pass;

impl Node for Pass {
    type Input = u32;
    type Output = u32;
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

fn config() -> PiperConfig {
    PiperConfig {
        sample_interval: Duration::from_millis(1),
        poll_interval: Duration::from_millis(1),
        global_worker_cap: Some(2),
        csv_telemetry: None,
    }
}

pipeline! {
    pub struct BadStateGraph {
        type Input = u32;
        type Output = u32;
        type Error = MacroError;

        config = config();
        nodes = {
            left = node("left", Pass),
            right = node("right", Pass),
        };
        graph = {
            input -> [left, right];
            [left, right] -> output;
        };
        return_state = usize;
    }
}
"#;
    let dir = fixture_dir("bad_state_graph");
    write_manifest(&dir);
    write_fixture(&dir, source);
    let output = cargo_check_release(&dir);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("return_state requires exactly one managed node"),
        "{stderr}"
    );
}

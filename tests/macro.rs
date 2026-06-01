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

//! `marco-load mirror --relations` run as a command, against an in-process
//! marco: a checkpoint whose relations differ exits 3 before any
//! shard file is fetched, the same relations mirror and exit 0, any other
//! failure exits 1, and without the flag nothing changes.
#![cfg(feature = "hubble")]

use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

mod common;
use common::*;

const TOKEN: &str = "export-token-0123456789";

#[test]
fn relations_differ_has_its_own_exit_code() {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let snap = tmp.path().join("snap");
    // follow, block and listitem: the relations common::relations writes
    marco_server::synth::write(&snap, 300, 4).unwrap();
    let token = tmp.path().join("export-token");
    std::fs::write(&token, TOKEN).unwrap();
    let addr = rt.block_on(async {
        let auth = marco_server::auth::AuthConfig { export_token: Some(TOKEN.into()), ..Default::default() };
        let app =
            marco_server::app::App::open(marco_server::app::Config { auth, ..marco_server::app::Config::new(snap) })
                .unwrap();
        let ui = Arc::new(marco_server::ui::Ui::load(None).unwrap());
        marco_server::serve(app, ui).await.unwrap().1
    });
    let mirror = |dir: &Path, rels: Option<&Path>| -> Output {
        let url = format!("http://{addr}");
        let mut args = vec!["mirror", "--url", &url, "--token-file", s(&token), "--dir", s(dir)];
        if let Some(r) = rels {
            args.extend(["--relations", s(r)]);
        }
        Command::new(bin()).args(&args).output().unwrap()
    };
    let same = relations(tmp.path());
    let more = tmp.path().join("more.toml");
    std::fs::write(
        &more,
        std::fs::read_to_string(&same).unwrap()
            + "\n[[relation]]\nname = \"listblock\"\ncollection = \"app.bsky.graph.listblock\"\nsubject = \"subject\"\n\
               subject_collection = \"app.bsky.graph.list\"\n",
    )
    .unwrap();

    let m = tmp.path().join("refused");
    let out = mirror(&m, Some(&more));
    let err = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(3), "{err}");
    assert!(err.contains("relations differ") && err.contains("listblock"), "{err}");
    assert!(out.stdout.is_empty());
    let files: Vec<String> = std::fs::read_dir(m.join("files"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(files, ["directory.marco"], "only the directory file");
    assert!(!m.join("current").exists());

    let m = tmp.path().join("same");
    let out = mirror(&m, Some(&same));
    assert_eq!(out.status.code(), Some(0), "{}", String::from_utf8_lossy(&out.stderr));
    let r: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!((&r["fetched"], &r["relations_checked"]), (&r["files"], &serde_json::json!(true)));
    assert!(m.join("current/manifest.json").is_file());

    // no flag: the checkpoint is mirrored whatever its relations
    let m = tmp.path().join("unchecked");
    let out = mirror(&m, None);
    assert_eq!(out.status.code(), Some(0), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(m.join("current/manifest.json").is_file());

    // anything else that stops a mirror is an ordinary failure
    let out = mirror(&tmp.path().join("missing"), Some(&tmp.path().join("no-such.toml")));
    assert_eq!(out.status.code(), Some(1));
}

//! The Scylla source config (TOML): where the cluster is, how to read it,
//! and which table holds which relation under which column names.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::frame::types::Consistency;
use scylla::frame::Compression;
use scylla::policies::load_balancing::DefaultPolicy;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Contact points, host:port. The driver discovers the rest of the cluster.
    pub nodes: Vec<String>,
    pub keyspace: String,
    /// Read only from replicas in this datacenter.
    #[serde(default)]
    pub datacenter: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    /// A file holding the password (never in the config itself).
    #[serde(default)]
    pub password_file: Option<PathBuf>,
    /// Default local_one with a datacenter, else one. For a production
    /// rebuild, local_quorum (or a repair first) reads past a replica that missed writes.
    #[serde(default)]
    pub consistency: Option<String>,
    /// Rows per page.
    #[serde(default = "default_page_size")]
    pub page_size: i32,
    /// Ranges per replica shard: the unit of work and of resume.
    #[serde(default = "default_ranges_per_shard")]
    pub ranges_per_shard: usize,
    /// Ranges read at once per replica shard.
    #[serde(default = "default_in_flight")]
    pub in_flight_per_shard: usize,
    /// SELECT … BYPASS CACHE, so a full scan doesn't evict the cluster's working set.
    #[serde(default = "yes")]
    pub bypass_cache: bool,
    /// lz4, snappy or none.
    #[serde(default = "default_compression")]
    pub compression: String,
    #[serde(default = "default_timeout")]
    pub request_timeout_secs: f64,
    /// Attempts per page before the scan stops (it resumes from its last checkpoint).
    #[serde(default = "default_retries")]
    pub retries: u32,
    #[serde(rename = "table")]
    pub tables: Vec<Table>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Table {
    /// The marco relation (relations config) the table's rows are records of.
    pub relation: String,
    /// The table's name in the keyspace.
    pub name: String,
    /// The column holding the record's repo DID.
    #[serde(default = "default_actor")]
    pub actor: String,
    /// The column holding the record key.
    #[serde(default = "default_rkey")]
    pub rkey: String,
    /// The column holding the subject DID.
    #[serde(default = "default_subject")]
    pub subject: String,
    /// A list relation's table: the column holding the list's AT URI
    /// (default list). Only a list relation's table has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list: Option<String>,
    /// A table that holds each record itself: the column with its JSON (blob
    /// or text). The relation's rule reads the subject (and list) from it, as
    /// the live path does, so the subject and list columns aren't read. A
    /// relation with `when` or a `$self` subject needs one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<String>,
}

/// A list table's list column when its config leaves it out.
pub const DEFAULT_LIST: &str = "list";

fn default_page_size() -> i32 {
    5000
}
fn default_ranges_per_shard() -> usize {
    8
}
fn default_in_flight() -> usize {
    2
}
fn yes() -> bool {
    true
}
fn default_compression() -> String {
    "lz4".into()
}
fn default_timeout() -> f64 {
    60.0
}
fn default_retries() -> u32 {
    8
}
fn default_actor() -> String {
    "actor_did".into()
}
fn default_rkey() -> String {
    "rkey".into()
}
fn default_subject() -> String {
    "target_did".into()
}

/// A CQL identifier, quoted when it isn't a plain lower-case one.
pub fn ident(s: &str) -> String {
    let plain = s.chars().next().is_some_and(|c| c.is_ascii_lowercase())
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if plain {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
}

impl Config {
    pub fn parse(s: &str) -> Result<Config> {
        let c: Config = toml::from_str(s).context("scylla config")?;
        c.validate()?;
        Ok(c)
    }

    pub fn read(p: &Path) -> Result<Config> {
        Config::parse(&std::fs::read_to_string(p).with_context(|| format!("read {}", p.display()))?)
    }

    fn validate(&self) -> Result<()> {
        if self.nodes.is_empty() {
            bail!("scylla config: nodes is empty");
        }
        if self.tables.is_empty() {
            bail!("scylla config: no [[table]]");
        }
        if self.page_size < 1 || self.ranges_per_shard < 1 || self.in_flight_per_shard < 1 {
            bail!("scylla config: page_size, ranges_per_shard and in_flight_per_shard must be at least 1");
        }
        self.consistency()?;
        self.compression()?;
        let mut seen = std::collections::HashSet::new();
        for t in &self.tables {
            if !seen.insert(&t.relation) {
                bail!("scylla config: two tables for relation {:?}", t.relation);
            }
        }
        Ok(())
    }

    /// The consistency's name, with the default filled in.
    pub fn consistency_name(&self) -> String {
        match &self.consistency {
            Some(c) => c.to_ascii_lowercase(),
            None if self.datacenter.is_some() => "local_one".into(),
            None => "one".into(),
        }
    }

    pub fn consistency(&self) -> Result<Consistency> {
        Ok(match self.consistency_name().as_str() {
            "one" => Consistency::One,
            "local_one" => Consistency::LocalOne,
            "two" => Consistency::Two,
            "quorum" => Consistency::Quorum,
            "local_quorum" => Consistency::LocalQuorum,
            "all" => Consistency::All,
            c => bail!("scylla config: unknown consistency {c:?}"),
        })
    }

    fn compression(&self) -> Result<Option<Compression>> {
        Ok(match self.compression.to_ascii_lowercase().as_str() {
            "lz4" => Some(Compression::Lz4),
            "snappy" => Some(Compression::Snappy),
            "none" | "" => None,
            c => bail!("scylla config: unknown compression {c:?}"),
        })
    }

    pub async fn connect(&self) -> Result<Session> {
        let mut b = SessionBuilder::new().known_nodes(&self.nodes).compression(self.compression()?);
        if let Some(dc) = &self.datacenter {
            // never another datacenter, even for a page retried away from its owner
            let lb = DefaultPolicy::builder()
                .prefer_datacenter(dc.clone())
                .permit_dc_failover(false)
                .token_aware(true)
                .build();
            let profile =
                ExecutionProfile::builder().load_balancing_policy(lb).consistency(self.consistency()?).build();
            b = b.default_execution_profile_handle(profile.into_handle());
        }
        if let Some(u) = &self.username {
            let p = match &self.password_file {
                Some(f) => std::fs::read_to_string(f)
                    .with_context(|| format!("password_file {}", f.display()))?
                    .trim_end()
                    .to_string(),
                None => bail!("scylla config: username needs password_file"),
            };
            b = b.user(u, p);
        }
        b.build().await.with_context(|| format!("connect to {:?}", self.nodes))
    }

    /// Fills in each list relation's list column, and refuses a list column
    /// on a table whose relation has no list, or a relation not in `rels`.
    /// A record table names no subject or list column, and a relation whose
    /// edges only the record can say (`when`, `$self`) needs a record table.
    pub fn resolve(&mut self, rels: &marco_core::Relations) -> Result<()> {
        for t in &mut self.tables {
            let Some(id) = rels.by_name(&t.relation) else {
                bail!("table relation {:?} isn't in the relations config", t.relation)
            };
            let r = rels.get(id);
            if r.owner_of.is_some() {
                bail!(
                    "{:?} is a companion; scan its list relation, {:?}",
                    t.relation,
                    r.owner_of.as_deref().unwrap_or("")
                );
            }
            if t.record.is_some() {
                if t.list.is_some() || t.subject != default_subject() {
                    bail!(
                        "table {:?} has a record column: its subject and list come from the record, so it names neither column",
                        t.name
                    );
                }
                continue;
            }
            if !r.when.is_empty() || r.is_self() {
                bail!(
                    "table {:?}: {:?} has `when` or a $self subject, which the scan reads from the record: name the table's record column (record = \"...\")",
                    t.name,
                    t.relation
                );
            }
            match (&r.list, &t.list) {
                (Some(_), None) => t.list = Some(DEFAULT_LIST.into()),
                (None, Some(_)) => {
                    bail!("table {:?} names a list column, but {:?} isn't a list relation", t.name, t.relation)
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs_f64(self.request_timeout_secs.max(0.1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_config_parses() {
        let mut c = Config::parse(include_str!("../../../../examples/scylla.toml")).unwrap();
        c.resolve(&marco_core::Relations::bsky_default()).unwrap();
        let rels: Vec<_> = c.tables.iter().map(|t| t.relation.as_str()).collect();
        assert_eq!(rels, ["follow", "block", "listitem"]);
        assert_eq!(c.tables[2].list.as_deref(), Some("list"));
    }

    #[test]
    fn parses_with_defaults() {
        let c = Config::parse(
            r#"
            nodes = ["127.0.0.1:9042"]
            keyspace = "bsky"
            [[table]]
            relation = "follow"
            name = "follows"
            [[table]]
            relation = "block"
            name = "Blocks"
            subject = "subject"
            "#,
        )
        .unwrap();
        assert_eq!(c.page_size, 5000);
        assert_eq!(c.ranges_per_shard, 8);
        assert_eq!(c.in_flight_per_shard, 2);
        assert!(c.bypass_cache);
        assert_eq!(c.consistency_name(), "one");
        let dc = Config { datacenter: Some("east".into()), ..c.clone() };
        assert_eq!(dc.consistency_name(), "local_one");
        assert_eq!(
            Config { consistency: Some("LOCAL_QUORUM".into()), ..dc }.consistency().unwrap(),
            Consistency::LocalQuorum
        );
        assert_eq!(c.tables[0].actor, "actor_did");
        assert_eq!(c.tables[1].subject, "subject");
        assert_eq!(ident(&c.tables[1].name), "\"Blocks\"");
        assert_eq!(ident("target_did"), "target_did");
        let mut l = Config::parse(
            r#"
            nodes = ["127.0.0.1:9042"]
            keyspace = "bsky"
            [[table]]
            relation = "follow"
            name = "follows"
            [[table]]
            relation = "listitem"
            name = "list_items"
            subject = "subject"
            "#,
        )
        .unwrap();
        let rels = marco_core::Relations::bsky_default();
        l.resolve(&rels).unwrap();
        assert_eq!((l.tables[0].list.as_deref(), l.tables[1].list.as_deref()), (None, Some("list")));
        let mut bad = l.clone();
        bad.tables[0].list = Some("list".into());
        assert!(bad.resolve(&rels).is_err(), "a follow table has no list column");
        bad.tables[0] = Table { relation: "listitem.owner".into(), ..l.tables[1].clone() };
        assert!(bad.resolve(&rels).is_err(), "a companion isn't scanned");
        bad.tables[0].relation = "repost".into();
        assert!(bad.resolve(&rels).is_err());
        assert!(Config::parse("nodes = []\nkeyspace = \"k\"\n[[table]]\nrelation = \"a\"\nname = \"b\"").is_err());
        assert!(Config::parse(
            "nodes = [\"x\"]\nkeyspace = \"k\"\nbogus = 1\n[[table]]\nrelation = \"a\"\nname = \"b\""
        )
        .is_err());
    }

    /// The defaults, then a list subscription (a list's URI as the subject)
    /// and a list's purpose (its own URI, while it's a mod list).
    fn list_subject_rels() -> marco_core::Relations {
        marco_core::Relations::parse_toml(
            r#"
[[relation]]
name = "follow"
collection = "app.bsky.graph.follow"
subject = "subject"

[[relation]]
name = "listitem"
collection = "app.bsky.graph.listitem"
subject = "subject"
list = "list"

[[relation]]
name = "listblock"
collection = "app.bsky.graph.listblock"
subject = "subject"
subject_collection = "app.bsky.graph.list"

[[relation]]
name = "modlist"
collection = "app.bsky.graph.list"
subject = "$self"
when = { purpose = "app.bsky.graph.defs#modlist" }
"#,
        )
        .unwrap()
    }

    /// A list subscription's table is a plain one whose subject column holds
    /// the list's URI. A list's purpose is only in the record, so its table
    /// names a record column and no subject or list column.
    #[test]
    fn list_subscriptions_and_purposes() {
        let rels = list_subject_rels();
        let mut c = Config::parse(
            r#"
            nodes = ["127.0.0.1:9042"]
            keyspace = "bsky"
            [[table]]
            relation = "listblock"
            name = "listblocks"
            subject = "list"
            [[table]]
            relation = "modlist"
            name = "lists"
            record = "raw"
            "#,
        )
        .unwrap();
        c.resolve(&rels).unwrap();
        assert_eq!((c.tables[0].subject.as_str(), c.tables[0].list.as_deref()), ("list", None));
        assert_eq!((c.tables[1].record.as_deref(), c.tables[1].list.as_deref()), (Some("raw"), None));

        let no_record = Table { record: None, ..c.tables[1].clone() };
        let e = Config { tables: vec![no_record], ..c.clone() }.resolve(&rels).unwrap_err();
        assert!(format!("{e:#}").contains("record column"), "{e:#}");
        for bad in [
            Table { subject: "subject".into(), ..c.tables[1].clone() },
            Table { list: Some("list".into()), ..c.tables[1].clone() },
        ] {
            assert!(
                Config { tables: vec![bad], ..c.clone() }.resolve(&rels).is_err(),
                "a record table names no column"
            );
        }
        // any relation's table can be a record table: a list relation's list comes from its record too
        let li = Table { relation: "listitem".into(), name: "listitems".into(), ..c.tables[1].clone() };
        let mut l = Config { tables: vec![li], ..c.clone() };
        l.resolve(&rels).unwrap();
        assert_eq!(l.tables[0].list, None);
        // a plan written before record columns reads back the same
        let t: Table = serde_json::from_str(
            r#"{"relation":"follow","name":"follows","actor":"actor_did","rkey":"rkey","subject":"target_did"}"#,
        )
        .unwrap();
        assert!(!serde_json::to_string(&t).unwrap().contains("record"));
    }
}

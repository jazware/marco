//! The console's history: every 5 s the server reads its own registry and
//! keeps rates and quantiles from the differences, for 24 h. That's ~4 MB,
//! and it lets a page show the last hour without a Prometheus.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use prometheus::proto::{MetricFamily, MetricType};

use crate::app::{now_ms, App};

pub const STEP: Duration = Duration::from_secs(5);
const KEEP: usize = 86_400 / 5;
const MAX_POINTS: usize = 180;

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Key {
    pub metric: &'static str,
    pub op: String,
    pub rel: String,
    pub outcome: String,
    pub dir: String,
    pub rpc: String,
}

#[derive(Clone)]
struct Hist {
    count: u64,
    /// (upper bound, cumulative count)
    buckets: Vec<(f64, u64)>,
}

type Prev = (Instant, HashMap<String, Hist>, HashMap<String, f64>);

/// (step, point times, each series' values)
pub type Window = (i64, Vec<i64>, Vec<(Key, Vec<f64>)>);

#[derive(Default)]
pub struct Ring {
    keys: Vec<Key>,
    index: HashMap<Key, usize>,
    samples: VecDeque<(i64, Vec<f32>)>,
    prev: Option<Prev>,
}

fn label(m: &prometheus::proto::Metric, name: &str) -> String {
    m.get_label().iter().find(|l| l.name() == name).map(|l| l.value().to_string()).unwrap_or_default()
}

/// A series' key: its family and label values, leaving out the RPC path,
/// whose fast and pool histograms the console sums.
fn id(fam: &str, m: &prometheus::proto::Metric) -> String {
    let mut s = fam.to_string();
    for l in m.get_label().iter().filter(|l| l.name() != "path") {
        s.push('|');
        s.push_str(l.value());
    }
    s
}

/// The q-quantile of the observations between two cumulative histograms,
/// interpolated inside its bucket.
fn quantile(now: &Hist, before: Option<&Hist>, q: f64) -> f64 {
    let n = now.count - before.map_or(0, |b| b.count);
    if n == 0 {
        return 0.0;
    }
    let want = q * n as f64;
    let mut lo = 0.0;
    let mut prev_cum = 0u64;
    for (i, &(ub, cum)) in now.buckets.iter().enumerate() {
        let c = cum - before.and_then(|b| b.buckets.get(i)).map_or(0, |x| x.1);
        if c as f64 >= want {
            let inb = (c - prev_cum).max(1) as f64;
            return lo + (ub - lo) * ((want - prev_cum as f64) / inb).clamp(0.0, 1.0);
        }
        prev_cum = c;
        lo = ub;
    }
    lo
}

impl Ring {
    fn slot(&mut self, k: Key) -> usize {
        if let Some(&i) = self.index.get(&k) {
            return i;
        }
        self.keys.push(k.clone());
        self.index.insert(k, self.keys.len() - 1);
        self.keys.len() - 1
    }

    pub fn sample(&mut self, fams: &[MetricFamily], extra: &[(Key, f64)]) {
        let now = Instant::now();
        let mut hists = HashMap::new();
        let mut ctrs = HashMap::new();
        for f in fams {
            for m in f.get_metric() {
                match f.get_field_type() {
                    MetricType::HISTOGRAM => {
                        let h = m.get_histogram();
                        let e = hists.entry(id(f.name(), m)).or_insert_with(|| Hist {
                            count: 0,
                            buckets: h.get_bucket().iter().map(|b| (b.upper_bound(), 0)).collect(),
                        });
                        e.count += h.get_sample_count();
                        for (x, b) in e.buckets.iter_mut().zip(h.get_bucket()) {
                            x.1 += b.cumulative_count();
                        }
                    }
                    MetricType::COUNTER => {
                        ctrs.insert(id(f.name(), m), m.get_counter().get_value());
                    }
                    _ => {}
                }
            }
        }
        let mut vals: Vec<(Key, f64)> = extra.to_vec();
        if let Some((t0, ph, pc)) = &self.prev {
            let dt = now.duration_since(*t0).as_secs_f64().max(1e-3);
            let rate = |id: &str, v: f64| (v - pc.get(id).copied().unwrap_or(0.0)).max(0.0) / dt;
            let mut rpc_err: HashMap<String, f64> = HashMap::new();
            let mut lookups: HashMap<String, (f64, f64)> = HashMap::new();
            let mut seen = std::collections::HashSet::new();
            for f in fams {
                for m in f.get_metric() {
                    let i = id(f.name(), m);
                    if !seen.insert(i.clone()) {
                        continue;
                    }
                    match f.name() {
                        "marco_op_seconds" | "marco_rpc_seconds" | "marco_merge_seconds" => {
                            let h = &hists[&i];
                            let b = ph.get(&i);
                            let n = h.count - b.map_or(0, |b| b.count).min(h.count);
                            let (op, rel, rpc, dir) =
                                (label(m, "op"), label(m, "rel"), label(m, "rpc"), label(m, "dir"));
                            let k = |metric| Key {
                                metric,
                                op: op.clone(),
                                rel: rel.clone(),
                                rpc: rpc.clone(),
                                dir: dir.clone(),
                                ..Default::default()
                            };
                            match f.name() {
                                "marco_op_seconds" => {
                                    vals.push((k("op_rate"), n as f64 / dt));
                                    vals.push((k("op_p50"), quantile(h, b, 0.5)));
                                    vals.push((k("op_p99"), quantile(h, b, 0.99)));
                                }
                                "marco_rpc_seconds" => {
                                    vals.push((k("rpc_rate"), n as f64 / dt));
                                    vals.push((k("rpc_p99"), quantile(h, b, 0.99)));
                                }
                                _ => vals.push((k("merge_p99"), quantile(h, b, 0.99))),
                            }
                        }
                        "marco_applied_total" => vals.push((
                            Key {
                                metric: "applied",
                                rel: label(m, "rel"),
                                outcome: label(m, "outcome"),
                                ..Default::default()
                            },
                            rate(&i, ctrs[&i]),
                        )),
                        "marco_merges_total" => vals.push((
                            Key { metric: "merges", rel: label(m, "rel"), dir: label(m, "dir"), ..Default::default() },
                            rate(&i, ctrs[&i]),
                        )),
                        "marco_rpc_errors_total" => {
                            *rpc_err.entry(label(m, "rpc")).or_default() += rate(&i, ctrs[&i]);
                        }
                        "marco_store_io_seconds"
                        | "marco_store_miss_pool_wait_seconds"
                        | "marco_store_request_pages"
                        | "marco_store_request_rounds" => {
                            let h = &hists[&i];
                            let b = ph.get(&i);
                            let n = h.count - b.map_or(0, |b| b.count).min(h.count);
                            let k = |metric| Key {
                                metric,
                                rpc: label(m, "rpc"),
                                dir: label(m, "queue"),
                                ..Default::default()
                            };
                            match f.name() {
                                "marco_store_io_seconds" => {
                                    vals.push((k("store_reads"), n as f64 / dt));
                                    vals.push((k("store_read_p50"), quantile(h, b, 0.5)));
                                    vals.push((k("store_read_p99"), quantile(h, b, 0.99)));
                                }
                                "marco_store_miss_pool_wait_seconds" => {
                                    vals.push((k("store_pool_wait_p99"), quantile(h, b, 0.99)))
                                }
                                "marco_store_request_pages" => vals.push((k("store_pages_p99"), quantile(h, b, 0.99))),
                                _ => vals.push((k("store_rounds_p99"), quantile(h, b, 0.99))),
                            }
                        }
                        "marco_store_io_bytes_total" => vals.push((
                            Key { metric: "store_io_bytes", dir: label(m, "queue"), ..Default::default() },
                            rate(&i, ctrs[&i]),
                        )),
                        "marco_store_lookups_total" if label(m, "source") != "job" => {
                            let r = rate(&i, ctrs[&i]);
                            let e = lookups.entry(label(m, "structure")).or_default();
                            e.0 += r;
                            if label(m, "result") == "miss" {
                                e.1 += r;
                            }
                        }
                        "process_cpu_seconds_total" => {
                            vals.push((Key { metric: "cpu_cores", ..Default::default() }, rate(&i, ctrs[&i])))
                        }
                        _ => {}
                    }
                }
            }
            for (rpc, r) in rpc_err {
                vals.push((Key { metric: "rpc_errors", rpc, ..Default::default() }, r));
            }
            for (structure, (all, miss)) in lookups {
                vals.push((Key { metric: "store_lookups", op: structure.clone(), ..Default::default() }, all));
                vals.push((Key { metric: "store_misses", op: structure, ..Default::default() }, miss));
            }
        }
        let first = self.prev.is_none();
        self.prev = Some((now, hists, ctrs));
        // the first sample has nothing to take differences from
        if first {
            return;
        }
        let mut row = vec![f32::NAN; self.keys.len()];
        for (k, v) in vals {
            let i = self.slot(k);
            if i >= row.len() {
                row.resize(i + 1, f32::NAN);
            }
            row[i] = v as f32;
        }
        if self.samples.len() == KEEP {
            self.samples.pop_front();
        }
        self.samples.push_back((now_ms(), row));
    }

    /// The window as at most 180 points, each the mean of its samples.
    pub fn window(&self, secs: u64) -> Window {
        let from = now_ms() - secs as i64 * 1000;
        let rows: Vec<&(i64, Vec<f32>)> = self.samples.iter().filter(|(t, _)| *t >= from).collect();
        let per = rows.len().div_ceil(MAX_POINTS).max(1);
        let groups: Vec<&[&(i64, Vec<f32>)]> = rows.chunks(per).collect();
        let ts = groups.iter().map(|g| g.last().unwrap().0).collect();
        let series = self
            .keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let v = groups
                    .iter()
                    .map(|g| {
                        let xs: Vec<f64> = g
                            .iter()
                            .filter_map(|r| r.1.get(i).copied())
                            .filter(|x| !x.is_nan())
                            .map(f64::from)
                            .collect();
                        if xs.is_empty() {
                            0.0
                        } else {
                            xs.iter().sum::<f64>() / xs.len() as f64
                        }
                    })
                    .collect();
                (k.clone(), v)
            })
            .collect();
        (STEP.as_millis() as i64 * per as i64, ts, series)
    }
}

/// Samples forever.
pub fn spawn(app: Arc<App>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(STEP);
        loop {
            tick.tick().await;
            let a = app.clone();
            let _ = tokio::task::spawn_blocking(move || sample(&a)).await;
        }
    });
}

fn sample(app: &App) {
    app.sample_rates();
    let (delta, tombs) = {
        let g = app.graph();
        g.refresh_gauges();
        crate::admin_svc::delta_totals(&g)
    };
    let p = crate::sys::proc_self();
    let (depth, _) = app.engine.io_depth();
    let extra = [
        (Key { metric: "store_io_queue", ..Default::default() }, depth as f64),
        (Key { metric: "store_pool_busy", ..Default::default() }, crate::metrics::POOL_BUSY.get() as f64),
        (Key { metric: "rss_bytes", ..Default::default() }, p.rss_bytes as f64),
        (Key { metric: "delta_entries", ..Default::default() }, delta as f64),
        (Key { metric: "tombstones", ..Default::default() }, tombs as f64),
    ];
    let fams = prometheus::gather();
    app.series.lock().unwrap().sample(&fams, &extra);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rates_and_quantiles_from_differences() {
        let h = prometheus::register_histogram_vec!(
            "marco_rpc_seconds_test",
            "test",
            &["rpc"],
            vec![0.001, 0.002, 0.004, 0.008]
        )
        .unwrap();
        let fam = |h: &prometheus::HistogramVec| {
            let mut f = h.collect().remove(0);
            f.set_name("marco_rpc_seconds".into());
            f
        };
        use prometheus::core::Collector;
        let mut r = Ring::default();
        h.with_label_values(&["Graph/Has"]).observe(0.0005);
        r.sample(&[fam(&h)], &[]);
        for _ in 0..99 {
            h.with_label_values(&["Graph/Has"]).observe(0.0015);
        }
        h.with_label_values(&["Graph/Has"]).observe(0.007);
        r.sample(&[fam(&h)], &[]);
        let (_, ts, series) = r.window(60);
        assert_eq!(ts.len(), 1, "the first sample only sets the baseline");
        let get = |m: &str| series.iter().find(|(k, _)| k.metric == m && k.rpc == "Graph/Has").unwrap().1.clone();
        assert!(get("rpc_rate")[0] > 0.0);
        let p99 = get("rpc_p99")[0];
        assert!((0.001..=0.00201).contains(&p99), "the 99th of 100 is in the 1-2 ms bucket: {p99}");
    }
}

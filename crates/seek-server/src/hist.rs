//! Histograms without shared cache lines, as marco's op histograms are: each
//! thread adds to its own cache-line aligned shard, a scrape sums them. A
//! `prometheus` Histogram is one set of atomics every thread adds to, which
//! at tens of threads on the keystroke path costs more than the query.
//! Buckets are powers of two.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

use prometheus::core::{Collector, Desc};
use prometheus::proto::{self, MetricFamily, MetricType};

const BUCKETS: usize = 32;
const SHARDS: usize = 64;

#[repr(align(128))]
struct Shard {
    count: AtomicU64,
    sum: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static SLOT: usize = NEXT_SLOT.fetch_add(1, Relaxed) % SHARDS;
}

pub struct Hist {
    shards: Box<[Shard]>,
    first_pow: u32,
}

impl Hist {
    fn new(first_pow: u32) -> Hist {
        let shards = (0..SHARDS)
            .map(|_| Shard {
                count: AtomicU64::new(0),
                sum: AtomicU64::new(0),
                buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            })
            .collect();
        Hist { shards, first_pow }
    }

    /// One observation, in the family's raw unit (nanoseconds for a latency).
    #[inline]
    pub fn observe(&self, v: u64) {
        let s = &self.shards[SLOT.with(|s| *s)];
        s.count.fetch_add(1, Relaxed);
        s.sum.fetch_add(v, Relaxed);
        let pow = 64 - v.saturating_sub(1).leading_zeros();
        let b = pow.saturating_sub(self.first_pow) as usize;
        if b < BUCKETS {
            s.buckets[b].fetch_add(1, Relaxed);
        }
    }

    #[inline]
    pub fn since(&self, t: std::time::Instant) {
        self.observe(t.elapsed().as_nanos() as u64);
    }

    fn sum(&self) -> (u64, u64, [u64; BUCKETS]) {
        let mut b = [0u64; BUCKETS];
        let (mut c, mut sum) = (0, 0);
        for s in self.shards.iter() {
            c += s.count.load(Relaxed);
            sum += s.sum.load(Relaxed);
            for (i, x) in s.buckets.iter().enumerate() {
                b[i] += x.load(Relaxed);
            }
        }
        (c, sum, b)
    }
}

/// A histogram family: a name, its labels, and one [`Hist`] per label set, made up front so every
/// series exists at 0 from startup.
pub struct Family {
    name: String,
    help: String,
    labels: Vec<String>,
    /// Raw units per exported unit: 1e9 for nanoseconds exported as seconds, 1 for counts.
    scale: f64,
    first_pow: u32,
    hists: Mutex<HashMap<Vec<String>, Arc<Hist>>>,
    desc: Desc,
}

impl Family {
    pub fn with(&self, values: &[&str]) -> Arc<Hist> {
        let key: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        self.hists.lock().unwrap().entry(key).or_insert_with(|| Arc::new(Hist::new(self.first_pow))).clone()
    }
}

/// A latency family in seconds (observations in ns, buckets from 64 ns to ~2 min).
pub fn seconds(name: &str, help: &str, labels: &[&str]) -> Arc<Family> {
    register(name, help, labels, 1e9, 6)
}

/// A count family (buckets 1, 2, 4, ... 2^31).
pub fn counts(name: &str, help: &str, labels: &[&str]) -> Arc<Family> {
    register(name, help, labels, 1.0, 0)
}

fn register(name: &str, help: &str, labels: &[&str], scale: f64, first_pow: u32) -> Arc<Family> {
    let desc = Desc::new(name.into(), help.into(), labels.iter().map(|s| s.to_string()).collect(), HashMap::new())
        .expect("histogram desc");
    let f = Arc::new(Family {
        name: name.into(),
        help: help.into(),
        labels: labels.iter().map(|s| s.to_string()).collect(),
        scale,
        first_pow,
        hists: Mutex::new(HashMap::new()),
        desc,
    });
    // a second registration of the same name (tests open several servers) keeps the first
    let _ = prometheus::register(Box::new(Collect(f.clone())));
    f
}

struct Collect(Arc<Family>);

impl Collector for Collect {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.0.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let f = &self.0;
        let mut mf = MetricFamily::default();
        mf.set_name(f.name.clone());
        mf.set_help(f.help.clone());
        mf.set_field_type(MetricType::HISTOGRAM);
        let hists = f.hists.lock().unwrap();
        let mut keys: Vec<&Vec<String>> = hists.keys().collect();
        keys.sort();
        for k in keys {
            let (count, sum, buckets) = hists[k].sum();
            let mut h = proto::Histogram::default();
            h.set_sample_count(count);
            h.set_sample_sum(sum as f64 / f.scale);
            let mut cum = 0;
            let mut bs = Vec::with_capacity(BUCKETS);
            for (i, &b) in buckets.iter().enumerate() {
                cum += b;
                let mut bk = proto::Bucket::default();
                bk.set_cumulative_count(cum);
                bk.set_upper_bound((1u64 << (f.first_pow as usize + i)) as f64 / f.scale);
                bs.push(bk);
            }
            h.set_bucket(bs);
            let mut m = proto::Metric::default();
            m.set_label(
                f.labels
                    .iter()
                    .zip(k)
                    .map(|(n, v)| {
                        let mut l = proto::LabelPair::default();
                        l.set_name(n.clone());
                        l.set_value(v.clone());
                        l
                    })
                    .collect(),
            );
            m.set_histogram(h);
            mf.mut_metric().push(m);
        }
        vec![mf]
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn buckets_and_scrape() {
        let f = super::seconds("seek_test_hist_seconds", "test", &["mode"]);
        let h = f.with(&["a"]);
        let _zero = f.with(&["b"]);
        for ns in [1, 64, 65, 1000] {
            h.observe(ns);
        }
        let text = marco_ops::metrics_text();
        assert!(text.contains(r#"seek_test_hist_seconds_count{mode="a"} 4"#), "{text}");
        assert!(text.contains(r#"seek_test_hist_seconds_count{mode="b"} 0"#));
        assert!(text.contains(r#"seek_test_hist_seconds_bucket{mode="a",le="0.000000064"} 2"#));
    }
}

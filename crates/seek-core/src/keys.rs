//! An actor's keys: folded strings a query's fold is a byte prefix of, each
//! with a match tier. Handle keys and name keys come apart so a profile edit
//! or a handle change can replace one side and keep the other.

use std::collections::HashSet;

use crate::fold::{fold_into, word_starts};

/// The folded handle.
pub const HANDLE: u8 = 0;
/// The folded display name.
pub const NAME: u8 = 1;
/// A later label or hyphen part of the handle, inside the part its owner picked.
pub const HSEG: u8 = 2;
/// A later word of the display name.
pub const NSEG: u8 = 3;

/// Keys and queries are cut at this many bytes: a longer query matches on its first 32.
pub const MAXK: usize = 32;
const HANDLE_SEGMENTS: usize = 3;
const NAME_WORDS: usize = 4;

pub type Key = (Box<[u8]>, u8);

/// `s` cut to at most [`MAXK`] bytes on a character boundary, trailing space dropped.
pub fn cut(s: &str) -> &str {
    if s.len() <= MAXK {
        return s;
    }
    let mut e = MAXK;
    while !s.is_char_boundary(e) {
        e -= 1;
    }
    s[..e].trim_end()
}

/// Hosting suffixes: the last two labels of handles a provider hands out (`bsky.social`). They
/// get no keys, since every one of their users would match them.
#[derive(Clone, Debug, Default)]
pub struct Hosting(HashSet<String>);

impl Hosting {
    pub fn new<I: IntoIterator<Item = String>>(it: I) -> Hosting {
        Hosting(it.into_iter().collect())
    }

    /// Suffixes with at least `min` handles under them.
    pub fn from_handles<'a>(handles: impl Iterator<Item = &'a str>, min: u64) -> Hosting {
        let mut count: rustc_hash::FxHashMap<&str, u64> = Default::default();
        for h in handles {
            if let Some(s) = suffix2(h) {
                *count.entry(s).or_default() += 1;
            }
        }
        Hosting(count.into_iter().filter(|&(_, c)| c >= min).map(|(s, _)| s.to_string()).collect())
    }

    pub fn contains(&self, s: &str) -> bool {
        self.0.contains(s)
    }

    pub fn sorted(&self) -> Vec<String> {
        let mut v: Vec<String> = self.0.iter().cloned().collect();
        v.sort();
        v
    }
}

/// The last two labels of a handle with at least three.
fn suffix2(h: &str) -> Option<&str> {
    let mut dots = h.rmatch_indices('.');
    dots.next()?;
    let (i, _) = dots.next()?;
    Some(&h[i + 1..])
}

/// Appends the handle's keys: the folded handle, and its label and hyphen parts after the first
/// inside the owner's part (not a hosting suffix, not the TLD).
pub fn handle_keys(handle: &str, hosting: &Hosting, buf: &mut String, out: &mut Vec<Key>) {
    if handle.is_empty() || handle == "handle.invalid" {
        return;
    }
    fold_into(handle, buf);
    if buf.is_empty() {
        return;
    }
    let labels = handle.split('.').count();
    let own_labels = match suffix2(handle) {
        Some(s) if hosting.contains(s) => labels - 2,
        _ => labels.saturating_sub(1).max(1),
    };
    let own: String = handle.split('.').take(own_labels).collect::<Vec<_>>().join(".");
    let mut own_f = String::new();
    fold_into(&own, &mut own_f);
    out.push((cut(buf).as_bytes().into(), HANDLE));
    let mut starts = Vec::new();
    word_starts(buf, HANDLE_SEGMENTS, &mut starts);
    for &s in &starts {
        if s < own_f.len() {
            out.push((cut(&buf[s..]).as_bytes().into(), HSEG));
        }
    }
}

/// Appends the display name's keys: its whole fold, then one from each of its first later words.
pub fn name_keys(name: &str, buf: &mut String, out: &mut Vec<Key>) {
    if name.is_empty() {
        return;
    }
    fold_into(name, buf);
    if buf.is_empty() {
        return;
    }
    out.push((cut(buf).as_bytes().into(), NAME));
    let mut starts = Vec::new();
    word_starts(buf, NAME_WORDS, &mut starts);
    for &s in &starts {
        out.push((cut(&buf[s..]).as_bytes().into(), NSEG));
    }
}

/// Sorts keys and keeps one per string, its best (lowest) tier.
pub fn normalise(out: &mut Vec<Key>) {
    out.sort_unstable();
    out.dedup_by(|b, a| a.0 == b.0);
}

/// Every key of an actor, normalised.
pub fn keys(handle: &str, name: &str, hosting: &Hosting, buf: &mut String, out: &mut Vec<Key>) {
    out.clear();
    handle_keys(handle, hosting, buf, out);
    name_keys(name, buf, out);
    normalise(out);
}

pub fn is_handle_tier(t: u8) -> bool {
    t == HANDLE || t == HSEG
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ks(h: &str, n: &str) -> Vec<(String, u8)> {
        let hosting = Hosting::new(["bsky.social".to_string()]);
        let mut out = Vec::new();
        keys(h, n, &hosting, &mut String::new(), &mut out);
        out.into_iter().map(|(k, t)| (String::from_utf8(k.into()).unwrap(), t)).collect()
    }

    #[test]
    fn handle_and_name_keys() {
        assert_eq!(
            ks("mary-jane.bsky.social", "Mary Jane Doe"),
            vec![
                ("doe".into(), NSEG),
                ("jane bsky social".into(), HSEG),
                ("jane doe".into(), NSEG),
                ("mary jane bsky social".into(), HANDLE),
                ("mary jane doe".into(), NAME),
            ]
        );
        assert_eq!(
            ks("alice.example.com", ""),
            vec![("alice example com".into(), HANDLE), ("example com".into(), HSEG)]
        );
        assert_eq!(ks("bob.bsky.social", ""), vec![("bob bsky social".into(), HANDLE)]);
        assert!(ks("handle.invalid", "").is_empty());
        // the same string from the handle and the name keeps the handle's tier
        assert_eq!(ks("jay.com", "Jay.com"), vec![("com".into(), NSEG), ("jay com".into(), HANDLE)]);
    }

    #[test]
    fn long_keys_cut() {
        let k = ks("", "a very long display name that goes past the cap for sure");
        assert!(k.iter().all(|(s, _)| s.len() <= MAXK));
        assert_eq!(cut("ééééééééééééééééé"), "éééééééééééééééé");
    }

    #[test]
    fn hosting_from_counts() {
        let hs = ["a.bsky.social", "b.bsky.social", "c.example.com", "x.com"];
        let h = Hosting::from_handles(hs.iter().copied(), 2);
        assert_eq!(h.sorted(), vec!["bsky.social".to_string()]);
    }
}

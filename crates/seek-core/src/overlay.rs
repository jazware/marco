//! The live overlay: what changed since the base was built. An actor the
//! overlay speaks for (a base actor whose handle, name or status changed, or
//! an actor the base doesn't have) is represented by the overlay alone: a
//! query skips its base postings and reads its keys here. It's small (one
//! artifact interval of changes) and replaced whole at each swap.

use std::collections::BTreeSet;

use rustc_hash::FxHashMap;

use crate::did::{key_of, DidKey};
use crate::index::{posting, tier, Index};
use crate::keys::{self, is_handle_tier, Key, HANDLE};

#[derive(Clone, Debug, Default)]
struct Actor {
    handle: Vec<Key>,
    name: Vec<Key>,
    hidden: bool,
    rank: u16,
    handle_hash: u32,
}

pub struct Overlay {
    base_n: u32,
    actors: FxHashMap<u32, Actor>,
    new_ids: FxHashMap<DidKey, u32>,
    new_dids: Vec<DidKey>,
    keys: BTreeSet<(Box<[u8]>, u32)>,
    key_bytes: usize,
    /// Jetstream's `time_us` of the last event applied.
    pub cursor_us: u64,
    /// Counts every change, so a reader can tell the overlay moved.
    pub epoch: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub actors: usize,
    pub new_actors: usize,
    pub keys: usize,
    pub key_bytes: usize,
}

impl Overlay {
    pub fn new(base: &Index) -> Overlay {
        Overlay {
            base_n: base.len(),
            actors: FxHashMap::default(),
            new_ids: FxHashMap::default(),
            new_dids: Vec::new(),
            keys: BTreeSet::new(),
            key_bytes: 0,
            cursor_us: base.meta.cursor_us,
            epoch: 0,
        }
    }

    pub fn stats(&self) -> Stats {
        Stats {
            actors: self.actors.len(),
            new_actors: self.new_dids.len(),
            keys: self.keys.len(),
            key_bytes: self.key_bytes,
        }
    }

    /// A DID's id: the base's, or one this overlay gave it.
    pub fn id_of(&self, base: &Index, did: &DidKey) -> Option<u32> {
        base.id_of_key(did).or_else(|| self.new_ids.get(did).copied())
    }

    pub fn did(&self, base: &Index, id: u32) -> Option<String> {
        if id < self.base_n {
            base.did(id)
        } else {
            self.new_dids.get((id - self.base_n) as usize).map(DidKey::to_did)
        }
    }

    /// Whether the overlay speaks for `id`, so its base postings don't count.
    #[inline]
    pub fn shadows(&self, id: u32) -> bool {
        !self.actors.is_empty() && self.actors.contains_key(&id)
    }

    #[inline]
    pub fn hidden(&self, base: &Index, id: u32) -> bool {
        match self.actors.get(&id) {
            Some(a) => a.hidden,
            None => id < self.base_n && base.hidden(id),
        }
    }

    /// The fingerprint of the actor's handle now (`index::handle_hash`), 0 for none.
    #[inline]
    pub fn handle_hash(&self, base: &Index, id: u32) -> u32 {
        match self.actors.get(&id) {
            Some(a) => a.handle_hash,
            None if id < self.base_n => base.handle_hash(id),
            None => 0,
        }
    }

    #[inline]
    pub fn rank(&self, base: &Index, id: u32) -> u16 {
        match self.actors.get(&id) {
            Some(a) => a.rank,
            None if id < self.base_n => base.rank(id),
            None => 0,
        }
    }

    /// Calls `f(key, tier)` for each key `id` has now.
    pub fn keys_of(&self, base: &Index, id: u32, mut f: impl FnMut(&[u8], u8)) {
        match self.actors.get(&id) {
            Some(a) => a.handle.iter().chain(&a.name).for_each(|(k, t)| f(k, *t)),
            None if id < self.base_n => base.keys_of(id, f),
            None => {}
        }
    }

    fn take(&mut self, base: &Index, did: &str, create: bool) -> Option<(u32, Actor)> {
        let key = key_of(did);
        let id = match self.id_of(base, &key) {
            Some(id) => id,
            None if create => {
                let id = self.base_n + self.new_dids.len() as u32;
                self.new_ids.insert(key.clone(), id);
                self.new_dids.push(key);
                id
            }
            None => return None,
        };
        let a = match self.actors.remove(&id) {
            Some(a) => {
                let mut all: Vec<Key> = a.handle.iter().chain(&a.name).cloned().collect();
                keys::normalise(&mut all);
                for (k, t) in all {
                    if self.keys.remove(&(k.clone(), posting(id, t))) {
                        self.key_bytes -= k.len() + 40;
                    }
                }
                a
            }
            None if id < self.base_n => {
                let mut a = Actor {
                    hidden: base.hidden(id),
                    rank: base.rank(id),
                    handle_hash: base.handle_hash(id),
                    ..Default::default()
                };
                base.keys_of(id, |k, t| {
                    let side = if is_handle_tier(t) { &mut a.handle } else { &mut a.name };
                    side.push((k.into(), t));
                });
                a
            }
            None => Actor::default(),
        };
        Some((id, a))
    }

    fn put(&mut self, id: u32, a: Actor) {
        // one key per string across both sides, its best tier, as the base has it
        let mut all: Vec<Key> = a.handle.iter().chain(&a.name).cloned().collect();
        keys::normalise(&mut all);
        for (k, t) in all {
            let l = k.len();
            if self.keys.insert((k, posting(id, t))) {
                self.key_bytes += l + 40;
            }
        }
        self.actors.insert(id, a);
        self.epoch += 1;
    }

    /// The actor's verified handle now: None indexes it with no handle.
    pub fn set_handle(&mut self, base: &Index, did: &str, handle: Option<&str>) {
        let Some((id, mut a)) = self.take(base, did, handle.is_some()) else { return };
        a.handle.clear();
        a.handle_hash = crate::index::handle_hash(handle.unwrap_or(""));
        if let Some(h) = handle {
            keys::handle_keys(h, &base.hosting, &mut String::new(), &mut a.handle);
        }
        self.put(id, a);
    }

    /// The actor's display name now (None or blank: none).
    pub fn set_name(&mut self, base: &Index, did: &str, name: Option<&str>) {
        let name = name.filter(|n| !n.trim().is_empty());
        let Some((id, mut a)) = self.take(base, did, name.is_some()) else { return };
        a.name.clear();
        if let Some(n) = name {
            keys::name_keys(n, &mut String::new(), &mut a.name);
        }
        self.put(id, a);
    }

    /// Taken down, deleted or deactivated (true), or active again.
    pub fn set_hidden(&mut self, base: &Index, did: &str, hidden: bool) {
        let key = key_of(did);
        let Some(id) = self.id_of(base, &key) else { return };
        if !self.actors.contains_key(&id) && id < self.base_n && base.hidden(id) == hidden {
            return;
        }
        let Some((id, mut a)) = self.take(base, did, false) else { return };
        a.hidden = hidden;
        self.put(id, a);
    }

    /// The folded handle the actor is indexed under now, if any (cut as keys are).
    pub fn handle_key(&self, base: &Index, did: &str) -> Option<Vec<u8>> {
        let id = self.id_of(base, &key_of(did))?;
        let mut out = None;
        self.keys_of(base, id, |k, t| {
            if t == HANDLE {
                out = Some(k.to_vec());
            }
        });
        out
    }

    /// Overlay postings whose key starts with `q`, in key order.
    pub fn range<'a>(&'a self, q: &'a [u8]) -> impl Iterator<Item = (&'a [u8], u32)> + 'a {
        let start: (Box<[u8]>, u32) = (q.into(), 0);
        self.keys.range(start..).take_while(move |(k, _)| k.starts_with(q)).map(|(k, p)| (&**k, *p))
    }

    /// Overlay exact handle matches of `q`.
    pub fn exact_handles<'a>(&'a self, q: &'a [u8]) -> impl Iterator<Item = u32> + 'a {
        self.range(q).filter(move |(k, p)| *k == q && tier(*p) == HANDLE).map(|(_, p)| p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold::fold;
    use crate::index::actor;
    use crate::index::tests::{build_people, people};

    fn matches(ov: &Overlay, q: &str) -> Vec<u32> {
        let f = fold(q);
        let mut v: Vec<u32> = ov.range(f.as_bytes()).map(|(_, p)| actor(p)).collect();
        v.dedup();
        v
    }

    #[test]
    fn changes_shadow_the_base() {
        let ps = people();
        let ix = build_people(&ps, &[]);
        let mut ov = Overlay::new(&ix);
        assert_eq!(ov.cursor_us, 42);
        let alice = ix.id_of(&ps[0].did).unwrap();
        assert!(!ov.shadows(alice));

        ov.set_name(&ix, &ps[0].did, Some("Zelda"));
        assert!(ov.shadows(alice));
        assert_eq!(matches(&ov, "zel"), vec![alice]);
        assert_eq!(matches(&ov, "alice b"), vec![alice], "the handle's keys stay");
        assert!(matches(&ov, "alice s").is_empty(), "the old name's keys go");
        assert_eq!(ov.rank(&ix, alice), 100);

        ov.set_handle(&ix, &ps[0].did, Some("zed.example.com"));
        assert!(matches(&ov, "alice").is_empty());
        assert_eq!(matches(&ov, "example"), vec![alice]);
        assert_eq!(ov.exact_handles(fold("zed.example.com").as_bytes()).count(), 1);
        assert_eq!(ov.handle_key(&ix, &ps[0].did).unwrap(), b"zed example com");
        assert_eq!(ov.handle_hash(&ix, alice), crate::index::handle_hash("zed.example.com"));

        ov.set_handle(&ix, &ps[0].did, None);
        assert!(matches(&ov, "zed").is_empty());
        assert_eq!(matches(&ov, "zel"), vec![alice]);

        let new = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        ov.set_hidden(&ix, new, true);
        assert_eq!(ov.stats().new_actors, 0, "an account event alone adds no one");
        ov.set_name(&ix, new, Some("Newcomer"));
        let nid = ov.id_of(&ix, &key_of(new)).unwrap();
        assert_eq!(nid, ix.len());
        assert_eq!(ov.did(&ix, nid).unwrap(), new);
        assert_eq!(ov.rank(&ix, nid), 0);
        assert_eq!(matches(&ov, "newc"), vec![nid]);

        let bob = ix.id_of(&ps[3].did).unwrap();
        ov.set_hidden(&ix, &ps[3].did, false);
        assert!(!ov.shadows(bob), "no change, nothing to shadow");
        ov.set_hidden(&ix, &ps[3].did, true);
        assert!(ov.hidden(&ix, bob));
        assert_eq!(matches(&ov, "jose"), vec![bob], "a hidden actor keeps its keys for when it's back");
        assert_eq!(ov.stats().actors, 3);
        // every key the overlay holds belongs to an actor it speaks for, and the byte count is exact
        assert!(ov.keys.iter().all(|(_, p)| ov.actors.contains_key(&actor(*p))));
        assert_eq!(ov.stats().key_bytes, ov.keys.iter().map(|(k, _)| k.len() + 40).sum::<usize>());
    }
}

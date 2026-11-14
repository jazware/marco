//! DIDs as the artifact keeps them: marco's packing (a did:plc in 15 bytes,
//! in string order), and seek's ids in [`DidKey`] order: every did:plc
//! first, then every other DID by string, the order marco's ranks file uses.

pub use marco_core::did::{pack_plc, plc_from_bytes, plc_to_bytes, unpack_plc, DidKey, PACKED_LEN as PACKED};

pub fn key_of(did: &str) -> DidKey {
    match pack_plc(did) {
        Some(v) => DidKey::Plc(v),
        None => DidKey::Other(did.into()),
    }
}

/// Bucket starts by the top `bits` of the sorted packed values, about 16 a bucket.
pub fn radix(plc: impl ExactSizeIterator<Item = u128> + Clone) -> (u32, Vec<u32>) {
    let bits = (64 - ((plc.len() / 16).max(1) as u64).leading_zeros()).clamp(4, 28);
    let n = 1usize << bits;
    let mut starts = vec![0u32; n + 1];
    for v in plc {
        starts[(v >> (120 - bits)) as usize + 1] += 1;
    }
    for i in 0..n {
        starts[i + 1] += starts[i];
    }
    (bits, starts)
}

/// The handle syntax from the atproto spec, lowercase.
pub fn valid_handle(h: &str) -> bool {
    let labels: Vec<&str> = h.split('.').collect();
    h.len() <= 253
        && labels.len() >= 2
        && labels.iter().all(|l| {
            (1..=63).contains(&l.len())
                && l.bytes().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
                && !l.starts_with('-')
                && !l.ends_with('-')
        })
        && labels.last().is_some_and(|tld| tld.as_bytes()[0].is_ascii_alphabetic())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seek_order() {
        let a = "did:plc:22222222222222222222222a";
        let b = "did:plc:z72i7hdynmk6r22z27h6tvur";
        let mut v = [key_of("did:web:example.com"), key_of(b), key_of("did:key:x"), key_of(a)];
        v.sort();
        assert_eq!(v.map(|k| k.to_did()), [a, b, "did:key:x", "did:web:example.com"]);
    }

    #[test]
    fn handles() {
        assert!(valid_handle("alice.bsky.social"));
        assert!(!valid_handle("Alice.bsky.social"));
        assert!(!valid_handle("alice"));
        assert!(!valid_handle("a.1com"));
    }
}

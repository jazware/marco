//! Folding: the one normalisation both the index keys and the query go through.
//!
//! NFKD (so fullwidth forms, ligatures and the "fancy font" math letters become
//! plain ones), Latin/Greek/Cyrillic diacritics dropped, NFC again (Hangul
//! syllables recompose), lowercase, `ß` to `ss`. Anything that isn't a letter or
//! a digit (dots and hyphens in handles, punctuation, emoji) is a word break, and
//! words are joined by one space. So `@Mary-Jane.bsky.social` and `mary jane b`
//! fold to keys that share a prefix.

use unicode_normalization::UnicodeNormalization;

fn is_diacritic(c: char) -> bool {
    matches!(c as u32, 0x0300..=0x036F | 0x1AB0..=0x1AFF | 0x1DC0..=0x1DFF | 0x20D0..=0x20FF | 0xFE20..=0xFE2F)
}

/// Scripts written without spaces between words: every character starts a word.
pub fn is_cjk(c: char) -> bool {
    matches!(c as u32,
        0x3040..=0x30FF | 0x31F0..=0x31FF | 0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xF900..=0xFAFF
        | 0xAC00..=0xD7AF | 0x20000..=0x2A6DF)
}

pub fn fold_into(s: &str, out: &mut String) {
    out.clear();
    let mut sep = false;
    let base: String = s.nfkd().filter(|&c| !is_diacritic(c)).nfc().collect();
    for c in base.chars() {
        if c == 'ß' {
            out.push_str("ss");
            sep = false;
            continue;
        }
        if c.is_alphanumeric() {
            if sep && !out.is_empty() {
                out.push(' ');
            }
            sep = false;
            for l in c.to_lowercase() {
                out.push(l);
            }
        } else {
            sep = true;
        }
    }
}

pub fn fold(s: &str) -> String {
    let mut o = String::new();
    fold_into(s, &mut o);
    o
}

/// Byte offsets where a suffix key may start: after each space, and before
/// each CJK character that follows another character of the same word.
pub fn word_starts(f: &str, max: usize, out: &mut Vec<usize>) {
    out.clear();
    let mut prev: Option<char> = None;
    for (i, c) in f.char_indices() {
        if out.len() >= max {
            break;
        }
        let start = match prev {
            None => false,
            Some(' ') => c != ' ',
            Some(p) => is_cjk(c) && p != ' ',
        };
        if start {
            out.push(i);
        }
        prev = Some(c);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folds() {
        assert_eq!(fold("@Mary-Jane.bsky.social"), "mary jane bsky social");
        assert_eq!(fold("José Ñúñez"), "jose nunez");
        assert_eq!(fold("𝓙𝓪𝓷𝓮 ✨🏳️‍⚧️"), "jane");
        assert_eq!(fold("ＡＢＣ"), "abc");
        assert_eq!(fold("Straße"), "strasse");
        assert_eq!(fold("山田 太郎"), "山田 太郎");
        assert_eq!(fold("김철수"), "김철수");
        let mut w = Vec::new();
        word_starts("山田太郎", 8, &mut w);
        assert_eq!(w, vec![3, 6, 9]);
        word_starts("jane doe smith", 8, &mut w);
        assert_eq!(w, vec![5, 9]);
    }
}

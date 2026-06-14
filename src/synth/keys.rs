//! Unique key synthesis formatted to match observed data.

use polars::prelude::DataType;
use rand::Rng;
use rand_chacha::ChaCha8Rng;

use crate::synth::profile_input::SynthColumn;

/// How key values for a column are constructed.
#[derive(Debug)]
pub enum KeyKind {
    SequentialInt { start: i64 },
    PaddedDigits { width: usize },
    UuidLike,
    PatternCounter { pattern: String },
}

/// Detects the key construction strategy from the column's profile.
pub fn detect_key_kind(col: &SynthColumn) -> KeyKind {
    if col.dtype.is_primitive_numeric() || matches!(col.dtype, DataType::Decimal(_, _)) {
        let start = col.quantiles.as_ref().map(|q| q[0] as i64).unwrap_or(1);
        return KeyKind::SequentialInt { start };
    }
    let top_pattern = col
        .pattern_sample
        .first()
        .map(|p| p.value.as_str())
        .unwrap_or("");
    if col.min_length == 36 && col.max_length == 36 && top_pattern.matches('-').count() == 4 {
        return KeyKind::UuidLike;
    }
    if top_pattern == "N" && col.min_length == col.max_length && col.min_length > 0 {
        return KeyKind::PaddedDigits {
            width: col.min_length,
        };
    }
    if top_pattern.is_empty() {
        return KeyKind::PaddedDigits { width: 8 };
    }
    KeyKind::PatternCounter {
        pattern: top_pattern.to_string(),
    }
}

/// Produces the key value for a row index. Unique by construction for every
/// kind: ints/padded embed the index, UUIDs consume 16 rng bytes per index in
/// stream order, pattern keys replace their last digit run with the index.
pub fn key_string(kind: &KeyKind, index: usize, rng: &mut ChaCha8Rng) -> String {
    match kind {
        // NOTE: SequentialInt keys are emitted directly as an i64 Series in the
        // engine (see is_numeric_kind); this arm exists for completeness.
        KeyKind::SequentialInt { start } => (start + index as i64).to_string(),
        KeyKind::PaddedDigits { width } => format!("{:0width$}", index + 1, width = width),
        KeyKind::UuidLike => {
            let mut bytes = [0u8; 16];
            rng.fill(&mut bytes);
            uuid::Builder::from_random_bytes(bytes)
                .into_uuid()
                .to_string()
        }
        KeyKind::PatternCounter { pattern } => {
            let idx_str = (index + 1).to_string();
            // Replace the LAST digit-run token with the index; if the pattern
            // has no digit run, append the index. Use char_indices (byte
            // offsets) consistently so multibyte pattern chars don't desync.
            let last_n_byte = pattern
                .char_indices()
                .filter(|(_, c)| *c == 'N')
                .map(|(byte_pos, _)| byte_pos)
                .next_back();
            if let Some(n_byte) = last_n_byte {
                let mut out = String::new();
                for (byte_pos, c) in pattern.char_indices() {
                    match c {
                        'N' if byte_pos == n_byte => out.push_str(&idx_str),
                        'N' => out.push('0'),
                        'a' => out.push((b'a' + (index % 26) as u8) as char),
                        'd' => out.push('0'),
                        other => out.push(other),
                    }
                }
                out
            } else {
                format!("{pattern}{idx_str}")
            }
        }
    }
}

/// True when generated key strings should be parsed back to integers
/// (numeric key columns build an integer Series, not strings).
pub fn is_numeric_kind(kind: &KeyKind) -> bool {
    matches!(kind, KeyKind::SequentialInt { .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{profiler::ValueFrequency, synth::samplers::stream_rng};
    use polars::prelude::DataType;

    fn col(
        dtype: DataType,
        pattern: &str,
        min_len: usize,
        max_len: usize,
        min: Option<&str>,
    ) -> crate::synth::profile_input::SynthColumn {
        crate::synth::profile_input::SynthColumn {
            name: "k".into(),
            dtype,
            null_percentage: 0.0,
            non_null_count: 100,
            distinct_count: 100,
            unique_ratio: 1.0,
            histogram: None,
            quantiles: min.map(|m| {
                let v = m.parse::<f64>().unwrap();
                vec![v, v, v, v, v + 100.0]
            }),
            top_values: vec![],
            pattern_sample: if pattern.is_empty() {
                vec![]
            } else {
                vec![ValueFrequency {
                    value: pattern.into(),
                    freq: 100,
                }]
            },
            min_length: min_len,
            max_length: max_len,
        }
    }

    #[test]
    fn integer_keys_are_sequential_from_observed_min() {
        let c = col(DataType::Int64, "", 1, 1, Some("1000"));
        let kind = detect_key_kind(&c);
        assert!(matches!(kind, KeyKind::SequentialInt { start: 1000 }));
        let mut rng = stream_rng(1, "t", "k", 0);
        assert_eq!(key_string(&kind, 0, &mut rng), "1000");
        assert_eq!(key_string(&kind, 5, &mut rng), "1005");
    }

    #[test]
    fn padded_digit_keys_keep_width() {
        let c = col(DataType::String, "N", 8, 8, None);
        let kind = detect_key_kind(&c);
        assert!(matches!(kind, KeyKind::PaddedDigits { width: 8 }));
        let mut rng = stream_rng(1, "t", "k", 0);
        assert_eq!(key_string(&kind, 41, &mut rng), "00000042"); // 1-based to avoid all-zero key
    }

    #[test]
    fn uuid_shaped_keys_are_valid_and_deterministic() {
        let c = col(DataType::String, "NaN-aNaN-Na-aNa-NaNa", 36, 36, None);
        let kind = detect_key_kind(&c);
        assert!(matches!(kind, KeyKind::UuidLike));
        let mut r1 = stream_rng(1, "t", "k", 0);
        let mut r2 = stream_rng(1, "t", "k", 0);
        let a = key_string(&kind, 0, &mut r1);
        let b = key_string(&kind, 0, &mut r2);
        assert_eq!(a, b, "deterministic");
        assert_eq!(a.len(), 36);
        assert_eq!(a.matches('-').count(), 4);
    }

    #[test]
    fn pattern_keys_embed_index_for_uniqueness() {
        let c = col(DataType::String, "aaa-N", 7, 9, None);
        let kind = detect_key_kind(&c);
        let mut rng = stream_rng(1, "t", "k", 0);
        let mut seen = std::collections::HashSet::new();
        for i in 0..1000 {
            let k = key_string(&kind, i, &mut rng);
            assert!(seen.insert(k.clone()), "duplicate key {k} at {i}");
        }
    }

    #[test]
    fn pattern_keys_with_non_ascii_are_unique() {
        // A pattern containing a multibyte char before the N run (e.g. from
        // source data like "café-0001"): byte offset != char index.
        let c = col(DataType::String, "café-N", 8, 12, None);
        let kind = detect_key_kind(&c);
        let mut rng = stream_rng(1, "t", "k", 0);
        let mut seen = std::collections::HashSet::new();
        for i in 0..1000 {
            let k = key_string(&kind, i, &mut rng);
            assert!(seen.insert(k.clone()), "duplicate key {k} at index {i}");
        }
    }
}

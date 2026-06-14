//! Deterministic seeded samplers for synthetic value generation.

#![allow(dead_code)]

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sha2::{Digest, Sha256};

use crate::profiler::{HistogramBucket, ValueFrequency};

/// Derives an isolated, reproducible RNG stream for (seed, table, column, round).
/// Adding or removing other columns never perturbs this stream.
pub fn stream_rng(seed: u64, table: &str, column: &str, round: u64) -> ChaCha8Rng {
    let mut hasher = Sha256::new();
    hasher.update(seed.to_le_bytes());
    hasher.update(table.as_bytes());
    hasher.update([0u8]);
    hasher.update(column.as_bytes());
    hasher.update([0u8]);
    hasher.update(round.to_le_bytes());
    let digest = hasher.finalize();
    let mut seed_bytes = [0u8; 32];
    seed_bytes.copy_from_slice(&digest);
    ChaCha8Rng::from_seed(seed_bytes)
}

/// Samples a value from a histogram: weighted bucket pick, uniform within.
pub fn sample_histogram(buckets: &[HistogramBucket], rng: &mut ChaCha8Rng) -> f64 {
    let total: u64 = buckets.iter().map(|b| b.count).sum();
    if total == 0 {
        return buckets.first().map(|b| b.lo).unwrap_or(0.0);
    }
    let mut target = rng.gen_range(0..total);
    for b in buckets {
        if target < b.count {
            return b.lo + (b.hi - b.lo) * rng.r#gen::<f64>();
        }
        target -= b.count;
    }
    buckets.last().map(|b| b.hi).unwrap_or(0.0)
}

/// Maps a uniform u ∈ [0,1] through the histogram's empirical CDF (for copula).
pub fn histogram_quantile(buckets: &[HistogramBucket], u: f64) -> f64 {
    let total: u64 = buckets.iter().map(|b| b.count).sum();
    if total == 0 {
        return buckets.first().map(|b| b.lo).unwrap_or(0.0);
    }
    let target = u.clamp(0.0, 1.0) * total as f64;
    let mut cum = 0.0;
    for b in buckets {
        let next = cum + b.count as f64;
        if target <= next && b.count > 0 {
            let frac = ((target - cum) / b.count as f64).clamp(0.0, 1.0);
            return b.lo + (b.hi - b.lo) * frac;
        }
        cum = next;
    }
    buckets.last().map(|b| b.hi).unwrap_or(0.0)
}

/// Maps a uniform u through a piecewise-linear CDF over [min,p25,median,p75,max].
pub fn quantiles_quantile(q: &[f64], u: f64) -> f64 {
    assert_eq!(
        q.len(),
        5,
        "quantiles_quantile requires exactly 5 quantile points"
    );
    let u = u.clamp(0.0, 1.0);
    let seg = (u * 4.0).floor().min(3.0) as usize; // 0..=3
    let frac = u * 4.0 - seg as f64;
    q[seg] + (q[seg + 1] - q[seg]) * frac
}

/// Picks an index into `values` weighted by frequency.
///
/// # Precondition
///
/// The slice must be non-empty; callers are responsible for guarding this.
/// An empty slice (or one whose total frequency is zero) returns `0` as a
/// non-panicking fallback, but that index is not valid to dereference and
/// callers must not rely on it producing a meaningful value.
pub fn sample_weighted_index(values: &[ValueFrequency], rng: &mut ChaCha8Rng) -> usize {
    let total: usize = values.iter().map(|v| v.freq).sum();
    if total == 0 || values.is_empty() {
        return 0;
    }
    let mut target = rng.gen_range(0..total);
    for (i, v) in values.iter().enumerate() {
        if target < v.freq {
            return i;
        }
        target -= v.freq;
    }
    values.len() - 1
}

/// Generates a string matching a dtoo pattern (`a`=letter, `d`=digit,
/// `N`=digit run, everything else literal), targeting the observed length range.
pub fn generate_from_pattern(
    pattern: &str,
    min_len: usize,
    max_len: usize,
    rng: &mut ChaCha8Rng,
) -> String {
    let n_runs = pattern.chars().filter(|c| *c == 'N').count();
    let fixed: usize = pattern.chars().filter(|c| *c != 'N').count();
    let mut run_lengths = vec![0usize; n_runs];
    if n_runs > 0 {
        let budget_min = min_len.saturating_sub(fixed).max(n_runs);
        let budget_max = max_len.saturating_sub(fixed).max(budget_min);
        let total_digits = if budget_min == budget_max {
            budget_min
        } else {
            rng.gen_range(budget_min..=budget_max)
        };
        let base = total_digits / n_runs;
        let extra = total_digits % n_runs;
        for (i, len) in run_lengths.iter_mut().enumerate() {
            *len = base + usize::from(i < extra);
        }
    }
    let mut out = String::new();
    let mut run_idx = 0;
    for c in pattern.chars() {
        match c {
            'a' => out.push((b'a' + rng.gen_range(0..26u8)) as char),
            'd' => out.push((b'0' + rng.gen_range(0..10u8)) as char),
            'N' => {
                for _ in 0..run_lengths[run_idx] {
                    out.push((b'0' + rng.gen_range(0..10u8)) as char);
                }
                run_idx += 1;
            }
            other => out.push(other),
        }
    }
    out
}

/// Decides nullness for one value at the observed null percentage.
pub fn is_null_draw(null_percentage: f64, rng: &mut ChaCha8Rng) -> bool {
    null_percentage > 0.0 && rng.r#gen::<f64>() * 100.0 < null_percentage
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiler::{HistogramBucket, ValueFrequency};

    #[test]
    fn stream_rng_is_deterministic_and_column_isolated() {
        let mut a1 = stream_rng(42, "t", "a", 0);
        let mut a2 = stream_rng(42, "t", "a", 0);
        let mut b = stream_rng(42, "t", "b", 0);
        let mut a_round1 = stream_rng(42, "t", "a", 1);
        let v1: f64 = a1.r#gen();
        assert_eq!(v1, a2.r#gen::<f64>(), "same stream reproduces");
        assert_ne!(v1, b.r#gen::<f64>(), "different column, different stream");
        assert_ne!(
            v1,
            a_round1.r#gen::<f64>(),
            "different round, different stream"
        );
    }

    #[test]
    fn histogram_sampling_respects_bucket_weights_and_bounds() {
        let buckets = vec![
            HistogramBucket {
                lo: 0.0,
                hi: 10.0,
                count: 90,
            },
            HistogramBucket {
                lo: 10.0,
                hi: 100.0,
                count: 10,
            },
        ];
        let mut rng = stream_rng(1, "t", "c", 0);
        let mut low = 0;
        for _ in 0..1000 {
            let v = sample_histogram(&buckets, &mut rng);
            assert!((0.0..=100.0).contains(&v));
            if v <= 10.0 {
                low += 1;
            }
        }
        assert!(
            (800..=980).contains(&low),
            "≈90% in first bucket, got {low}"
        );
    }

    #[test]
    fn histogram_quantile_maps_uniform_through_cdf() {
        let buckets = vec![
            HistogramBucket {
                lo: 0.0,
                hi: 10.0,
                count: 50,
            },
            HistogramBucket {
                lo: 10.0,
                hi: 20.0,
                count: 50,
            },
        ];
        assert!((histogram_quantile(&buckets, 0.0) - 0.0).abs() < 1e-9);
        assert!((histogram_quantile(&buckets, 0.5) - 10.0).abs() < 1e-9);
        assert!((histogram_quantile(&buckets, 1.0) - 20.0).abs() < 1e-9);
        assert!((histogram_quantile(&buckets, 0.25) - 5.0).abs() < 1e-9);
    }

    #[test]
    fn quantile_fallback_interpolates_five_points() {
        let q = vec![0.0, 25.0, 50.0, 75.0, 100.0];
        assert!((quantiles_quantile(&q, 0.0) - 0.0).abs() < 1e-9);
        assert!((quantiles_quantile(&q, 0.5) - 50.0).abs() < 1e-9);
        assert!((quantiles_quantile(&q, 0.125) - 12.5).abs() < 1e-9);
        assert!((quantiles_quantile(&q, 1.0) - 100.0).abs() < 1e-9);
    }

    #[test]
    fn weighted_value_sampling_follows_frequencies() {
        let values = vec![
            ValueFrequency {
                value: "a".into(),
                freq: 90,
            },
            ValueFrequency {
                value: "b".into(),
                freq: 10,
            },
        ];
        let mut rng = stream_rng(7, "t", "c", 0);
        let a_count = (0..1000)
            .filter(|_| values[sample_weighted_index(&values, &mut rng)].value == "a")
            .count();
        assert!((830..=960).contains(&a_count), "≈90% a, got {a_count}");
    }

    #[test]
    fn pattern_generation_matches_shape() {
        let mut rng = stream_rng(3, "t", "c", 0);
        for _ in 0..100 {
            let s = generate_from_pattern("aaa-N", 5, 8, &mut rng);
            assert!(s.len() >= 5 && s.len() <= 8, "length {} of {s}", s.len());
            let bytes = s.as_bytes();
            assert!(bytes[0].is_ascii_lowercase());
            assert!(bytes[1].is_ascii_lowercase());
            assert!(bytes[2].is_ascii_lowercase());
            assert_eq!(bytes[3], b'-');
            assert!(bytes[4..].iter().all(u8::is_ascii_digit));
        }
        // Pattern with literal-only content just reproduces literals.
        assert_eq!(generate_from_pattern("--", 2, 2, &mut rng), "--");
    }

    #[test]
    fn null_draws_match_percentage() {
        let mut rng = stream_rng(9, "t", "c", 0);
        let nulls = (0..10_000).filter(|_| is_null_draw(25.0, &mut rng)).count();
        assert!((2200..=2800).contains(&nulls), "≈25% nulls, got {nulls}");
    }

    #[test]
    fn stream_rng_is_table_isolated() {
        let mut t1 = stream_rng(42, "orders", "id", 0);
        let mut t2 = stream_rng(42, "customers", "id", 0);
        assert_ne!(
            t1.r#gen::<u64>(),
            t2.r#gen::<u64>(),
            "same column name, different table → different stream"
        );
    }

    #[test]
    fn pattern_generation_distributes_multiple_runs() {
        let mut rng = stream_rng(1, "t", "c", 0);
        for _ in 0..100 {
            // "NaN" = digit-run, letter, digit-run; fixed=1 (the 'a'); target length 3..=8
            let s = generate_from_pattern("NaN", 3, 8, &mut rng);
            assert!(s.len() >= 3 && s.len() <= 8, "len {} of {s}", s.len());
            // exactly one lowercase letter, rest digits
            assert_eq!(s.chars().filter(|c| c.is_ascii_alphabetic()).count(), 1);
            assert!(s.chars().filter(|c| c.is_ascii_digit()).count() >= 2);
        }
    }
}

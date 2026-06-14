//! Gaussian copula: PSD repair, Cholesky, correlated uniform generation.
//! Matrices here are k×k for k profiled numeric columns — small — so the
//! linear algebra is implemented in-module rather than adding a crate.

#![allow(dead_code)] // consumed by Task 12 (engine)

use rand::Rng;
use rand_chacha::ChaCha8Rng;

/// Cyclic Jacobi eigendecomposition of a symmetric matrix.
/// Returns (eigenvalues, eigenvectors-as-columns).
#[allow(clippy::needless_range_loop)]
pub fn jacobi_eigen(matrix: &[Vec<f64>]) -> (Vec<f64>, Vec<Vec<f64>>) {
    let n = matrix.len();
    let mut a: Vec<Vec<f64>> = matrix.to_vec();
    let mut v = vec![vec![0.0; n]; n];
    for (i, row) in v.iter_mut().enumerate() {
        row[i] = 1.0;
    }
    for _sweep in 0..100 {
        let mut off = 0.0;
        for i in 0..n {
            for j in (i + 1)..n {
                off += a[i][j] * a[i][j];
            }
        }
        if off < 1e-18 {
            break;
        }
        for p in 0..n {
            for q in (p + 1)..n {
                if a[p][q].abs() < 1e-15 {
                    continue;
                }
                let theta = (a[q][q] - a[p][p]) / (2.0 * a[p][q]);
                let t = theta.signum() / (theta.abs() + (theta * theta + 1.0).sqrt());
                let c = 1.0 / (t * t + 1.0).sqrt();
                let s = t * c;
                for k in 0..n {
                    let akp = a[k][p];
                    let akq = a[k][q];
                    a[k][p] = c * akp - s * akq;
                    a[k][q] = s * akp + c * akq;
                }
                for k in 0..n {
                    let apk = a[p][k];
                    let aqk = a[q][k];
                    a[p][k] = c * apk - s * aqk;
                    a[q][k] = s * apk + c * aqk;
                }
                for k in 0..n {
                    let vkp = v[k][p];
                    let vkq = v[k][q];
                    v[k][p] = c * vkp - s * vkq;
                    v[k][q] = s * vkp + c * vkq;
                }
            }
        }
    }
    let values = (0..n).map(|i| a[i][i]).collect();
    (values, v)
}

/// Repairs a sampled correlation matrix to positive definite in place:
/// clamp eigenvalues at ε, reconstruct, re-normalize diagonal to 1.
#[allow(clippy::needless_range_loop)]
pub fn psd_repair(matrix: &mut [Vec<f64>]) {
    let n = matrix.len();
    let (mut values, vectors) = jacobi_eigen(matrix);
    for v in &mut values {
        *v = v.max(1e-10);
    }
    let mut out = vec![vec![0.0; n]; n];
    for i in 0..n {
        for j in 0..n {
            let mut s = 0.0;
            for (k, lambda) in values.iter().enumerate() {
                s += vectors[i][k] * lambda * vectors[j][k];
            }
            out[i][j] = s;
        }
    }
    // Re-normalize to a correlation matrix (unit diagonal, symmetric).
    for i in 0..n {
        for j in 0..n {
            let d = (out[i][i] * out[j][j]).sqrt();
            matrix[i][j] = if d > 0.0 { out[i][j] / d } else { 0.0 };
        }
    }
    for i in 0..n {
        matrix[i][i] = 1.0;
        for j in 0..i {
            let avg = (matrix[i][j] + matrix[j][i]) / 2.0;
            matrix[i][j] = avg;
            matrix[j][i] = avg;
        }
    }
}

/// Cholesky factorization (lower-triangular). None if not positive definite.
#[allow(clippy::needless_range_loop)]
pub fn cholesky(matrix: &[Vec<f64>]) -> Option<Vec<Vec<f64>>> {
    let n = matrix.len();
    let mut l = vec![vec![0.0; n]; n];
    for i in 0..n {
        for j in 0..=i {
            let mut s = matrix[i][j];
            for k in 0..j {
                s -= l[i][k] * l[j][k];
            }
            if i == j {
                if s <= 0.0 {
                    return None;
                }
                l[i][j] = s.sqrt();
            } else {
                l[i][j] = s / l[j][j];
            }
        }
    }
    Some(l)
}

/// One standard normal via Box-Muller.
fn standard_normal(rng: &mut ChaCha8Rng) -> f64 {
    let u1: f64 = rng.r#gen::<f64>().max(1e-12);
    let u2: f64 = rng.r#gen();
    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
}

/// Error function approximation (Abramowitz & Stegun 7.1.26, |err| < 1.5e-7).
fn erf(x: f64) -> f64 {
    let sign = x.signum();
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * x);
    let y = 1.0
        - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t
            + 0.254829592)
            * t
            * (-x * x).exp();
    sign * y
}

/// Standard normal CDF Φ.
pub fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

/// Generates `rows` vectors of k correlated uniforms from a Cholesky factor.
pub fn correlated_uniforms(chol: &[Vec<f64>], rows: usize, rng: &mut ChaCha8Rng) -> Vec<Vec<f64>> {
    let k = chol.len();
    let mut out = Vec::with_capacity(rows);
    for _ in 0..rows {
        let z: Vec<f64> = (0..k).map(|_| standard_normal(rng)).collect();
        let mut row = Vec::with_capacity(k);
        for (i, l_row) in chol.iter().enumerate() {
            let mut s = 0.0;
            for j in 0..=i {
                s += l_row[j] * z[j];
            }
            row.push(normal_cdf(s));
        }
        out.push(row);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::synth::samplers::stream_rng;

    #[test]
    fn normal_cdf_known_values() {
        assert!((normal_cdf(0.0) - 0.5).abs() < 1e-6);
        assert!((normal_cdf(1.96) - 0.975).abs() < 1e-3);
        assert!((normal_cdf(-1.96) - 0.025).abs() < 1e-3);
    }

    #[test]
    #[allow(clippy::needless_range_loop)]
    fn psd_repair_fixes_invalid_matrix() {
        // This matrix is NOT positive semi-definite (eigenvalue < 0).
        let mut m = vec![
            vec![1.0, 0.9, 0.9],
            vec![0.9, 1.0, -0.9],
            vec![0.9, -0.9, 1.0],
        ];
        psd_repair(&mut m);
        // After repair: symmetric, unit diagonal, Cholesky succeeds.
        for i in 0..3 {
            assert!((m[i][i] - 1.0).abs() < 1e-9);
            for j in 0..3 {
                assert!((m[i][j] - m[j][i]).abs() < 1e-9);
            }
        }
        assert!(cholesky(&m).is_some(), "repaired matrix must factor");
    }

    #[test]
    fn cholesky_of_identity_is_identity() {
        let m = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        let l = cholesky(&m).expect("identity factors");
        assert!((l[0][0] - 1.0).abs() < 1e-9);
        assert!((l[1][0]).abs() < 1e-9);
        assert!((l[1][1] - 1.0).abs() < 1e-9);
    }

    #[test]
    fn correlated_uniforms_reproduce_target_correlation() {
        let mut m = vec![vec![1.0, 0.8], vec![0.8, 1.0]];
        psd_repair(&mut m);
        let l = cholesky(&m).expect("factor");
        let mut rng = stream_rng(5, "t", "__copula__", 0);
        let rows = correlated_uniforms(&l, 5000, &mut rng);
        assert_eq!(rows.len(), 5000);
        assert_eq!(rows[0].len(), 2);
        for row in &rows {
            for u in row {
                assert!((0.0..=1.0).contains(u));
            }
        }
        // Spearman of uniforms ≈ rank correlation of the Gaussian copula:
        // 6/π·asin(ρ/2) ≈ 0.786 for ρ=0.8. Accept a generous band.
        let xs: Vec<f64> = rows.iter().map(|r| r[0]).collect();
        let ys: Vec<f64> = rows.iter().map(|r| r[1]).collect();
        let r = sample_pearson(&xs, &ys);
        assert!((0.68..=0.88).contains(&r), "got correlation {r}");
    }

    fn sample_pearson(xs: &[f64], ys: &[f64]) -> f64 {
        let n = xs.len() as f64;
        let mx = xs.iter().sum::<f64>() / n;
        let my = ys.iter().sum::<f64>() / n;
        let (mut c, mut vx, mut vy) = (0.0, 0.0, 0.0);
        for (x, y) in xs.iter().zip(ys) {
            c += (x - mx) * (y - my);
            vx += (x - mx) * (x - mx);
            vy += (y - my) * (y - my);
        }
        c / (vx.sqrt() * vy.sqrt())
    }
}

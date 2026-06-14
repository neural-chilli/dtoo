//! Synthetic data generation from statistical profiles (spec 35).

pub mod copula;
pub mod engine;
pub mod keys;
pub mod profile_input;
pub mod rules;
pub mod samplers;
pub mod spec;

pub use engine::run;

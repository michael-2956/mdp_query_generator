use std::{path::PathBuf, str::FromStr};

use equivalence_testing::rl_env::environment::ConstraintMeetingEnvironment;
use pyo3::prelude::*;

pub mod query_constraint_wrapper;

/// A simple environment struct, storing some internal 'state'.
#[pyclass]
pub struct ConstraintMeetingEnvironmentWrapper {
    env: ConstraintMeetingEnvironment,
}

#[pymethods]
impl ConstraintMeetingEnvironmentWrapper {
    /// Constructor visible to Python.  e.g. `env = my_rust_env.ConstraintMeetingEnvironmentWrapper(0)`
    #[new]
    pub fn new(config_path: &str) -> Self {
        // let inner_constraint = query_constraint.into_inner();
        ConstraintMeetingEnvironmentWrapper {
            env: ConstraintMeetingEnvironment::new(
                // inner_constraint,
                PathBuf::from_str(config_path).unwrap()
            ),
        }
    }

    /// Reset the environment Returns the initial observation \
    /// and previous generated query if such exists
    pub fn reset(&mut self) -> PyResult<(Vec<bool>, Option<String>)> {
        Ok(self.env.reset())
    }

    /// Step the environment. Returns `(mask, anticall_reward, terminated)`.
    pub fn step(&mut self, action: Option<usize>) -> PyResult<(Option<Vec<bool>>, f32, bool)> {
        Ok(self.env.step(action))
    }

    pub fn pop_query(&mut self) -> PyResult<Option<String>> {
        Ok(self.env.pop_query())
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn pyo3_env_wrapper(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ConstraintMeetingEnvironmentWrapper>()?;
    // m.add_class::<ConstraintTypeWrapper>()?;
    // m.add_class::<ConstraintMetricWrapper>()?;
    // m.add_class::<QueryConstraintWrapper>()?;
    Ok(())
}

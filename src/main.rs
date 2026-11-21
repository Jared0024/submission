mod input;
mod names;
mod output;
mod partition;
mod report;

use std::{env, path::PathBuf, thread};

use anyhow::{Context, Result, ensure};
use input::{Dimensions, Input, WEEK_MS};
use partition::{Sale, Stamp};
use std::path::Path;

fn main() -> Result<()> {
    let data = PathBuf::from(env::var_os("DATA_DIR").unwrap_or_else(|| "/data".into()));
    let output = PathBuf::from(env::var_os("OUTPUT_DIR").unwrap_or_else(|| "/output".into()));
    let workers = match env::var("NCPU") {
        Ok(value) => value
            .parse::<usize>()
            .context("NCPU must be a positive integer")?,
        Err(env::VarError::NotPresent) => thread::available_parallelism().map_or(1, usize::from),
        Err(error) => return Err(error.into()),
    };
    ensure!(workers > 0, "NCPU must be a positive integer");
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()?;
    // At most a quarter of the advertised memory for shared read-only event maps.
    // Without a memory hint, parse and release one range at a time.
    let mapping_budget = env::var("MEM_GB")
        .ok()
        .and_then(|gb| gb.parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_mul(1 << 28);
    let input = Input::open(&data, mapping_budget)?;
    let dimensions = input.dimensions();
    let base = input.start.saturating_sub(WEEK_MS);
    pool.install(|| {
        if i128::from(input.end) - i128::from(base) <= i128::from(u32::MAX) + 1 {
            run_window::<u32>(&input, &dimensions, workers, base, &output)
        } else {
            run_window::<i64>(&input, &dimensions, workers, base, &output)
        }
    })
}

fn run_window<T: Stamp>(
    input: &Input,
    dimensions: &Dimensions<'_>,
    workers: usize,
    base: i64,
    output: &Path,
) -> Result<()> {
    if dimensions.categories.len() <= 256 {
        run::<T, u32>(input, dimensions, workers, base, output)
    } else {
        run::<T, u64>(input, dimensions, workers, base, output)
    }
}

fn run<T: Stamp, S: Sale>(
    input: &Input,
    dimensions: &Dimensions<'_>,
    workers: usize,
    base: i64,
    output: &Path,
) -> Result<()> {
    let (partitions, people) = partition::partition::<T, S>(input, dimensions, workers, base)?;
    let (users, campaigns) = report::build(
        partitions,
        dimensions,
        people.len(),
        base,
        input.start..input.end,
    );
    output::write_all(
        output,
        users,
        campaigns,
        &people,
        &dimensions.campaign_names,
    )
}

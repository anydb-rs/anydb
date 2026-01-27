use std::{fs, time::Duration};

use anyhow::Result;

mod bytesvec_impl;
mod database;
mod fjall_impl;
mod lmdb_impl;
mod pcovec_impl;
mod redb_impl;
mod runner;

use bytesvec_impl::*;
use database::DatabaseBenchmark;
use fjall_impl::*;
use lmdb_impl::*;
use pcovec_impl::*;
use redb_impl::*;
use runner::*;
pub use runner::{BenchConfig, Database};

struct AccumulatedTimes {
    open: Vec<Duration>,
    linear: Vec<Duration>,
    random: Vec<Duration>,
    random_rayon: Vec<Duration>,
}

impl AccumulatedTimes {
    fn new() -> Self {
        Self {
            open: Vec::new(),
            linear: Vec::new(),
            random: Vec::new(),
            random_rayon: Vec::new(),
        }
    }

    fn to_result(
        name: String,
        write_time: Duration,
        times: &AccumulatedTimes,
        disk_size: u64,
        config: BenchConfig,
        run_index: usize,
    ) -> BenchmarkResult {
        BenchmarkResult {
            name,
            open_time: avg(&times.open),
            write_time,
            linear_read_time: avg(&times.linear),
            random_read_time: avg(&times.random),
            random_read_rayon: avg(&times.random_rayon),
            disk_size,
            config,
            run_index,
        }
    }
}

trait DatabaseBenchmarkTrait {
    fn name(&self) -> &str;
    fn run_open(&mut self, runner: &BenchmarkRunner) -> Result<Duration>;
    fn run_read_sequential(&mut self, runner: &BenchmarkRunner) -> Result<Duration>;
    fn run_read_random(&mut self, runner: &BenchmarkRunner, indices: &[u64]) -> Result<Duration>;
    fn run_read_random_rayon(
        &mut self,
        runner: &BenchmarkRunner,
        indices: &[u64],
    ) -> Result<Duration>;
    fn push_open(&mut self, duration: Duration);
    fn push_linear(&mut self, duration: Duration);
    fn push_random(&mut self, duration: Duration);
    fn push_random_rayon(&mut self, duration: Duration);
    fn to_result(&self, runner: &BenchmarkRunner, run_index: usize) -> Result<BenchmarkResult>;
    fn cleanup(&self, runner: &BenchmarkRunner) -> Result<()>;
}

struct DbBenchmark<DB: DatabaseBenchmark> {
    write_time: Duration,
    times: AccumulatedTimes,
    _phantom: std::marker::PhantomData<DB>,
}

impl<DB: DatabaseBenchmark> DbBenchmark<DB> {
    fn new(runner: &BenchmarkRunner) -> Result<Self> {
        let write_time = runner.prepare_database::<DB>()?;
        Ok(Self {
            write_time,
            times: AccumulatedTimes::new(),
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<DB: DatabaseBenchmark + 'static> DatabaseBenchmarkTrait for DbBenchmark<DB> {
    fn name(&self) -> &str {
        DB::name()
    }

    fn run_open(&mut self, runner: &BenchmarkRunner) -> Result<Duration> {
        let name = DB::name();
        let path = runner.db_path(name);
        let start = std::time::Instant::now();
        let db = DB::open(&path)?;
        let duration = start.elapsed();
        drop(db);
        Ok(duration)
    }

    fn run_read_sequential(&mut self, runner: &BenchmarkRunner) -> Result<Duration> {
        let name = DB::name();
        let path = runner.db_path(name);
        let db = DB::open(&path)?;

        let start = std::time::Instant::now();
        let _sum = db.read_sequential()?;
        let duration = start.elapsed();

        drop(db);
        Ok(duration)
    }

    fn run_read_random(&mut self, runner: &BenchmarkRunner, indices: &[u64]) -> Result<Duration> {
        let name = DB::name();
        let path = runner.db_path(name);
        let db = DB::open(&path)?;

        let start = std::time::Instant::now();
        let _sum = db.read_random(indices)?;
        let duration = start.elapsed();

        drop(db);
        Ok(duration)
    }

    fn run_read_random_rayon(
        &mut self,
        runner: &BenchmarkRunner,
        indices: &[u64],
    ) -> Result<Duration> {
        let name = DB::name();
        let path = runner.db_path(name);
        let db = DB::open(&path)?;

        let start = std::time::Instant::now();
        let _sum = db.read_random_rayon(indices)?;
        let duration = start.elapsed();

        drop(db);
        Ok(duration)
    }

    fn to_result(&self, runner: &BenchmarkRunner, run_index: usize) -> Result<BenchmarkResult> {
        let disk_size = runner.measure_disk_size::<DB>()?;
        Ok(AccumulatedTimes::to_result(
            DB::name().to_string(),
            self.write_time,
            &self.times,
            disk_size,
            runner.config().clone(),
            run_index,
        ))
    }

    fn push_open(&mut self, duration: Duration) {
        self.times.open.push(duration);
    }

    fn push_linear(&mut self, duration: Duration) {
        self.times.linear.push(duration);
    }

    fn push_random(&mut self, duration: Duration) {
        self.times.random.push(duration);
    }

    fn push_random_rayon(&mut self, duration: Duration) {
        self.times.random_rayon.push(duration);
    }

    fn cleanup(&self, runner: &BenchmarkRunner) -> Result<()> {
        runner.cleanup::<DB>()
    }
}

pub fn run(configs: &[BenchConfig]) -> Result<()> {
    println!("VecDB Benchmark Suite");

    // Create bench_data in the crate directory (where Cargo.toml is)
    let crate_dir = env!("CARGO_MANIFEST_DIR");
    let base_path = std::path::PathBuf::from(crate_dir).join("bench_data");

    if base_path.exists() {
        std::fs::remove_dir_all(&base_path)?;
    }
    std::fs::create_dir_all(&base_path)?;

    let mut all_results = Vec::new();

    for (config_idx, config) in configs.iter().enumerate() {
        println!("\n=== Running Benchmark {} ===", config_idx + 1);
        println!(
            "Config: {} writes, {}% random reads, {} iterations",
            config.write_count,
            config.random_read_percent * 100.0,
            config.num_iterations
        );

        let runner = BenchmarkRunner::new(&base_path, config.clone());

        // Generate random indices (same for all databases and iterations)
        let indices = runner.generate_random_indices();

        // Phase 1: Prepare all databases (write data)
        println!("\nPreparing databases:");
        let mut db_benchmarks: Vec<Box<dyn DatabaseBenchmarkTrait>> = Vec::new();

        for db in &config.databases {
            match db {
                Database::PcoVec => {
                    db_benchmarks.push(Box::new(DbBenchmark::<PcoVecBench>::new(&runner)?));
                }
                Database::BytesVec => {
                    db_benchmarks.push(Box::new(DbBenchmark::<BytesVecBench>::new(&runner)?));
                }
                Database::Fjall => {
                    db_benchmarks.push(Box::new(DbBenchmark::<FjallBench>::new(&runner)?));
                }
                Database::Redb => {
                    db_benchmarks.push(Box::new(DbBenchmark::<RedbBench>::new(&runner)?));
                }
                Database::Lmdb => {
                    db_benchmarks.push(Box::new(DbBenchmark::<LmdbBench>::new(&runner)?));
                }
            }
        }

        // Helper to run a test across all databases
        let run_test =
            |name: &str,
             benchmarks: &mut [Box<dyn DatabaseBenchmarkTrait>],
             run_fn: &dyn Fn(&mut Box<dyn DatabaseBenchmarkTrait>) -> Result<Duration>|
             -> Result<()> {
                println!("    {}", name);
                for db_bench in benchmarks.iter_mut() {
                    print!("      {}... ", db_bench.name());
                    std::io::Write::flush(&mut std::io::stdout()).ok();
                    let duration = run_fn(db_bench)?;
                    println!("{duration:?}");
                }
                Ok(())
            };

        // Phase 2: Run interleaved iterations
        println!("\nRunning iterations:");
        for i in 1..=config.num_iterations {
            println!("  Iteration {}/{}:", i, config.num_iterations);

            run_test("open()", &mut db_benchmarks, &|db| {
                let duration = db.run_open(&runner)?;
                db.push_open(duration);
                Ok(duration)
            })?;

            run_test("linear", &mut db_benchmarks, &|db| {
                let duration = db.run_read_sequential(&runner)?;
                db.push_linear(duration);
                Ok(duration)
            })?;

            run_test("read_rand", &mut db_benchmarks, &|db| {
                let duration = db.run_read_random(&runner, &indices)?;
                db.push_random(duration);
                Ok(duration)
            })?;

            run_test("read_rand(rayon)", &mut db_benchmarks, &|db| {
                let duration = db.run_read_random_rayon(&runner, &indices)?;
                db.push_random_rayon(duration);
                Ok(duration)
            })?;
        }

        // Phase 3: Build results and cleanup
        for db_bench in db_benchmarks {
            let result = db_bench.to_result(&runner, config_idx)?;
            db_bench.cleanup(&runner)?;
            all_results.push(result);
        }

        // Cleanup after each benchmark config
        println!("\nCleaning up benchmark data...");
        if base_path.exists() {
            fs::remove_dir_all(&base_path)?;
            fs::create_dir_all(&base_path)?;
        }
    }

    // Print summary
    println!();
    BenchmarkRunner::print_summary(&all_results);

    // Write README
    println!();
    BenchmarkRunner::write_readme(&all_results)?;
    println!("README.md updated!");

    // Cleanup
    if base_path.exists() {
        std::fs::remove_dir_all(base_path)?;
    }

    Ok(())
}

fn avg(durations: &[Duration]) -> Duration {
    if durations.is_empty() {
        return Duration::from_secs(0);
    }
    let total: Duration = durations.iter().sum();
    total / durations.len() as u32
}

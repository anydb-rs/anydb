use crate::database::DatabaseBenchmark;
use anyhow::Result;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub const WRITE_COUNT: u64 = 10_000_000;
pub const RANDOM_READ_PERCENT: f64 = 0.01; // 1% of writes
pub const RANDOM_SEED: u64 = 42;
pub const NUM_ITERATIONS: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Database {
    PcoVec,
    BytesVec,
    Fjall2,
    Fjall3,
    Redb,
    Lmdb,
}

impl Database {
    pub fn all() -> Vec<Database> {
        vec![
            Database::PcoVec,
            Database::BytesVec,
            Database::Fjall3,
            Database::Fjall2,
            Database::Redb,
            Database::Lmdb,
        ]
    }
}

#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub write_count: u64,
    pub random_read_percent: f64,
    pub random_seed: u64,
    pub num_iterations: usize,
    pub databases: Vec<Database>,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            write_count: WRITE_COUNT,
            random_read_percent: RANDOM_READ_PERCENT,
            random_seed: RANDOM_SEED,
            num_iterations: NUM_ITERATIONS,
            databases: Database::all(),
        }
    }
}

impl BenchConfig {
    pub fn random_read_count(&self) -> usize {
        (self.write_count as f64 * self.random_read_percent) as usize
    }
}

pub struct BenchmarkResult {
    pub name: String,
    pub open_time: Duration,
    pub write_time: Duration,
    pub linear_read_time: Duration,
    pub random_read_time: Duration,
    pub random_read_rayon: Duration,
    pub disk_size: u64,
    pub config: BenchConfig,
    pub run_index: usize,
}

impl BenchmarkResult {
    fn format_duration(d: Duration) -> String {
        let secs = d.as_secs_f64();
        if secs < 1.0 {
            format!("{:.2} ms", secs * 1000.0)
        } else if secs < 60.0 {
            format!("{:.2} s", secs)
        } else {
            let mins = (secs / 60.0).floor();
            let secs = secs % 60.0;
            format!("{:.0}m {:.2}s", mins, secs)
        }
    }

    fn format_size(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;

        if bytes >= GB {
            format!("{:.2} GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.2} MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.2} KB", bytes as f64 / KB as f64)
        } else {
            format!("{} B", bytes)
        }
    }

    fn format_throughput(ops: u64, duration: Duration) -> String {
        let secs = duration.as_secs_f64();
        if secs < 0.000001 {
            return String::from("N/A");
        }
        let ops_per_sec = ops as f64 / secs;
        if ops_per_sec >= 1_000_000.0 {
            let val = ops_per_sec / 1_000_000.0;
            format!("{} Mo/s", Self::format_compact(val))
        } else if ops_per_sec >= 1_000.0 {
            let val = ops_per_sec / 1_000.0;
            format!("{} Ko/s", Self::format_compact(val))
        } else {
            format!("{} o/s", Self::format_compact(ops_per_sec))
        }
    }

    fn format_bandwidth(bytes: u64, duration: Duration) -> String {
        let secs = duration.as_secs_f64();
        if secs < 0.000001 {
            return String::from("N/A");
        }
        let bytes_per_sec = bytes as f64 / secs;
        let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
        if mb_per_sec >= 1024.0 {
            format!("{} GB/s", Self::format_compact(mb_per_sec / 1024.0))
        } else {
            format!("{} MB/s", Self::format_compact(mb_per_sec))
        }
    }

    fn format_latency(ops: u64, duration: Duration) -> String {
        let secs = duration.as_secs_f64();
        if secs < 0.000001 {
            return String::from("N/A");
        }
        let latency_secs = secs / ops as f64;
        let latency_ns = latency_secs * 1_000_000_000.0;
        if latency_ns >= 1_000_000.0 {
            format!("{} ms", Self::format_compact(latency_ns / 1_000_000.0))
        } else if latency_ns >= 1_000.0 {
            format!("{} µs", Self::format_compact(latency_ns / 1_000.0))
        } else {
            format!("{} ns", Self::format_compact(latency_ns))
        }
    }

    fn format_compact(val: f64) -> String {
        if val >= 1000.0 {
            // For values >= 1000, show no decimals
            format!("{:.0}", val)
        } else if val >= 100.0 {
            // For 100-999, show no decimals
            format!("{:.1}", val)
        } else if val >= 10.0 {
            // For 10-99, show 1 decimal (e.g., 20.4)
            format!("{:.2}", val)
        } else {
            // For < 10, show 2 decimals (e.g., 9.99)
            format!("{:.3}", val)
        }
    }
}

pub struct BenchmarkRunner {
    base_path: PathBuf,
    config: BenchConfig,
}

impl BenchmarkRunner {
    pub fn new<P: AsRef<Path>>(base_path: P, config: BenchConfig) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
            config,
        }
    }

    pub fn config(&self) -> &BenchConfig {
        &self.config
    }

    pub fn db_path(&self, db_name: &str) -> PathBuf {
        self.base_path.join(db_name)
    }

    fn clean_path(&self, db_name: &str) -> Result<()> {
        let path = self.db_path(db_name);
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
        }
        std::fs::create_dir_all(&path)?;
        Ok(())
    }

    pub fn generate_random_indices(&self) -> Vec<u64> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(self.config.random_seed);
        let count = self.config.random_read_count();
        (0..count)
            .map(|_| rng.random_range(0..self.config.write_count))
            .collect()
    }

    pub fn prepare_database<DB: DatabaseBenchmark>(&self) -> Result<Duration> {
        let name = DB::name();
        print!("  {} ... ", name);

        // Clean and prepare
        self.clean_path(name)?;
        let path = self.db_path(name);

        // Create and write
        let mut db = DB::create(&path)?;

        let start = Instant::now();
        db.write_sequential(self.config.write_count)?;
        let write_time = start.elapsed();

        // Flush
        db.flush()?;

        drop(db);

        println!("{write_time:?}");

        Ok(write_time)
    }

    pub fn measure_disk_size<DB: DatabaseBenchmark>(&self) -> Result<u64> {
        let name = DB::name();
        let path = self.db_path(name);
        DB::disk_size(&path)
    }

    pub fn cleanup<DB: DatabaseBenchmark>(&self) -> Result<()> {
        let name = DB::name();
        self.clean_path(name)
    }

    pub fn print_summary(results: &[BenchmarkResult]) {
        println!("\nRESULTS\n");

        for result in results {
            let write_bytes = result.config.write_count * 8;
            let random_count = result.config.random_read_count();
            let random_bytes = random_count as u64 * 8;
            println!("{}", result.name);
            println!(
                "  Open:        {}",
                BenchmarkResult::format_duration(result.open_time)
            );
            println!("  Write:");
            println!(
                "    {}",
                BenchmarkResult::format_throughput(result.config.write_count, result.write_time)
            );
            println!(
                "    {}",
                BenchmarkResult::format_bandwidth(write_bytes, result.write_time)
            );
            println!(
                "    {}",
                BenchmarkResult::format_latency(result.config.write_count, result.write_time)
            );
            println!("  Linear:");
            println!(
                "    {}",
                BenchmarkResult::format_throughput(
                    result.config.write_count,
                    result.linear_read_time
                )
            );
            println!(
                "    {}",
                BenchmarkResult::format_bandwidth(write_bytes, result.linear_read_time)
            );
            println!(
                "    {}",
                BenchmarkResult::format_latency(result.config.write_count, result.linear_read_time)
            );
            println!("  Random:");
            println!(
                "    {}",
                BenchmarkResult::format_throughput(random_count as u64, result.random_read_time)
            );
            println!(
                "    {}",
                BenchmarkResult::format_bandwidth(random_bytes, result.random_read_time)
            );
            println!(
                "    {}",
                BenchmarkResult::format_latency(random_count as u64, result.random_read_time)
            );
            println!("  Random Rayon:");
            println!(
                "    {}",
                BenchmarkResult::format_throughput(random_count as u64, result.random_read_rayon)
            );
            println!(
                "    {}",
                BenchmarkResult::format_bandwidth(random_bytes, result.random_read_rayon)
            );
            println!(
                "    {}",
                BenchmarkResult::format_latency(random_count as u64, result.random_read_rayon)
            );
            println!(
                "  Disk Size:     {}",
                BenchmarkResult::format_size(result.disk_size)
            );
            println!();
        }
    }

    pub fn write_readme(results: &[BenchmarkResult]) -> Result<()> {
        // Write README.md to the crate directory (where Cargo.toml is)
        let crate_dir = env!("CARGO_MANIFEST_DIR");
        let readme_path = PathBuf::from(crate_dir).join("README.md");

        println!("Writing to: {}", readme_path.display());
        let mut file = std::fs::File::create(&readme_path)?;

        writeln!(file, "# VecDB Benchmark")?;
        writeln!(file)?;
        writeln!(
            file,
            "Benchmark comparing vecdb against popular embedded databases: fjall, redb, and lmdb."
        )?;
        writeln!(file)?;

        // Group results by run_index
        let mut run_map: HashMap<usize, Vec<&BenchmarkResult>> = HashMap::new();

        for result in results {
            run_map.entry(result.run_index).or_default().push(result);
        }

        // Sort by run_index for consistent ordering
        let mut run_groups: Vec<_> = run_map.into_iter().collect();
        run_groups.sort_by_key(|(run_idx, _)| *run_idx);

        for (idx, (_, group_results)) in run_groups.iter().enumerate() {
            let config = &group_results[0].config;

            if idx > 0 {
                writeln!(file, "---")?;
                writeln!(file)?;
            }

            writeln!(file, "## Benchmark {}", idx + 1)?;
            writeln!(file)?;
            writeln!(
                file,
                "**Test**: {} million sequential u64 writes, linear reads, and {}% random reads.",
                config.write_count / 1_000_000,
                config.random_read_percent * 100.0
            )?;
            writeln!(file)?;
            writeln!(
                file,
                "**Iterations**: {} pass{}",
                config.num_iterations,
                if config.num_iterations > 1 { "es" } else { "" }
            )?;
            writeln!(file)?;

            writeln!(file, "### Results")?;
            writeln!(file)?;

            Self::write_results_table(&mut file, group_results, config)?;
        }

        writeln!(file)?;
        writeln!(file, "## Run")?;
        writeln!(file)?;
        writeln!(file, "```bash")?;
        writeln!(file, "cargo run --release --bin vecdb_bench")?;
        writeln!(file, "```")?;

        Ok(())
    }

    fn write_results_table(
        file: &mut std::fs::File,
        results: &[&BenchmarkResult],
        config: &BenchConfig,
    ) -> Result<()> {
        // Find best values (min for time/size, max for throughput)
        let best_open = results.iter().map(|r| r.open_time).min().unwrap();
        let best_write = results.iter().map(|r| r.write_time).min().unwrap();
        let best_linear = results.iter().map(|r| r.linear_read_time).min().unwrap();
        let best_random = results.iter().map(|r| r.random_read_time).min().unwrap();
        let best_random_rayon = results.iter().map(|r| r.random_read_rayon).min().unwrap();
        let best_disk = results.iter().map(|r| r.disk_size).min().unwrap();

        let write_bytes = config.write_count * 8;
        let random_count = config.random_read_count();
        let random_bytes = random_count as u64 * 8;

        // Results table (transposed: metrics as rows, databases as columns)
        // Header row
        write!(file, "| Metric |")?;
        for result in results {
            write!(file, " {} |", result.name)?;
        }
        writeln!(file)?;

        // Separator row
        write!(file, "|--------|")?;
        for _ in results {
            write!(file, "--------|")?;
        }
        writeln!(file)?;

        // Open row
        write!(file, "| **Open** |")?;
        for result in results {
            let open_str = BenchmarkResult::format_duration(result.open_time);
            if result.open_time == best_open {
                write!(file, " **{}** |", open_str)?;
            } else {
                write!(file, " {} |", open_str)?;
            }
        }
        writeln!(file)?;

        // Write row
        write!(file, "| **Write** |")?;
        for result in results {
            let info = format!(
                "{}<br>{}<br>{}<br>{}",
                BenchmarkResult::format_duration(result.write_time),
                BenchmarkResult::format_throughput(config.write_count, result.write_time),
                BenchmarkResult::format_bandwidth(write_bytes, result.write_time),
                BenchmarkResult::format_latency(config.write_count, result.write_time)
            );
            if result.write_time == best_write {
                write!(file, " **{}** |", info)?;
            } else {
                write!(file, " {} |", info)?;
            }
        }
        writeln!(file)?;

        // Linear Read row
        write!(file, "| **Linear** |")?;
        for result in results {
            let info = format!(
                "{}<br>{}<br>{}<br>{}",
                BenchmarkResult::format_duration(result.linear_read_time),
                BenchmarkResult::format_throughput(config.write_count, result.linear_read_time),
                BenchmarkResult::format_bandwidth(write_bytes, result.linear_read_time),
                BenchmarkResult::format_latency(config.write_count, result.linear_read_time)
            );
            if result.linear_read_time == best_linear {
                write!(file, " **{}** |", info)?;
            } else {
                write!(file, " {} |", info)?;
            }
        }
        writeln!(file)?;

        // Random Read row
        write!(file, "| **Random** |")?;
        for result in results {
            let info = format!(
                "{}<br>{}<br>{}<br>{}",
                BenchmarkResult::format_duration(result.random_read_time),
                BenchmarkResult::format_throughput(random_count as u64, result.random_read_time),
                BenchmarkResult::format_bandwidth(random_bytes, result.random_read_time),
                BenchmarkResult::format_latency(random_count as u64, result.random_read_time)
            );
            if result.random_read_time == best_random {
                write!(file, " **{}** |", info)?;
            } else {
                write!(file, " {} |", info)?;
            }
        }
        writeln!(file)?;

        // Random Read Rayon row
        write!(file, "| **Random Rayon** |")?;
        for result in results {
            let info = format!(
                "{}<br>{}<br>{}<br>{}",
                BenchmarkResult::format_duration(result.random_read_rayon),
                BenchmarkResult::format_throughput(random_count as u64, result.random_read_rayon),
                BenchmarkResult::format_bandwidth(random_bytes, result.random_read_rayon),
                BenchmarkResult::format_latency(random_count as u64, result.random_read_rayon)
            );
            if result.random_read_rayon == best_random_rayon {
                write!(file, " **{}** |", info)?;
            } else {
                write!(file, " {} |", info)?;
            }
        }
        writeln!(file)?;

        // Disk Size row
        write!(file, "| **Disk Size** |")?;
        for result in results {
            let disk_str = BenchmarkResult::format_size(result.disk_size);
            if result.disk_size == best_disk {
                write!(file, " **{}** |", disk_str)?;
            } else {
                write!(file, " {} |", disk_str)?;
            }
        }
        writeln!(file)?;

        Ok(())
    }
}

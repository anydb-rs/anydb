use std::time::{Duration, Instant};

use vecdb::{
    BytesVec, Database, GenericStoredVec, ImportableVec, PcoVec, ScannableVec, Version,
};

const DEFAULT_VALUE_COUNT: usize = 10_000_000_000; // 10B u64s = 80 GB
const BATCH_SIZE: usize = 10_000_000;

fn value_count() -> usize {
    std::env::var("BENCH_COUNT")
        .ok()
        .and_then(|s| s.replace('_', "").parse().ok())
        .unwrap_or(DEFAULT_VALUE_COUNT)
}

fn range_sizes(count: usize) -> Vec<usize> {
    [
        1_000,
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        50_000_000,
        100_000_000,
        500_000_000,
        1_000_000_000,
    ]
    .into_iter()
    .filter(|&r| r <= count)
    .collect()
}

fn repetitions(range_size: usize) -> usize {
    match range_size {
        n if n < 10_000 => 5_000,
        n if n < 100_000 => 1_000,
        n if n < 1_000_000 => 100,
        n if n < 10_000_000 => 20,
        _ => 3,
    }
}

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn random_starts(count: usize, max_start: usize) -> Vec<usize> {
    let mut rng = 42u64;
    (0..count)
        .map(|_| xorshift(&mut rng) as usize % max_start.max(1))
        .collect()
}

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.1} GB", bytes as f64 / 1e9)
    } else if bytes >= 1_000_000 {
        format!("{:.1} MB", bytes as f64 / 1e6)
    } else if bytes >= 1_000 {
        format!("{:.1} KB", bytes as f64 / 1e3)
    } else {
        format!("{} B", bytes)
    }
}

fn format_duration(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns >= 1_000_000_000 {
        format!("{:.2} s", d.as_secs_f64())
    } else if ns >= 1_000_000 {
        format!("{:.2} ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.2} us", ns as f64 / 1e3)
    } else {
        format!("{} ns", ns)
    }
}

// --- Page cache eviction ---

/// Try to drop the OS page cache. Returns true if successful.
/// macOS: requires `purge` (needs root). Linux: writes to drop_caches (needs root).
fn drop_caches() -> bool {
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("purge")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    }
    #[cfg(target_os = "linux")]
    {
        // sync first, then drop caches
        std::process::Command::new("sync").status().ok();
        std::fs::write("/proc/sys/vm/drop_caches", "3").is_ok()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        false
    }
}

/// Check once whether cache eviction works and print status.
fn check_cache_eviction() -> bool {
    let ok = drop_caches();
    if ok {
        eprintln!("  Cache eviction: available (cold-cache benchmarks)");
    } else {
        eprintln!("  Cache eviction: unavailable (warm-cache benchmarks)");
        eprintln!("  Hint: run with sudo for cold-cache results");
    }
    ok
}

// --- Populate ---

fn populate<V: GenericStoredVec<usize, u64> + ImportableVec>(
    db: &Database,
    label: &str,
    count: usize,
) -> V {
    eprint!("  Populating {label} with {count} values...");
    flush();
    let start = Instant::now();
    let mut vec: V = V::import(db, "bench", Version::ONE).unwrap();
    let mut written = 0;
    while written < count {
        let end = (written + BATCH_SIZE).min(count);
        for i in written..end {
            vec.push(i as u64);
        }
        vec.write().unwrap();
        written = end;
        eprint!(
            "\r  Populating {label}: {:.0}%  ",
            written as f64 / count as f64 * 100.0
        );
    }
    db.flush().unwrap();
    eprintln!("\r  Populated {label} ({:?})       ", start.elapsed());
    vec
}

// --- Generic fold benchmark ---

/// Benchmarks a fold operation over a set of random ranges.
/// `fold_fn(from, to, acc) -> acc` is called for each start in `starts`,
/// with range `[start, start + range_size)`.
/// Returns the average duration per range.
fn bench_fold_fn<F: FnMut(usize, usize, u64) -> u64>(
    range_size: usize,
    starts: &[usize],
    mut fold_fn: F,
) -> Duration {
    let reps = starts.len();
    let mut sum = 0u64;
    let start = Instant::now();
    for &s in starts {
        sum = fold_fn(s, s + range_size, sum);
    }
    let elapsed = start.elapsed();
    std::hint::black_box(sum);
    elapsed / reps as u32
}

/// Benchmarks a single fold over the full range `[0, count)`.
fn bench_full_fold<F: FnMut(usize, usize, u64) -> u64>(
    count: usize,
    mut fold_fn: F,
) -> Duration {
    let mut sum = 0u64;
    let start = Instant::now();
    sum = fold_fn(0, count, sum);
    let elapsed = start.elapsed();
    std::hint::black_box(sum);
    elapsed
}

// --- BytesVec benchmarks ---

fn bench_bytes_vec(vec: &BytesVec<usize, u64>, count: usize, can_purge: bool) {
    let total_bytes = count * 8;
    let ranges = range_sizes(count);

    println!(
        "\n=== BytesVec<usize, u64> — {} values ({}) ===\n",
        count,
        format_bytes(total_bytes),
    );

    // Full scan
    println!("--- Full scan ---");
    let throughput = |d: Duration| total_bytes as f64 / d.as_secs_f64() / 1e9;

    if can_purge {
        drop_caches();
    }
    let io = bench_full_fold(count, |from, to, acc| {
        vec.fold_stored_io(from, to, acc, |a, v: u64| a.wrapping_add(v))
    });
    println!(
        "  IO:   {} ({:.1} GB/s)",
        format_duration(io),
        throughput(io)
    );

    if can_purge {
        drop_caches();
    }
    let mmap = bench_full_fold(count, |from, to, acc| {
        vec.fold_stored_mmap(from, to, acc, |a, v: u64| a.wrapping_add(v))
    });
    println!(
        "  Mmap: {} ({:.1} GB/s)",
        format_duration(mmap),
        throughput(mmap)
    );

    if can_purge {
        drop_caches();
    }
    let auto = bench_full_fold(count, |from, to, acc| {
        vec.fold_range(from, to, acc, |a, v: u64| a.wrapping_add(v))
    });
    println!(
        "  Auto: {} ({:.1} GB/s)",
        format_duration(auto),
        throughput(auto)
    );

    // Range scans
    println!("\n--- Range scans ---");
    println!(
        "{:>12} {:>10} {:>14} {:>14} {:>14}  {:<8}",
        "range", "bytes", "IO", "Mmap", "Auto", "winner"
    );
    println!("{}", "-".repeat(82));

    for &range_size in &ranges {
        let reps = repetitions(range_size);
        let max_start = count.saturating_sub(range_size);
        let starts = random_starts(reps, max_start);

        if can_purge {
            drop_caches();
        }
        let io_per = bench_fold_fn(range_size, &starts, |from, to, acc| {
            vec.fold_stored_io(from, to, acc, |a, v: u64| a.wrapping_add(v))
        });

        if can_purge {
            drop_caches();
        }
        let mmap_per = bench_fold_fn(range_size, &starts, |from, to, acc| {
            vec.fold_stored_mmap(from, to, acc, |a, v: u64| a.wrapping_add(v))
        });

        if can_purge {
            drop_caches();
        }
        let auto_per = bench_fold_fn(range_size, &starts, |from, to, acc| {
            vec.fold_range(from, to, acc, |a, v: u64| a.wrapping_add(v))
        });

        let times = [("IO", io_per), ("Mmap", mmap_per), ("Auto", auto_per)];
        let winner = times.iter().min_by_key(|(_, d)| *d).unwrap().0;

        let range_bytes = range_size * 8;
        println!(
            "{:>12} {:>10} {:>14} {:>14} {:>14}  {:<8}",
            range_size,
            format_bytes(range_bytes),
            format_duration(io_per),
            format_duration(mmap_per),
            format_duration(auto_per),
            winner,
        );
    }
}

// --- PcoVec benchmarks ---

fn bench_pco_vec(vec: &PcoVec<usize, u64>, count: usize, can_purge: bool) {
    let total_bytes = count * 8;
    let ranges = range_sizes(count);

    println!(
        "\n=== PcoVec<usize, u64> — {} values ({}) ===\n",
        count,
        format_bytes(total_bytes),
    );

    // Full scan
    println!("--- Full scan ---");
    let throughput = |d: Duration| total_bytes as f64 / d.as_secs_f64() / 1e9;

    if can_purge {
        drop_caches();
    }
    let auto = bench_full_fold(count, |from, to, acc| {
        vec.fold_range(from, to, acc, |a, v: u64| a.wrapping_add(v))
    });
    println!(
        "  Auto: {} ({:.1} GB/s)",
        format_duration(auto),
        throughput(auto)
    );

    // Range scans
    println!("\n--- Range scans ---");
    println!(
        "{:>12} {:>10} {:>14}",
        "range", "bytes", "Auto"
    );
    println!("{}", "-".repeat(40));

    for &range_size in &ranges {
        let reps = repetitions(range_size);
        let max_start = count.saturating_sub(range_size);
        let starts = random_starts(reps, max_start);

        if can_purge {
            drop_caches();
        }
        let auto_per = bench_fold_fn(range_size, &starts, |from, to, acc| {
            vec.fold_range(from, to, acc, |a, v: u64| a.wrapping_add(v))
        });

        let range_bytes = range_size * 8;
        println!(
            "{:>12} {:>10} {:>14}",
            range_size,
            format_bytes(range_bytes),
            format_duration(auto_per),
        );
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("both");
    let count = value_count();

    println!("BENCH_COUNT={count} (set env to override, e.g. BENCH_COUNT=1_000_000_000)");
    let can_purge = check_cache_eviction();
    println!();

    match mode {
        "bytes" => {
            let dir = tempfile::tempdir().unwrap();
            let db = Database::open(dir.path()).unwrap();
            let vec = populate::<BytesVec<usize, u64>>(&db, "BytesVec", count);
            bench_bytes_vec(&vec, count, can_purge);
        }
        "pco" => {
            let dir = tempfile::tempdir().unwrap();
            let db = Database::open(dir.path()).unwrap();
            let vec = populate::<PcoVec<usize, u64>>(&db, "PcoVec", count);
            bench_pco_vec(&vec, count, can_purge);
        }
        "both" | _ => {
            {
                let dir = tempfile::tempdir().unwrap();
                let db = Database::open(dir.path()).unwrap();
                let vec = populate::<BytesVec<usize, u64>>(&db, "BytesVec", count);
                bench_bytes_vec(&vec, count, can_purge);
            }
            {
                let dir = tempfile::tempdir().unwrap();
                let db = Database::open(dir.path()).unwrap();
                let vec = populate::<PcoVec<usize, u64>>(&db, "PcoVec", count);
                bench_pco_vec(&vec, count, can_purge);
            }
        }
    }
}

fn flush() {
    std::io::Write::flush(&mut std::io::stderr()).ok();
}

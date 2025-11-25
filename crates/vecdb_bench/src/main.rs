use vecdb_bench::{BenchConfig, Database, run};

fn main() {
    let configs = vec![
        BenchConfig::default(),
        BenchConfig {
            write_count: 100_000_000,
            databases: vec![
                Database::PcoVec,
                Database::ZeroCopyVec,
                Database::Fjall3,
                Database::Fjall2,
                Database::Redb,
                Database::Lmdb,
            ],
            ..Default::default()
        },
        BenchConfig {
            write_count: 1_000_000_000,
            databases: vec![
                // Database::ZeroCopyVecOld,
                Database::ZeroCopyVec,
                Database::PcoVec,
            ],
            ..Default::default()
        },
    ];
    run(&configs).unwrap();
}

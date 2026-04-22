use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::io::sink;

fn bench_engine(c: &mut Criterion) {
    let samples: &[(&str, &[u8])] = &[
        (
            "sample_01",
            include_bytes!("../tests/data/sample_01_simple_deposits_and_withdrawals.csv"),
        ),
        (
            "sample_05",
            include_bytes!("../tests/data/sample_05_dispute_chargeback_account_locks.csv"),
        ),
    ];

    let mut group = c.benchmark_group("pecrab_engine");

    for &(name, csv) in samples {
        group.throughput(Throughput::Bytes(csv.len() as u64));
        group.bench_function(name, |b| {
            b.iter(|| {
                pecrab::run_with_writer(csv, sink()).expect("engine failed");
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_engine);
criterion_main!(benches);

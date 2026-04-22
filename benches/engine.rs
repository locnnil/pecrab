use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::io::sink;

fn bench_engine(c: &mut Criterion) {
    let csv = include_bytes!("../tests/data/sample_01_simple_deposits_and_withdrawals.csv");

    let mut group = c.benchmark_group("pecrab_engine");
    group.throughput(Throughput::Bytes(csv.len() as u64));

    group.bench_function("sample_01", |b| {
        b.iter(|| {
            pecrab::run_with_writer(csv.as_slice(), sink())
                .expect("Failed to run PEcrab on benchmark data");
        });
    });

    group.finish();
}

criterion_group!(benches, bench_engine);
criterion_main!(benches);

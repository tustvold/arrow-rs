// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[macro_use]
extern crate criterion;

use criterion::Criterion;
use std::ops::Range;

use rand::Rng;

extern crate arrow;

use arrow::datatypes::*;
use arrow::util::test_util::seedable_rng;
use arrow::{array::*, util::bench_util::*};
use arrow_select::interleave::interleave;

fn do_bench(
    c: &mut Criterion,
    prefix: &str,
    len: usize,
    base: &dyn Array,
    slices: &[Range<usize>],
) {
    let mut rng = seedable_rng();

    let arrays: Vec<_> = slices
        .iter()
        .map(|r| base.slice(r.start, r.end - r.start))
        .collect();
    let values: Vec<_> = arrays.iter().map(|x| x.as_ref()).collect();

    let indices: Vec<_> = (0..len)
        .map(|_| {
            let array_idx = rng.gen_range(0..values.len());
            let value_idx = rng.gen_range(0..values[array_idx].len());
            (array_idx, value_idx)
        })
        .collect();

    c.bench_function(
        &format!("interleave {} {} {:?}", prefix, len, slices),
        |b| b.iter(|| criterion::black_box(interleave(&values, &indices).unwrap())),
    );
}

fn add_benchmark(c: &mut Criterion) {
    let a = create_primitive_array::<Int32Type>(1024, 0.);

    do_bench(c, "i32(0.0)", 100, &a, &[0..100, 100..230, 450..1000]);
    do_bench(c, "i32(0.0)", 400, &a, &[0..100, 100..230, 450..1000]);
    do_bench(c, "i32(0.0)", 1024, &a, &[0..100, 100..230, 450..1000]);
    do_bench(
        c,
        "i32(0.0)",
        1024,
        &a,
        &[0..100, 100..230, 450..1000, 0..1000],
    );

    let a = create_primitive_array::<Int32Type>(1024, 0.5);

    do_bench(c, "i32(0.5)", 100, &a, &[0..100, 100..230, 450..1000]);
    do_bench(c, "i32(0.5)", 400, &a, &[0..100, 100..230, 450..1000]);
    do_bench(c, "i32(0.5)", 1024, &a, &[0..100, 100..230, 450..1000]);
    do_bench(
        c,
        "i32(0.5)",
        1024,
        &a,
        &[0..100, 100..230, 450..1000, 0..1000],
    );
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);

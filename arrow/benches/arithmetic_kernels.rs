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
use rand::Rng;

use std::sync::Arc;

extern crate arrow;

use arrow::util::bench_util::*;
use arrow::{array::*, datatypes::Float32Type};
use arrow::{compute::kernels::arithmetic::*, util::test_util::seedable_rng};

fn add_benchmark(c: &mut Criterion) {
    const BATCH_SIZE: usize = 64 * 1024;
    for density in [0., 0.1, 0.5, 0.9, 1.0] {
        let arr_a = create_primitive_array::<Float32Type>(BATCH_SIZE, null_density);
        let arr_b = create_primitive_array::<Float32Type>(BATCH_SIZE, null_density);
        let scalar = seedable_rng().gen();

        c.bench_function(&format!("add({})", density), |b| {
            b.iter(|| criterion::black_box(add(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("add_scalar({})", density), |b| {
            b.iter(|| criterion::black_box(add_scalar(&arr_a, scalar).unwrap()))
        });
        c.bench_function(&format!("subtract({})", density), |b| {
            b.iter(|| criterion::black_box(subtract(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("subtract_scalar({})", density), |b| {
            b.iter(|| criterion::black_box(subtract_scalar(&arr_a, scalar).unwrap()))
        });
        c.bench_function(&format!("multiply({})", density), |b| {
            b.iter(|| criterion::black_box(multiply(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("multiply_scalar({})", density), |b| {
            b.iter(|| criterion::black_box(multiply_scalar(&arr_a, scalar).unwrap()))
        });
        c.bench_function(&format!("divide({})", density), |b| {
            b.iter(|| criterion::black_box(divide(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("divide_checked({})", density), |b| {
            b.iter(|| criterion::black_box(divide_checked(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("divide_scalar({})", density), |b| {
            b.iter(|| criterion::black_box(divide_scalar(&arr_a, scalar).unwrap()))
        });
        c.bench_function(&format!("modulo({})", density), |b| {
            b.iter(|| criterion::black_box(modulus(&arr_a, &arr_b).unwrap()))
        });
        c.bench_function(&format!("modulo_scalar({})", density), |b| {
            b.iter(|| criterion::black_box(modulus_scalar(&arr_a, scalar).unwrap()))
        });
    }
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);

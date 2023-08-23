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

use arrow_array::{Array, BooleanArray, GenericStringArray, OffsetSizeTrait};
use arrow_buffer::bit_util::set_bit;
use arrow_buffer::{BooleanBuffer, MutableBuffer};
use regex::Regex;

/// A string based predicate
pub enum Predicate<'a> {
    Eq(&'a str),
    IEqAscii(&'a str),
    Contains(&'a str),
    StartsWith(&'a str),
    IStartsWithAscii(&'a str),
    EndsWith(&'a str),
    IEndsWithAscii(&'a str),
    Regex(Regex),
}

impl<'a> Predicate<'a> {
    pub fn evaluate_array<O: OffsetSizeTrait>(
        &self,
        array: &GenericStringArray<O>,
        negate: bool,
    ) -> BooleanArray {
        match self {
            Predicate::Eq(v) => array_op(array, negate, |haystack| v == haystack),
            Predicate::IEqAscii(v) => {
                array_op(array, negate, |haystack| haystack.eq_ignore_ascii_case(v))
            }
            Predicate::Contains(v) => {
                array_op(array, negate, |haystack| haystack.contains(v))
            }
            Predicate::StartsWith(v) => {
                array_op(array, negate, |haystack| haystack.starts_with(v))
            }
            Predicate::IStartsWithAscii(v) => array_op(array, negate, |haystack| {
                starts_with_ignore_ascii_case(haystack, v)
            }),
            Predicate::EndsWith(v) => {
                array_op(array, negate, |haystack| haystack.ends_with(v))
            }
            Predicate::IEndsWithAscii(v) => array_op(array, negate, |haystack| {
                ends_with_ignore_ascii_case(haystack, v)
            }),
            Predicate::Regex(v) => {
                array_op(array, negate, |haystack| v.is_match(haystack))
            }
        }
    }
}

#[inline(never)]
fn array_op<O>(
    array: &GenericStringArray<O>,
    negate: bool,
    predicate: impl Fn(&str) -> bool,
) -> BooleanArray {
    // TODO: Test if iterator faster

    let len = array.len();
    let mut buffer = MutableBuffer::new_null(len);
    let slice = buffer.as_slice_mut();

    let f = |idx: usize| unsafe {
        if predicate(array.value(*idx)) != negate {
            set_bit(slice, idx)
        }
    };

    match array.nulls().filter(|x| x.null_count() > 0) {
        Some(n) => n.valid_indices().for_each(f),
        None => (0..len).for_each(f),
    };

    let values = BooleanBuffer::new(buffer.into(), 0, len);
    BooleanArray::new(values, array.nulls().cloned())
}

fn starts_with_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    let end = haystack.len().min(needle.len());
    haystack.is_char_boundary(end) && needle.eq_ignore_ascii_case(&haystack[..end])
}

fn ends_with_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    let start = haystack.len().saturating_sub(needle.len());
    haystack.is_char_boundary(start) && needle.eq_ignore_ascii_case(&haystack[start..])
}

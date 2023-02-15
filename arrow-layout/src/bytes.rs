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

use crate::{NullBufferRef, Offset, OffsetBufferRef};
use std::ops::Range;

mod private {
    pub trait BytesSealed {}
}

pub trait Bytes: private::BytesSealed + Send + Sync {
    unsafe fn slice_unchecked(&self, range: Range<usize>) -> &Self;
}

impl private::BytesSealed for [u8] {}
impl Bytes for [u8] {
    unsafe fn slice_unchecked(&self, range: Range<usize>) -> &Self {
        self.get_unchecked(range)
    }
}

impl private::BytesSealed for str {}
impl Bytes for str {
    unsafe fn slice_unchecked(&self, range: Range<usize>) -> &Self {
        self.get_unchecked(range)
    }
}

#[derive(Debug, Copy, Clone)]
pub enum BytesLayout<'a> {
    Utf8(BytesArrayRef<'a, i32, str>),
    Binary(BytesArrayRef<'a, i32, [u8]>),
    LargeUtf8(BytesArrayRef<'a, i64, str>),
    LargeBinary(BytesArrayRef<'a, i64, [u8]>),
}

#[derive(Debug)]
pub struct BytesArrayRef<'a, O: Offset, B: Bytes + ?Sized> {
    values: &'a B,
    offsets: OffsetBufferRef<'a, O>,
    nulls: NullBufferRef<'a>,
}

impl<'a, O: Offset, B: Bytes + ?Sized> Clone for BytesArrayRef<'a, O, B> {
    fn clone(&self) -> Self {
        Self {
            values: self.values,
            offsets: self.offsets,
            nulls: self.nulls,
        }
    }
}

impl<'a, O: Offset, B: Bytes + ?Sized> Copy for BytesArrayRef<'a, O, B> {}

impl<'a, O: Offset, B: Bytes + ?Sized> BytesArrayRef<'a, O, B> {
    /// # Safety
    ///
    /// - `nulls` must be the same length as `offsets + 1`
    /// - `offsets` must contain at least one element
    /// - `offsets` must be positive, monotonically increasing and less than `values.len()`
    pub unsafe fn new_unchecked(
        values: &'a B,
        offsets: OffsetBufferRef<'a, O>,
        nulls: NullBufferRef<'a>,
    ) -> Self {
        Self {
            values,
            offsets,
            nulls,
        }
    }

    #[inline]
    pub fn offsets(&self) -> OffsetBufferRef<'a, O> {
        self.offsets
    }

    #[inline]
    pub fn values(&self) -> &'a B {
        self.values
    }

    #[inline]
    pub fn nulls(&self) -> NullBufferRef<'a> {
        self.nulls
    }
}

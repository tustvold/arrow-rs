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

use std::ops::Range;

#[derive(Debug, Copy, Clone)]
pub struct BitmapRef<'a> {
    packed: &'a [u8],
    start: usize,
    end: usize,
}

impl<'a> BitmapRef<'a> {
    /// # Safety
    ///
    /// `range` must describe a valid range of `packed`
    pub unsafe fn new_unchecked(packed: &'a [u8], range: Range<usize>) -> Self {
        Self {
            packed,
            start: range.start,
            end: range.end,
        }
    }

    #[inline]
    pub fn range(&self) -> Range<usize> {
        self.start..self.end
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    #[inline]
    pub fn packed(&self) -> &'a [u8] {
        self.packed
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct NullBufferRef<'a> {
    nulls: Option<BitmapRef<'a>>,
    null_count: usize,
}

impl<'a> NullBufferRef<'a> {
    /// # Safety
    ///
    /// `null_count` must match the number of `0` bits in `nulls`
    pub unsafe fn new_unchecked(nulls: BitmapRef<'a>, null_count: usize) -> Self {
        Self {
            nulls: Some(nulls),
            null_count,
        }
    }

    #[inline]
    pub fn bitmap(&self) -> Option<BitmapRef<'a>> {
        self.nulls
    }

    #[inline]
    pub fn null_count(&self) -> usize {
        self.null_count
    }
}

#[derive(Debug, Copy, Clone)]
pub struct BooleanArrayRef<'a> {
    values: BitmapRef<'a>,
    nulls: NullBufferRef<'a>,
}

impl<'a> BooleanArrayRef<'a> {
    /// # Safety
    ///
    /// - `nulls` must be the same length as `values`
    pub unsafe fn new_unchecked(values: BitmapRef<'a>, nulls: NullBufferRef<'a>) -> Self {
        Self { values, nulls }
    }

    #[inline]
    pub fn values(&self) -> BitmapRef<'a> {
        self.values
    }

    #[inline]
    pub fn nulls(&self) -> NullBufferRef<'a> {
        self.nulls
    }
}

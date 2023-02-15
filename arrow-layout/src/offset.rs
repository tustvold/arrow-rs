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

mod private {
    pub trait OffsetSealed {}
}

pub trait Offset: private::OffsetSealed + Send + Sync + Copy {
    fn as_usize(self) -> usize;
}
impl private::OffsetSealed for i32 {}
impl Offset for i32 {
    fn as_usize(self) -> usize {
        self as _
    }
}
impl private::OffsetSealed for i64 {}
impl Offset for i64 {
    fn as_usize(self) -> usize {
        self as _
    }
}

#[derive(Debug)]
pub struct OffsetBufferRef<'a, O: Offset> {
    offsets: &'a [O],
}

impl<'a, O: Offset> Clone for OffsetBufferRef<'a, O> {
    fn clone(&self) -> Self {
        Self {
            offsets: self.offsets,
        }
    }
}
impl<'a, O: Offset> Copy for OffsetBufferRef<'a, O> {}

impl<'a, O: Offset> OffsetBufferRef<'a, O> {
    /// # Safety
    ///
    /// - offsets must be non-empty
    /// - offsets must be positive and monotonically increasing
    pub unsafe fn new_unchecked(offsets: &'a [O]) -> Self {
        Self { offsets }
    }
}

impl<'a, O: Offset> AsRef<[O]> for OffsetBufferRef<'a, O> {
    fn as_ref(&self) -> &[O] {
        self.offsets
    }
}

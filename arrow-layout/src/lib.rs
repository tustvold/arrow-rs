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

//! Abstractions for data in an arrow-compatible layout

mod boolean;
mod bytes;
mod offset;
mod primitive;
pub use boolean::*;
pub use bytes::*;
pub use offset::*;
pub use primitive::*;

/// A generic type that lays it data out in memory in an arrow-compatible manner
pub trait ArrayLayout {
    /// Returns the [`Layout`] of this array
    fn layout(&self) -> Layout<'_>;

    /// Returns the child at index [`idx`]
    fn child(&self, idx: usize) -> &dyn ArrayLayout;
}

/// An arrow-compatible layout of data in memory
#[derive(Debug, Copy, Clone)]
pub enum Layout<'a> {
    Null(usize),
    Boolean(BooleanArrayRef<'a>),
    Primitive(PrimitiveLayout<'a>),
    Bytes(BytesLayout<'a>),
}

impl<'a> From<PrimitiveLayout<'a>> for Layout<'a> {
    fn from(value: PrimitiveLayout<'a>) -> Self {
        Self::Primitive(value)
    }
}

impl<'a> From<BytesLayout<'a>> for Layout<'a> {
    fn from(value: BytesLayout<'a>) -> Self {
        Self::Bytes(value)
    }
}

impl<'a> From<BooleanArrayRef<'a>> for Layout<'a> {
    fn from(value: BooleanArrayRef<'a>) -> Self {
        Self::Boolean(value)
    }
}

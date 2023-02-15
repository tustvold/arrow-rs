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

use crate::NullBufferRef;
use arrow_buffer::i256;
use half::f16;

mod private {
    pub trait PrimitiveSealed {}
}

/// A primitive type that can be stored in
pub trait Primitive: private::PrimitiveSealed + Copy + Send + Sync {}

macro_rules! primitive {
    ($($t:ty),*) => {
        $(
            impl Primitive for $t {}
            impl private::PrimitiveSealed for $t {}
        )*
    };
}
primitive!(i8, i16, i32, i64, i128, i256);
primitive!(u8, u16, u32, u64);
primitive!(f16, f32, f64);

#[derive(Debug, Copy, Clone)]
pub enum PrimitiveLayout<'a> {
    Int8(PrimitiveArrayRef<'a, i8>),
    Int16(PrimitiveArrayRef<'a, i16>),
    Int32(PrimitiveArrayRef<'a, i32>),
    Int64(PrimitiveArrayRef<'a, i64>),
    Int128(PrimitiveArrayRef<'a, i128>),
    Int256(PrimitiveArrayRef<'a, i256>),
    UInt8(PrimitiveArrayRef<'a, u8>),
    UInt16(PrimitiveArrayRef<'a, u16>),
    UInt32(PrimitiveArrayRef<'a, u32>),
    UInt64(PrimitiveArrayRef<'a, u64>),
    Float16(PrimitiveArrayRef<'a, f16>),
    Float32(PrimitiveArrayRef<'a, f32>),
    Float64(PrimitiveArrayRef<'a, f64>),
}

macro_rules! from_array {
    ($t:ty, $v:ident) => {
        impl<'a> From<PrimitiveArrayRef<'a, $t>> for PrimitiveLayout<'a> {
            fn from(v: PrimitiveArrayRef<'a, $t>) -> Self {
                Self::$v(v)
            }
        }
    };
}

from_array!(i8, Int8);
from_array!(i16, Int16);
from_array!(i32, Int32);
from_array!(i64, Int64);
from_array!(i128, Int128);
from_array!(i256, Int256);
from_array!(u8, UInt8);
from_array!(u16, UInt16);
from_array!(u32, UInt32);
from_array!(u64, UInt64);
from_array!(f16, Float16);
from_array!(f32, Float32);
from_array!(f64, Float64);

#[derive(Debug, Copy, Clone)]
pub struct PrimitiveArrayRef<'a, T: Primitive> {
    values: &'a [T],
    nulls: NullBufferRef<'a>,
}

impl<'a, T: Primitive> PrimitiveArrayRef<'a, T> {
    /// # Safety
    ///
    /// - `nulls` must be the same length as `values`
    pub unsafe fn new_unchecked(values: &'a [T], nulls: NullBufferRef<'a>) -> Self {
        Self { values, nulls }
    }

    #[inline]
    pub fn values(&self) -> &'a [T] {
        self.values
    }

    #[inline]
    pub fn nulls(&self) -> NullBufferRef<'a> {
        self.nulls
    }
}

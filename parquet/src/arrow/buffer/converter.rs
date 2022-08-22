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

use crate::data_type::{FixedLenByteArray};
use arrow::array::{
    Array, ArrayRef, Decimal128Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder,
    IntervalDayTimeArray, IntervalDayTimeBuilder, IntervalYearMonthArray,
    IntervalYearMonthBuilder,
};
use std::sync::Arc;

use crate::errors::Result;
use std::marker::PhantomData;

use crate::arrow::buffer::bit_util::sign_extend_be;
#[cfg(test)]
use crate::data_type::ByteArray;

#[cfg(test)]
use arrow::array::{StringArray, StringBuilder};

/// A converter is used to consume record reader's content and convert it to arrow
/// primitive array.
pub trait Converter<S, T> {
    /// This method converts record reader's buffered content into arrow array.
    /// It will consume record reader's data, but will not reset record reader's
    /// state.
    fn convert(&self, source: S) -> Result<T>;
}

pub struct FixedSizeArrayConverter {
    byte_width: i32,
}

impl FixedSizeArrayConverter {
    pub fn new(byte_width: i32) -> Self {
        Self { byte_width }
    }
}

impl Converter<Vec<Option<FixedLenByteArray>>, FixedSizeBinaryArray>
    for FixedSizeArrayConverter
{
    fn convert(
        &self,
        source: Vec<Option<FixedLenByteArray>>,
    ) -> Result<FixedSizeBinaryArray> {
        let mut builder =
            FixedSizeBinaryBuilder::with_capacity(source.len(), self.byte_width);
        for v in source {
            match v {
                Some(array) => builder.append_value(array.data())?,
                None => builder.append_null(),
            }
        }

        Ok(builder.finish())
    }
}

pub struct DecimalArrayConverter {
    precision: u8,
    scale: u8,
}

impl DecimalArrayConverter {
    pub fn new(precision: u8, scale: u8) -> Self {
        Self { precision, scale }
    }
}

impl Converter<Vec<Option<FixedLenByteArray>>, Decimal128Array>
    for DecimalArrayConverter
{
    fn convert(&self, source: Vec<Option<FixedLenByteArray>>) -> Result<Decimal128Array> {
        let array = source
            .into_iter()
            .map(|array| array.map(|array| from_bytes_to_i128(array.data())))
            .collect::<Decimal128Array>()
            .with_precision_and_scale(self.precision, self.scale)?;

        Ok(array)
    }
}

// Convert the bytes array to i128.
// The endian of the input bytes array must be big-endian.
fn from_bytes_to_i128(b: &[u8]) -> i128 {
    // The bytes array are from parquet file and must be the big-endian.
    // The endian is defined by parquet format, and the reference document
    // https://github.com/apache/parquet-format/blob/54e53e5d7794d383529dd30746378f19a12afd58/src/main/thrift/parquet.thrift#L66
    i128::from_be_bytes(sign_extend_be(b))
}

/// An Arrow Interval converter, which reads the first 4 bytes of a Parquet interval,
/// and interprets it as an i32 value representing the Arrow YearMonth value
pub struct IntervalYearMonthArrayConverter {}

impl Converter<Vec<Option<FixedLenByteArray>>, IntervalYearMonthArray>
    for IntervalYearMonthArrayConverter
{
    fn convert(
        &self,
        source: Vec<Option<FixedLenByteArray>>,
    ) -> Result<IntervalYearMonthArray> {
        let mut builder = IntervalYearMonthBuilder::new(source.len());
        for v in source {
            match v {
                Some(array) => builder.append_value(i32::from_le_bytes(
                    array.data()[0..4].try_into().unwrap(),
                )),
                None => builder.append_null(),
            }
        }

        Ok(builder.finish())
    }
}

/// An Arrow Interval converter, which reads the last 8 bytes of a Parquet interval,
/// and interprets it as an i32 value representing the Arrow DayTime value
pub struct IntervalDayTimeArrayConverter {}

impl Converter<Vec<Option<FixedLenByteArray>>, IntervalDayTimeArray>
    for IntervalDayTimeArrayConverter
{
    fn convert(
        &self,
        source: Vec<Option<FixedLenByteArray>>,
    ) -> Result<IntervalDayTimeArray> {
        let mut builder = IntervalDayTimeBuilder::new(source.len());
        for v in source {
            match v {
                Some(array) => builder.append_value(i64::from_le_bytes(
                    array.data()[4..12].try_into().unwrap(),
                )),
                None => builder.append_null(),
            }
        }

        Ok(builder.finish())
    }
}

#[cfg(test)]
pub struct Utf8ArrayConverter {}

#[cfg(test)]
impl Converter<Vec<Option<ByteArray>>, StringArray> for Utf8ArrayConverter {
    fn convert(&self, source: Vec<Option<ByteArray>>) -> Result<StringArray> {
        let data_size = source
            .iter()
            .map(|x| x.as_ref().map(|b| b.len()).unwrap_or(0))
            .sum();

        let mut builder = StringBuilder::with_capacity(source.len(), data_size);
        for v in source {
            match v {
                Some(array) => builder.append_value(array.as_utf8()?),
                None => builder.append_null(),
            }
        }

        Ok(builder.finish())
    }
}

#[cfg(test)]
pub type Utf8Converter =
    ArrayRefConverter<Vec<Option<ByteArray>>, StringArray, Utf8ArrayConverter>;

pub type FixedLenBinaryConverter = ArrayRefConverter<
    Vec<Option<FixedLenByteArray>>,
    FixedSizeBinaryArray,
    FixedSizeArrayConverter,
>;
pub type IntervalYearMonthConverter = ArrayRefConverter<
    Vec<Option<FixedLenByteArray>>,
    IntervalYearMonthArray,
    IntervalYearMonthArrayConverter,
>;
pub type IntervalDayTimeConverter = ArrayRefConverter<
    Vec<Option<FixedLenByteArray>>,
    IntervalDayTimeArray,
    IntervalDayTimeArrayConverter,
>;

pub type DecimalFixedLengthByteArrayConverter = ArrayRefConverter<
    Vec<Option<FixedLenByteArray>>,
    Decimal128Array,
    DecimalArrayConverter,
>;

pub struct ArrayRefConverter<S, A, C> {
    _source: PhantomData<S>,
    _array: PhantomData<A>,
    converter: C,
}

impl<S, A, C> ArrayRefConverter<S, A, C>
where
    A: Array + 'static,
    C: Converter<S, A> + 'static,
{
    pub fn new(converter: C) -> Self {
        Self {
            _source: PhantomData,
            _array: PhantomData,
            converter,
        }
    }
}

impl<S, A, C> Converter<S, ArrayRef> for ArrayRefConverter<S, A, C>
where
    A: Array + 'static,
    C: Converter<S, A> + 'static,
{
    fn convert(&self, source: S) -> Result<ArrayRef> {
        self.converter
            .convert(source)
            .map(|array| Arc::new(array) as ArrayRef)
    }
}

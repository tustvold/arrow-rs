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

use crate::ArrayData;
use arrow_buffer::{ArrowNativeType, i256};
use arrow_layout::*;
use arrow_schema::{DataType, IntervalUnit};
use half::f16;
use std::ops::Range;

static SMALL_OFFSET: i32 = 0;
static LARGE_OFFSET: i64 = 0;

/// Returns the sliced range of `data`
fn range(data: &ArrayData) -> Range<usize> {
    data.offset()..data.offset() + data.len()
}

/// Extracts the null buffer from `data`
fn nulls(data: &ArrayData) -> NullBufferRef {
    data.null_buffer()
        .map(|x| unsafe {
            let bitmap = BitmapRef::new_unchecked(x, range(data));
            NullBufferRef::new_unchecked(bitmap, data.null_count())
        })
        .unwrap_or_default()
}

/// # Safety
///
/// data must contain offsets of type `O`
unsafe fn offsets<'a, O: Offset + ArrowNativeType>(
    data: &'a ArrayData,
    zero: &'a O,
) -> OffsetBufferRef<'a, O> {
    match data.is_empty() {
        true => OffsetBufferRef::new_unchecked(std::slice::from_ref(zero)),
        false => OffsetBufferRef::new_unchecked(
            &data.buffer(0)[data.offset()..data.offset() + data.len() + 1],
        ),
    }
}

/// # Safety
///
/// data must be a primitive containing elements of `T`
unsafe fn primitive<'a, T: Primitive + ArrowNativeType>(
    data: &'a ArrayData,
) -> PrimitiveLayout<'a>
where
    PrimitiveLayout<'a>: From<PrimitiveArrayRef<'a, T>>,
{
    unsafe { PrimitiveArrayRef::new_unchecked(data.buffer::<T>(0), nulls(data)) }.into()
}

impl ArrayLayout for ArrayData {
    fn logical_type(&self) -> &DataType {
        self.data_type()
    }

    fn layout(&self) -> Layout<'_> {
        unsafe {
            match self.data_type() {
                DataType::Null => Layout::Null(self.len()),
                DataType::Boolean => {
                    let values =
                        BitmapRef::new_unchecked(self.buffer::<u8>(0), range(self));
                    BooleanArrayRef::new_unchecked(values, nulls(self)).into()
                }
                DataType::Int8 => primitive::<i8>(self).into(),
                DataType::Int16 => primitive::<i16>(self).into(),
                DataType::Int32
                | DataType::Date32
                | DataType::Time32(_)
                | DataType::Interval(IntervalUnit::YearMonth) => {
                    primitive::<i32>(self).into()
                }
                DataType::Int64
                | DataType::Date64
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Timestamp(_, _)
                | DataType::Interval(IntervalUnit::DayTime) => {
                    primitive::<i64>(self).into()
                }
                DataType::Decimal128(_, _)
                | DataType::Interval(IntervalUnit::MonthDayNano) => {
                    primitive::<i128>(self).into()
                }
                DataType::Decimal256(_, _) => primitive::<i256>(self).into(),
                DataType::UInt8 => primitive::<u8>(self).into(),
                DataType::UInt16 => primitive::<u16>(self).into(),
                DataType::UInt32 => primitive::<u32>(self).into(),
                DataType::UInt64 => primitive::<u64>(self).into(),
                DataType::Float16 => primitive::<f16>(self).into(),
                DataType::Float32 => primitive::<f32>(self).into(),
                DataType::Float64 => primitive::<f64>(self).into(),
                DataType::Binary => {
                    let offsets = offsets(self, &SMALL_OFFSET);
                    let values = self.buffer(1);
                    let nulls = nulls(self);
                    let array = BytesArrayRef::new_unchecked(values, offsets, nulls);
                    BytesLayout::Binary(array).into()
                }
                DataType::FixedSizeBinary(_) => todo!(),
                DataType::LargeBinary => {
                    let offsets = offsets(self, &LARGE_OFFSET);
                    let values = self.buffer(1);
                    let nulls = nulls(self);
                    let array = BytesArrayRef::new_unchecked(values, offsets, nulls);
                    BytesLayout::LargeBinary(array).into()
                }
                DataType::Utf8 => {
                    let offsets = offsets(self, &SMALL_OFFSET);
                    let values = std::str::from_utf8_unchecked(self.buffer(1));
                    let nulls = nulls(self);
                    let array = BytesArrayRef::new_unchecked(values, offsets, nulls);
                    BytesLayout::Utf8(array).into()
                }
                DataType::LargeUtf8 => {
                    let offsets = offsets(self, &LARGE_OFFSET);
                    let values = std::str::from_utf8_unchecked(self.buffer(1));
                    let nulls = nulls(self);
                    let array = BytesArrayRef::new_unchecked(values, offsets, nulls);
                    BytesLayout::LargeUtf8(array).into()
                }
                DataType::List(_) => todo!(),
                DataType::FixedSizeList(_, _) => todo!(),
                DataType::LargeList(_) => todo!(),
                DataType::Struct(_) => todo!(),
                DataType::Union(_, _, _) => todo!(),
                DataType::Dictionary(_, _) => todo!(),
                DataType::Map(_, _) => todo!(),
                DataType::RunEndEncoded(_, _) => todo!(),
            }
        }
    }

    fn child(&self, idx: usize) -> &dyn ArrayLayout {
        &self.child_data()[idx]
    }
}

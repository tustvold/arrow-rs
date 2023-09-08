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

use crate::cast::AsArray;
use crate::iterator::ArrayIter;
use crate::types::Int8Type;
use crate::{Array, ListArray, PrimitiveArray};
use arrow_buffer::{ArrowNativeType, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, Field};
use std::sync::Arc;

/// A statically typed record that can be converted to/from arrow representation
///
/// For a dynamically type record representation see [arrow_row](https://docs.rs/arrow-row)
pub trait Record<'a> {
    type Array: Array + 'static;

    type Iter: Iterator<Item = Self>;

    fn collect<I: IntoIterator<Item = Self>>(s: I) -> Self::Array;

    fn iter(s: &'a dyn Array) -> Result<Self::Iter, ArrowError>;
}

impl<'a> Record<'a> for i8 {
    type Array = PrimitiveArray<Int8Type>;

    type Iter = std::iter::Copied<std::slice::Iter<'a, i8>>;

    fn collect<I: IntoIterator<Item = Self>>(s: I) -> Self::Array {
        PrimitiveArray::new(ScalarBuffer::from_iter(s), None)
    }

    fn iter(s: &'a dyn Array) -> Result<Self::Iter, ArrowError> {
        let s = s.as_primitive_opt::<Int8Type>().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Cannot downcast array as Int8Array: got {}",
                s.data_type()
            ))
        })?;
        if s.null_count() != 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Encountered nulls in non-nullable array".to_string(),
            ));
        }
        Ok(s.values().iter().copied())
    }
}

impl<'a> Record<'a> for Option<i8> {
    type Array = PrimitiveArray<Int8Type>;

    type Iter = ArrayIter<&'a PrimitiveArray<Int8Type>>;

    fn collect<I: IntoIterator<Item = Self>>(s: I) -> Self::Array {
        s.into_iter().collect()
    }

    fn iter(s: &'a dyn Array) -> Result<Self::Iter, ArrowError> {
        let s = s.as_primitive_opt::<Int8Type>().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Cannot downcast array as Int8Array: got {}",
                s.data_type()
            ))
        })?;
        Ok(s.iter())
    }
}

pub struct ListIter<'a, T> {
    offsets: std::slice::Iter<'a, i32>,
    current: usize,
    inner: T,
}

impl<'a, T: Iterator> Iterator for ListIter<'a, T> {
    type Item = Vec<T::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.offsets.next()?.as_usize();
        let collected = (self.current..next)
            .map(|_| self.inner.next().unwrap())
            .collect();
        self.current = next;
        Some(collected)
    }
}

impl<'a, T: Record<'a> + 'static> Record<'a> for Vec<T> {
    type Array = ListArray;
    type Iter = ListIter<'a, T::Iter>;

    fn collect<I: IntoIterator<Item = Self>>(s: I) -> Self::Array {
        let iter = s.into_iter();
        let mut current = 0_usize;
        let mut offsets = Vec::with_capacity(iter.size_hint().0 + 1);
        offsets.push(0);
        let iter = iter
            .inspect(|x| {
                current += x.len();
                offsets.push(current as i32);
            })
            .flatten();
        let values = T::collect(iter);
        assert!(i32::try_from(current).is_ok());

        let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };
        let field = Arc::new(Field::new("item", values.data_type().clone(), false));
        ListArray::new(field, offsets, Arc::new(values), None)
    }

    fn iter(s: &'a dyn Array) -> Result<Self::Iter, ArrowError> {
        let l = s.as_list_opt::<i32>().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Cannot downcast array as ListArray: got {}",
                s.data_type()
            ))
        })?;
        if l.null_count() != 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Encountered nulls in non-nullable array".to_string(),
            ));
        }
        let inner = T::iter(l.values().as_ref())?;
        let mut offsets = l.value_offsets().iter();
        Ok(ListIter {
            current: offsets.next().unwrap().as_usize(),
            offsets,
            inner,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let a: Vec<Vec<i8>> = vec![
            vec![1, 2, 3, 4, 5, 6],
            vec![0, 7, 3],
            vec![-1],
        ];
        let list = Record::collect(a);
        println!("{:?}", list);
    }

}

//
// impl Record for Option<i8> {
//     type Array = PrimitiveArray<Int8Type>;
//
//     fn collect<I: IntoIterator<Item = Self>>(s: I) -> Self::Array {
//         s.into_iter().collect()
//     }
//
//     fn iter(s: Self::Array) -> Vec<Self> {
//         s.iter().collect()
//     }
// }

// impl<T: Record> Record for Vec<T> {
//     type Array = ListArray;
//
//     fn convert_records(s: Vec<Self>) -> Self::Array {
//         todo!()
//     }
//
//     fn convert_columns(s: Self::Array) -> Vec<Self> {
//
//
//         let t = s.values().as_any().downcast_ref::<T::Array>().unwrap();
//         let values = T::convert_columns(t)
//         todo!()
//     }
// }

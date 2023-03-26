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

//! Defines builders that can be used to safely build arrays
//!
//! # Basic Usage
//!
//! Builders can be used to build simple, non-nested arrays
//!
//! ```
//! # use arrow_array::builder::Int32Builder;
//! # use arrow_array::PrimitiveArray;
//! let mut a = Int32Builder::new();
//! a.append_value(1);
//! a.append_null();
//! a.append_value(2);
//! let a = a.finish();
//!
//! assert_eq!(a, PrimitiveArray::from(vec![Some(1), None, Some(2)]));
//! ```
//!
//! ```
//! # use arrow_array::builder::StringBuilder;
//! # use arrow_array::{Array, StringArray};
//! let mut a = StringBuilder::new();
//! a.append_value("foo");
//! a.append_value("bar");
//! a.append_null();
//! let a = a.finish();
//!
//! assert_eq!(a, StringArray::from_iter([Some("foo"), Some("bar"), None]));
//! ```
//!
//! # Nested Usage
//!
//! Builders can also be used to build more complex nested arrays, such as lists
//!
//! ```
//! # use arrow_array::builder::{Int32Builder, ListBuilder};
//! # use arrow_array::ListArray;
//! # use arrow_array::types::Int32Type;
//! let mut a = ListBuilder::new(Int32Builder::new());
//! // [1, 2]
//! a.values().append_value(1);
//! a.values().append_value(2);
//! a.append(true);
//! // null
//! a.append(false);
//! // []
//! a.append(true);
//! // [3, null]
//! a.values().append_value(3);
//! a.values().append_null();
//! a.append(true);
//!
//! // [[1, 2], null, [], [3, null]]
//! let a = a.finish();
//!
//! assert_eq!(a, ListArray::from_iter_primitive::<Int32Type, _, _>([
//!     Some(vec![Some(1), Some(2)]),
//!     None,
//!     Some(vec![]),
//!     Some(vec![Some(3), None])]
//! ))
//! ```
//!
//! # Row Conversion
//!
//! It is common to have a statically defined row representation, and to want to convert
//! this to an arrow representation. An example of this can be seen below
//!
//! ```
//! use std::any::Any;
//! use arrow_array::builder::{ArrayBuilder, Int32Builder, ListBuilder, StringBuilder};
//! use arrow_array::{ArrayRef, RecordBatch, StructArray};
//! use arrow_schema::{DataType, Field};
//! use std::sync::Arc;
//! /// A representation of a row
//! struct Row {
//!     i32: i32,
//!     optional_i32: Option<i32>,
//!     string: Option<String>,
//!     i32_list: Option<Vec<Option<i32>>>,
//! }
//!
//! /// Converts `Vec<Row>` into `StructArray`
//! #[derive(Debug, Default)]
//! struct RowBuilder {
//!     i32: Int32Builder,
//!     string: StringBuilder,
//!     i32_list: ListBuilder<Int32Builder>,
//! }
//!
//! impl<'a> Extend<&'a Row> for RowBuilder {
//!     fn extend<T: IntoIterator<Item = &'a Row>>(&mut self, iter: T) {
//!         for row in iter {
//!             self.i32.append_value(row.i32);
//!             self.string.append_option(row.string.as_ref());
//!             match &row.i32_list {
//!                 Some(list) => {
//!                     list.iter().for_each(|v| self.i32_list.values().append_option(*v));
//!                     self.i32_list.append(true);
//!                 }
//!                 None => self.i32_list.append(false),
//!             }
//!         }
//!     }
//! }
//!
//! impl RowBuilder {
//!     /// Note: returns StructArray to allow nesting within another array if desired
//!     fn finish(&mut self) -> StructArray {
//!         let i32 = Arc::new(self.i32.finish()) as ArrayRef;
//!         let i32_field = Field::new("i32", DataType::Int32, false);
//!
//!         let string = Arc::new(self.string.finish()) as ArrayRef;
//!         let string_field = Field::new("i32", DataType::Utf8, false);
//!
//!         let i32_list = Arc::new(self.i32_list.finish()) as ArrayRef;
//!         let value_field = Box::new(Field::new("item", DataType::Int32, true));
//!         let i32_list_field = Field::new("i32_list", DataType::List(value_field), true);
//!
//!         StructArray::from(vec![
//!             (i32_field, i32),
//!             (string_field, string),
//!             (i32_list_field, i32_list),
//!         ])
//!     }
//! }
//!
//! /// Converts a slice of [`Row`] to a [`RecordBatch`]
//! fn rows_to_batch(rows: &[Row]) -> RecordBatch {
//!     let mut builder = RowBuilder::default();
//!     builder.extend(rows);
//!     RecordBatch::from(&builder.finish())
//! }
//! ```

mod boolean_buffer_builder;
pub use boolean_buffer_builder::*;

mod boolean_builder;
pub use boolean_builder::*;
mod buffer_builder;
pub use buffer_builder::*;
mod fixed_size_binary_builder;
pub use fixed_size_binary_builder::*;
mod fixed_size_list_builder;
pub use fixed_size_list_builder::*;
mod generic_bytes_builder;
pub use generic_bytes_builder::*;
mod generic_list_builder;
pub use generic_list_builder::*;
mod map_builder;
pub use map_builder::*;
mod null_buffer_builder;
mod primitive_builder;
pub use primitive_builder::*;
mod primitive_dictionary_builder;
pub use primitive_dictionary_builder::*;
mod primitive_run_builder;
pub use primitive_run_builder::*;
mod struct_builder;
pub use struct_builder::*;
mod generic_bytes_dictionary_builder;
pub use generic_bytes_dictionary_builder::*;
mod generic_byte_run_builder;
pub use generic_byte_run_builder::*;
mod union_builder;
pub use union_builder::*;

use crate::ArrayRef;
use std::any::Any;

/// Trait for dealing with different array builders at runtime
///
/// # Example
///
/// ```
/// // Create
/// # use arrow_array::{ArrayRef, StringArray};
/// # use arrow_array::builder::{ArrayBuilder, Float64Builder, Int64Builder, StringBuilder};
///
/// let mut data_builders: Vec<Box<dyn ArrayBuilder>> = vec![
///     Box::new(Float64Builder::new()),
///     Box::new(Int64Builder::new()),
///     Box::new(StringBuilder::new()),
/// ];
///
/// // Fill
/// data_builders[0]
///     .as_any_mut()
///     .downcast_mut::<Float64Builder>()
///     .unwrap()
///     .append_value(3.14);
/// data_builders[1]
///     .as_any_mut()
///     .downcast_mut::<Int64Builder>()
///     .unwrap()
///     .append_value(-1);
/// data_builders[2]
///     .as_any_mut()
///     .downcast_mut::<StringBuilder>()
///     .unwrap()
///     .append_value("🍎");
///
/// // Finish
/// let array_refs: Vec<ArrayRef> = data_builders
///     .iter_mut()
///     .map(|builder| builder.finish())
///     .collect();
/// assert_eq!(array_refs[0].len(), 1);
/// assert_eq!(array_refs[1].is_null(0), false);
/// assert_eq!(
///     array_refs[2]
///         .as_any()
///         .downcast_ref::<StringArray>()
///         .unwrap()
///         .value(0),
///     "🍎"
/// );
/// ```
pub trait ArrayBuilder: Any + Send {
    /// Returns the number of array slots in the builder
    fn len(&self) -> usize;

    /// Returns whether number of array slots is zero
    fn is_empty(&self) -> bool;

    /// Builds the array
    fn finish(&mut self) -> ArrayRef;

    /// Builds the array without resetting the underlying builder.
    fn finish_cloned(&self) -> ArrayRef;

    /// Returns the builder as a non-mutable `Any` reference.
    ///
    /// This is most useful when one wants to call non-mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_ref` to get a reference on the specific builder.
    fn as_any(&self) -> &dyn Any;

    /// Returns the builder as a mutable `Any` reference.
    ///
    /// This is most useful when one wants to call mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_mut` to get a reference on the specific builder.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any>;
}

/// Builder for [`ListArray`]s (i32 offsets)
///
/// [`ListArray`]: crate::array::ListArray
///
/// # Example
///
/// ```
/// # use arrow_array::builder::{StringBuilder, ListBuilder};
/// # use arrow_array::ListArray;
/// // Build a 3 element array of lists:
/// //
/// // column
/// // ---------
/// // [one]
/// // []
/// // [two, three]
///
/// let mut builder = ListBuilder::new(StringBuilder::new());
/// // [one]
/// builder.values().append_value("one");
/// builder.append(true);
/// // []
/// builder.append(true);
/// // [two, three]
/// builder.values().append_value("two");
/// builder.values().append_value("three");
/// builder.append(true);
///
/// // Create an array
/// let list_array: ListArray = builder.finish();
/// ```
pub type ListBuilder<T> = GenericListBuilder<i32, T>;

/// Builder for [`LargeListArray`]s (i64 offsets)
///
/// [`LargeListArray`]: crate::array::LargeListArray
///
/// # Example
///
/// ```
/// # use arrow_array::builder::{StringBuilder, LargeListBuilder};
/// # use arrow_array::LargeListArray;
/// // Build a 3 element array of lists:
/// //
/// // column
/// // ---------
/// // [one],
/// // [],
/// // [two, three]
///
/// let mut builder = LargeListBuilder::new(StringBuilder::new());
/// // [one]
/// builder.values().append_value("one");
/// builder.append(true);
/// // []
/// builder.append(true);
/// // [two, three]
/// builder.values().append_value("two");
/// builder.values().append_value("three");
/// builder.append(true);
///
/// // Create an array
/// let list_array: LargeListArray = builder.finish();
/// ```
pub type LargeListBuilder<T> = GenericListBuilder<i64, T>;

/// A binary array builder with i32 offsets
pub type BinaryBuilder = GenericBinaryBuilder<i32>;
/// A binary array builder with i64 offsets
pub type LargeBinaryBuilder = GenericBinaryBuilder<i64>;

/// A string array builder with i32 offsets
pub type StringBuilder = GenericStringBuilder<i32>;
/// A string array builder with i64 offsets
pub type LargeStringBuilder = GenericStringBuilder<i64>;

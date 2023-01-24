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

//! An alternative JSON reader that interprets bytes in place

use crate::raw::boolean_array::BooleanArrayDecoder;
use crate::raw::list_array::ListArrayDecoder;
use crate::raw::primitive_array::PrimitiveArrayDecoder;
use crate::raw::string_array::StringArrayDecoder;
use crate::raw::struct_array::StructArrayDecoder;
use crate::raw::tape::{Tape, TapeDecoder, TapeElement};
use arrow_array::types::*;
use arrow_array::{downcast_integer, make_array, RecordBatch, RecordBatchReader};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, SchemaRef};
use std::io::BufRead;

mod boolean_array;
mod list_array;
mod primitive_array;
mod string_array;
mod struct_array;
mod tape;

/// A builder for [`RawReader`]
pub struct RawReaderBuilder {
    batch_size: usize,

    schema: SchemaRef,
}

impl RawReaderBuilder {
    /// Create a new [`RawReaderBuilder`] with the provided [`SchemaRef`]
    ///
    /// This could be obtained using [`infer_json_schema`] if not known
    ///
    /// [`infer_json_schema`]: crate::reader::infer_json_schema
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            batch_size: 1024,
            schema,
        }
    }

    /// Sets the batch size in rows to read
    pub fn with_batch_size(self, batch_size: usize) -> Self {
        Self { batch_size, ..self }
    }

    /// Create with the provided [`BufRead`]
    pub fn build<R: BufRead>(self, reader: R) -> Result<RawReader<R>, ArrowError> {
        Ok(RawReader {
            reader,
            decoder: RawDecoder::try_new(self.schema, self.batch_size)?,
        })
    }
}

/// A [`RecordBatchReader`] that reads newline-delimited JSON data with a known schema
/// directly into the corresponding arrow arrays
///
/// This makes it significantly faster than [`Reader`]
///
/// Lines consisting solely of ASCII whitespace are ignored
///
/// [`Reader`]: crate::reader::Reader
pub struct RawReader<R> {
    reader: R,
    decoder: RawDecoder,
}

impl<R> std::fmt::Debug for RawReader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawReader")
            .field("decoder", &self.decoder)
            .finish()
    }
}

impl<R: BufRead> RawReader<R> {
    /// Reads the next [`RecordBatch`] returning `Ok(None)` if EOF
    fn read(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        loop {
            let buf = self.reader.fill_buf()?;
            let read = buf.len();
            let decoded = self.decoder.decode(buf)?;

            self.reader.consume(decoded);
            if decoded == read {
                return self.decoder.flush();
            }
        }
    }
}

impl<R: BufRead> Iterator for RawReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read().transpose()
    }
}

impl<R: BufRead> RecordBatchReader for RawReader<R> {
    fn schema(&self) -> SchemaRef {
        self.decoder.schema.clone()
    }
}

/// [`RawDecoder`] provides a low-level interface for reading JSON data from a byte stream
///
/// See [`RawReader`] for a higher-level interface
pub struct RawDecoder {
    tape: TapeDecoder,
    decoder: Box<dyn ArrayDecoder>,
    batch_size: usize,
    schema: SchemaRef,
}

impl std::fmt::Debug for RawDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawDecoder")
            .field("schema", &self.schema)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl RawDecoder {
    /// Create a [`RawDecoder`] with the provided schema and batch size
    pub fn try_new(schema: SchemaRef, batch_size: usize) -> Result<Self, ArrowError> {
        let decoder = make_decoder(DataType::Struct(schema.fields.clone()), false)?;
        // TODO: This should probably include nested fields
        let num_fields = schema.fields().len();

        Ok(Self {
            tape: TapeDecoder::new(batch_size, num_fields),
            decoder,
            batch_size,
            schema,
        })
    }

    /// Read JSON objects from `buf`, returning the number of bytes read
    ///
    /// This method returns once `batch_size` objects have been parsed since the
    /// last call to [`Self::flush`], or `buf` is exhausted. Any remaining bytes
    /// should be included in the next call to [`Self::decode`]
    ///
    /// There is no requirement that `buf` contains a whole number of records, facilitating
    /// integration with arbitrary byte streams, such as that yielded by [`BufRead`]
    ///
    /// For example, a similar construction to [`RawReader`] could be implemented with
    ///
    /// ```
    /// # use std::io::BufRead;
    /// # use arrow_array::RecordBatch;
    /// # use arrow_json::raw::RawDecoder;
    /// # use arrow_schema::{ArrowError, SchemaRef};
    ///
    /// fn read_from_json<R: BufRead>(
    ///     mut reader: R,
    ///     schema: SchemaRef,
    ///     batch_size: usize,
    /// ) -> Result<impl Iterator<Item = Result<RecordBatch, ArrowError>>, ArrowError> {
    ///     let mut decoder = RawDecoder::try_new(schema, batch_size)?;
    ///     let mut next = move || loop {
    ///         // RawDecoder is agnostic that buf doesn't contain whole records
    ///         let buf = reader.fill_buf()?;
    ///         let read = buf.len();
    ///         let decoded = decoder.decode(buf)?;
    ///
    ///         // Consume the number of bytes read
    ///         reader.consume(decoded);
    ///         if decoded == read {
    ///             return decoder.flush();
    ///         }
    ///     };
    ///     Ok(std::iter::from_fn(move || next().transpose()))
    /// }
    /// ```
    pub fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        self.tape.decode(buf)
    }

    /// Flushes the currently buffered data to a [`RecordBatch`]
    ///
    /// Returns `Ok(None)` if no buffered data
    ///
    /// Note: if called part way through decoding a record, this will return an error
    pub fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let tape = self.tape.finish()?;

        if tape.num_rows() == 0 {
            return Ok(None);
        }

        // First offset is null sentinel
        let mut next_object = 1;
        let pos: Vec<_> = (0..tape.num_rows())
            .map(|_| {
                let end = match tape.get(next_object) {
                    TapeElement::StartObject(end) => end,
                    _ => unreachable!("corrupt tape"),
                };
                std::mem::replace(&mut next_object, end + 1)
            })
            .collect();

        let decoded = self.decoder.decode(&tape, &pos)?;
        self.tape.clear();

        // Sanity check
        assert!(matches!(decoded.data_type(), DataType::Struct(_)));
        assert_eq!(decoded.null_count(), 0);
        assert_eq!(decoded.len(), pos.len());

        // Clear out buffer
        let columns = decoded
            .child_data()
            .iter()
            .map(|x| make_array(x.clone()))
            .collect();

        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok(Some(batch))
    }
}

trait ArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError>;
}

macro_rules! primitive_decoder {
    ($t:ty, $data_type:expr) => {
        Ok(Box::new(PrimitiveArrayDecoder::<$t>::new($data_type)))
    };
}

fn make_decoder(
    data_type: DataType,
    is_nullable: bool,
) -> Result<Box<dyn ArrayDecoder>, ArrowError> {
    downcast_integer! {
        data_type => (primitive_decoder, data_type),
        DataType::Float32 => primitive_decoder!(Float32Type, data_type),
        DataType::Float64 => primitive_decoder!(Float64Type, data_type),
        DataType::Boolean => Ok(Box::<BooleanArrayDecoder>::default()),
        DataType::Utf8 => Ok(Box::<StringArrayDecoder::<i32>>::default()),
        DataType::LargeUtf8 => Ok(Box::<StringArrayDecoder::<i64>>::default()),
        DataType::List(_) => Ok(Box::new(ListArrayDecoder::<i32>::new(data_type, is_nullable)?)),
        DataType::LargeList(_) => Ok(Box::new(ListArrayDecoder::<i64>::new(data_type, is_nullable)?)),
        DataType::Struct(_) => Ok(Box::new(StructArrayDecoder::new(data_type, is_nullable)?)),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            Err(ArrowError::JsonError(format!("{} is not supported by JSON", data_type)))
        }
        d => Err(ArrowError::NotYetImplemented(format!("Support for {} in JSON reader", d)))
    }
}

fn tape_error(d: TapeElement, expected: &str) -> ArrowError {
    ArrowError::JsonError(format!("expected {expected} got {d}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::{
        as_boolean_array, as_largestring_array, as_list_array, as_primitive_array,
        as_string_array, as_struct_array,
    };
    use arrow_array::types::Int32Type;
    use arrow_array::Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::io::Cursor;
    use std::sync::Arc;

    fn do_read(buf: &str, batch_size: usize, schema: SchemaRef) -> Vec<RecordBatch> {
        RawReaderBuilder::new(schema)
            .with_batch_size(batch_size)
            .build(Cursor::new(buf.as_bytes()))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[test]
    fn test_basic() {
        let buf = r#"
        {"a": 1, "b": 2, "c": true}
        {"a": 2E0, "b": 4, "c": false}

        {"b": 6, "a": 2.0}
        {"b": "5", "a": 2}
        {"b": 4e0}
        {"b": 7, "a": null}
        "#;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Boolean, true),
        ]));

        let batches = do_read(buf, 1024, schema);
        assert_eq!(batches.len(), 1);

        let col1 = as_primitive_array::<Int64Type>(batches[0].column(0));
        assert_eq!(col1.null_count(), 2);
        assert_eq!(col1.values(), &[1, 2, 2, 2, 0, 0]);
        assert!(col1.is_null(4));
        assert!(col1.is_null(5));

        let col2 = as_primitive_array::<Int32Type>(batches[0].column(1));
        assert_eq!(col2.null_count(), 0);
        assert_eq!(col2.values(), &[2, 4, 6, 5, 4, 7]);

        let col3 = as_boolean_array(batches[0].column(2));
        assert_eq!(col3.null_count(), 4);
        assert!(col3.value(0));
        assert!(!col3.is_null(0));
        assert!(!col3.value(1));
        assert!(!col3.is_null(1));
    }

    #[test]
    fn test_string() {
        let buf = r#"
        {"a": "1", "b": "2"}
        {"a": "hello", "b": "shoo"}
        {"b": "\t😁foo", "a": "\nfoobar\ud83d\ude00\u0061\u0073\u0066\u0067\u00FF"}
        
        {"b": null}
        {"b": "", "a": null}

        "#;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::LargeUtf8, true),
        ]));

        let batches = do_read(buf, 1024, schema);
        assert_eq!(batches.len(), 1);

        let col1 = as_string_array(batches[0].column(0));
        assert_eq!(col1.null_count(), 2);
        assert_eq!(col1.value(0), "1");
        assert_eq!(col1.value(1), "hello");
        assert_eq!(col1.value(2), "\nfoobar😀asfgÿ");
        assert!(col1.is_null(3));
        assert!(col1.is_null(4));

        let col2 = as_largestring_array(batches[0].column(1));
        assert_eq!(col2.null_count(), 1);
        assert_eq!(col2.value(0), "2");
        assert_eq!(col2.value(1), "shoo");
        assert_eq!(col2.value(2), "\t😁foo");
        assert!(col2.is_null(3));
        assert_eq!(col2.value(4), "");
    }

    #[test]
    fn test_complex() {
        let buf = r#"
           {"list": [], "nested": {"a": 1, "b": 2}, "nested_list": {"list2": [{"c": 3}, {"c": 4}]}}
           {"list": [5, 6], "nested": {"a": 7}, "nested_list": {"list2": []}}
           {"list": null, "nested": {"a": null}}
        "#;

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "list",
                DataType::List(Box::new(Field::new("element", DataType::Int32, false))),
                true,
            ),
            Field::new(
                "nested",
                DataType::Struct(vec![
                    Field::new("a", DataType::Int32, false),
                    Field::new("b", DataType::Int32, false),
                ]),
                true,
            ),
            Field::new(
                "nested_list",
                DataType::Struct(vec![Field::new(
                    "list2",
                    DataType::List(Box::new(Field::new(
                        "element",
                        DataType::Struct(vec![Field::new("c", DataType::Int32, false)]),
                        false,
                    ))),
                    true,
                )]),
                true,
            ),
        ]));

        let batches = do_read(buf, 1024, schema);
        assert_eq!(batches.len(), 1);

        let list = as_list_array(batches[0].column(0).as_ref());
        assert_eq!(list.value_offsets(), &[0, 0, 2, 2]);
        assert_eq!(list.null_count(), 1);
        assert!(list.is_null(4));
        let list_v = list.values();
        let list_values = as_primitive_array::<Int32Type>(list_v.as_ref());
        assert_eq!(list_values.values(), &[5, 6]);

        let nested = as_struct_array(batches[0].column(1).as_ref());
        let a = as_primitive_array::<Int32Type>(nested.column(0).as_ref());
        assert_eq!(list.null_count(), 1);
        assert_eq!(a.values(), &[1, 7, 0]);
        assert!(list.is_null(2));

        let b = as_primitive_array::<Int32Type>(nested.column(1).as_ref());
        assert_eq!(b.null_count(), 2);
        assert_eq!(b.len(), 3);
        assert_eq!(b.value(0), 2);
        assert!(b.is_null(1));
        assert!(b.is_null(2));

        let nested_list = as_struct_array(batches[0].column(2).as_ref());
        let list2 = as_list_array(nested_list.column(0).as_ref());
        assert_eq!(list2.null_count(), 1);
        assert_eq!(list2.value_offsets(), &[0, 2, 2, 2]);
        assert!(list2.is_null(3));

        let list2_v = list2.values();
        let list2_values = as_struct_array(list2_v.as_ref());

        let c = as_primitive_array::<Int32Type>(list2_values.column(0));
        assert_eq!(c.values(), &[3, 4]);
    }
}

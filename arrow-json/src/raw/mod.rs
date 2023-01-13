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
use crate::raw::primitive_array::PrimitiveArrayDecoder;
use crate::raw::string_array::StringArrayDecoder;
use crate::raw::struct_array::StructArrayDecoder;
use arrow_array::types::*;
use arrow_array::{downcast_integer, make_array, RecordBatch, RecordBatchReader};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, SchemaRef};
use std::borrow::Cow;
use std::io::BufRead;

mod boolean_array;
mod primitive_array;
mod string_array;
mod struct_array;

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
        let decoder = make_decoder(
            DataType::Struct(self.schema.fields.clone()),
            self.batch_size,
        )?;

        Ok(RawReader {
            reader,
            decoder,
            schema: self.schema,
            batch_size: self.batch_size,
        })
    }
}

/// A [`RecordBatchReader`] that reads newline-delimited JSON data with a known schema
/// directly into the corresponding arrow arrays
///
/// This makes it significantly faster than [`Reader`], however, it currently
/// does not support nested data
///
/// Lines consisting solely of ASCII whitespace are ignored
///
/// [`Reader`]: crate::reader::Reader
pub struct RawReader<R> {
    reader: R,
    batch_size: usize,
    decoder: Box<dyn ArrayDecoder>,
    schema: SchemaRef,
}

impl<R> std::fmt::Debug for RawReader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawReader")
            .field("batch_size", &self.batch_size)
            .field("schema", &self.schema)
            .finish()
    }
}

fn delimit_rows(buf: &[u8], end_offsets: &mut Vec<usize>, to_read: usize) {
    let mut has_non_whitespace = false;

    for (idx, b) in buf.iter().enumerate() {
        match b {
            b'\n' if has_non_whitespace => {
                end_offsets.push(idx);
                has_non_whitespace = false;

                if end_offsets.len() >= to_read {
                    return;
                }
            }
            b => has_non_whitespace |= !b.is_ascii_whitespace(),
        }
    }
}

impl<R: BufRead> RawReader<R> {
    /// Reads the next [`RecordBatch`] returning `Ok(None)` if EOF
    fn read(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let mut read = 0;
        let mut temporary = Vec::with_capacity(128);
        let mut end_offsets = Vec::with_capacity(self.batch_size);
        while read < self.batch_size {
            let buf = self.reader.fill_buf()?;
            if buf.is_empty() {
                if !temporary.is_empty() {
                    let s = std::str::from_utf8(&temporary).expect("TODO");
                    self.decoder.visit(s)?;
                    read += 1;
                }
                break;
            }

            end_offsets.clear();
            delimit_rows(buf, &mut end_offsets, self.batch_size - read);

            if end_offsets.is_empty() {
                // Need to read more data
                temporary.extend_from_slice(buf);
                let consumed = buf.len();
                self.reader.consume(consumed);
                continue;
            }

            read += end_offsets.len();
            let last_offset = *end_offsets.last().unwrap();
            let s = std::str::from_utf8(&buf[..last_offset]).expect("TODO");
            let mut start_offset = 0;
            for end_offset in end_offsets.iter().copied() {
                let visited = self.decoder.visit(&s[start_offset..end_offset])?;
                if s[start_offset + visited..end_offset]
                    .contains(|c: char| c.is_whitespace())
                {
                    panic!("TODO")
                }
                start_offset = end_offset;
            }
            self.reader.consume(last_offset);
        }

        let decoded = self.decoder.flush();

        // Sanity check
        assert!(matches!(decoded.data_type(), DataType::Struct(_)));
        assert_eq!(decoded.null_count(), 0);
        assert_eq!(decoded.len(), read);

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

impl<R: BufRead> Iterator for RawReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read().transpose()
    }
}

impl<R: BufRead> RecordBatchReader for RawReader<R> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

trait ArrayDecoder {
    /// Visits the provided row
    ///
    /// Returns the number of bytes read
    fn visit(&mut self, row: usize, buf: &str) -> Result<usize, ArrowError>;

    /// Flushes the contents of the decoder to an [`ArrayData`]
    fn flush(&mut self) -> ArrayData;
}

macro_rules! primitive_decoder {
    ($t:ty, $data_type:expr, $batch_size:expr) => {
        Ok(Box::new(PrimitiveArrayDecoder::<$t>::new(
            $data_type,
            $batch_size,
        )))
    };
}

fn make_decoder(
    data_type: DataType,
    batch_size: usize,
) -> Result<Box<dyn ArrayDecoder>, ArrowError> {
    downcast_integer! {
        data_type => (primitive_decoder, data_type, batch_size),
        DataType::Float32 => primitive_decoder!(Float32Type, data_type, batch_size),
        DataType::Float64 => primitive_decoder!(Float64Type, data_type, batch_size),
        DataType::Boolean => Ok(Box::new(BooleanArrayDecoder::new(batch_size))),
        DataType::Utf8 => Ok(Box::new(StringArrayDecoder::<i32>::new(batch_size))),
        DataType::LargeUtf8 => Ok(Box::new(StringArrayDecoder::<i64>::new(batch_size))),
        DataType::Struct(_) => Ok(Box::new(StructArrayDecoder::new(data_type, batch_size)?)),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            Err(ArrowError::JsonError(format!("{} is not supported by JSON", data_type)))
        }
        d => Err(ArrowError::NotYetImplemented(format!("Support for {} in JSON reader", d)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::{
        as_boolean_array, as_largestring_array, as_primitive_array, as_string_array,
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
        {"a": 2, "b": 4, "c": false}

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
        assert_eq!(col3.value(0), true);
        assert!(!col3.is_null(0));
        assert_eq!(col3.value(1), false);
        assert!(!col3.is_null(1));
    }

    #[test]
    fn test_string() {
        let buf = r#"
        {"a": "1", "b": "2"}
        {"a": "hello", "b": "shoo"}
        {"b": "\t😁foo", "a": "\nfoobar"}
        
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
        assert_eq!(col1.value(2), "\nfoobar");
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
}

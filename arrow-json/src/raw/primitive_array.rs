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

use num::NumCast;

use arrow_array::builder::PrimitiveBuilder;
use arrow_array::{Array, ArrowPrimitiveType};
use arrow_cast::parse::Parser;
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType};

use crate::raw::ArrayDecoder;

pub struct PrimitiveArrayDecoder<P: ArrowPrimitiveType> {
    builder: PrimitiveBuilder<P>,
}

impl<P: ArrowPrimitiveType> PrimitiveArrayDecoder<P> {
    pub fn new(data_type: DataType, batch_size: usize) -> Self {
        Self {
            builder: PrimitiveBuilder::with_capacity(batch_size)
                .with_data_type(data_type),
        }
    }
}

impl<P> ArrayDecoder for PrimitiveArrayDecoder<P>
where
    P: ArrowPrimitiveType + Parser,
    P::Native: NumCast,
{
    fn visit(&mut self, row: &str) -> Result<usize, ArrowError> {
        let trimmed = row.trim_start();
        let trimmed_bytes = row.len() - trimmed.len();

        match trimmed.as_bytes()[0] {
            b'"' => todo!(),
            b'n' if trimmed.starts_with("null") => {
                self.builder.append_null();
                return Ok(trimmed_bytes + 4);
            }
            _ => {
                let (s, _) = trimmed
                    .split_once(|c: char| !matches!(c, ']' | ',' | '}'))
                    .expect("todo");

                match P::parse(s) {
                    None => Err(ArrowError::JsonError(format!(
                        "expected primitive got {}",
                        s
                    ))),
                    Some(v) => {
                        self.builder.append_value(v);
                        Ok(trimmed_bytes + s.len())
                    }
                }
            }
        }
    }

    fn flush(&mut self) -> ArrayData {
        self.builder.finish().into_data()
    }
}

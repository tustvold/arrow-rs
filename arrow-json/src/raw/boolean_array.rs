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

use arrow_array::builder::BooleanBuilder;
use arrow_array::Array;
use arrow_data::ArrayData;
use arrow_schema::ArrowError;

use crate::raw::ArrayDecoder;

pub struct BooleanArrayDecoder {
    builder: BooleanBuilder,
}

impl BooleanArrayDecoder {
    pub fn new(batch_size: usize) -> Self {
        Self {
            builder: BooleanBuilder::with_capacity(batch_size),
        }
    }
}

impl ArrayDecoder for BooleanArrayDecoder {
    fn visit(&mut self, row: usize, row: &str) -> Result<usize, ArrowError> {
        let trimmed = row.trim_start();
        let trimmed_bytes = row.len() - trimmed.len();
        match row.as_bytes()[0] {
            b't' => {
                if trimmed.starts_with("true") {
                    self.builder.append_value(true);
                    return Ok(4 + trimmed_bytes);
                }
            }
            b'f' => {
                if trimmed.starts_with("false") {
                    self.builder.append_value(false);
                    return Ok(5 + trimmed_bytes);
                }
            }
            b'n' => {
                if trimmed.starts_with("null") {
                    self.builder.append_null();
                    return Ok(4 + trimmed_bytes);
                }
            }
            b'"' => todo!(),
            _ => {}
        }

        Err(ArrowError::JsonError(format!(
            "expected boolean got {}",
            row
        )))
    }

    fn flush(&mut self) -> ArrayData {
        self.builder.finish().into_data()
    }
}

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

use crate::datatypes::{DataType, Field, Schema};
use std::fmt::Debug;
use std::ops::Range;

#[derive(Debug, Copy, Clone)]
pub enum Type {
    B1,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
}

impl Type {
    pub fn cranelift_type(self) -> cranelift_codegen::ir::Type {
        use cranelift_codegen::ir::types;
        match self {
            Type::B1 => types::B1,
            Type::I8 => types::I8,
            Type::I16 => types::I16,
            Type::I32 => types::I32,
            Type::I64 => types::I64,
            Type::U8 => types::I8,
            Type::U16 => types::I16,
            Type::U32 => types::I32,
            Type::U64 => types::I64,
            Type::F32 => types::F32,
            Type::F64 => types::F64,
        }
    }
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::B1 => write!(f, "B1"),
            Type::I8 => write!(f, "I8"),
            Type::I16 => write!(f, "I16"),
            Type::I32 => write!(f, "I32"),
            Type::I64 => write!(f, "I64"),
            Type::U8 => write!(f, "U8"),
            Type::U16 => write!(f, "U16"),
            Type::U32 => write!(f, "U32"),
            Type::U64 => write!(f, "U64"),
            Type::F32 => write!(f, "F32"),
            Type::F64 => write!(f, "F64"),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum Value {
    Idx,
    Op(usize),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Idx => write!(f, "$idx"),
            Value::Op(idx) => write!(f, "${}", idx),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Buffer {
    /// The index in the buffer list
    pub index: usize,
    /// The type of the buffer
    pub ty: Type,
}

impl std::fmt::Display for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "$buffers.get<{}>({})", self.ty, self.index)
    }
}

#[derive(Debug)]
pub struct BufferValue {
    /// The buffer to load data from
    pub buffer: Buffer,
    /// The index within the buffer
    pub index: Value,
    /// A constant offset
    pub offset: usize,
}

impl std::fmt::Display for BufferValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.offset {
            0 => write!(f, "{}[{}]", self.buffer, self.index),
            _ => write!(f, "{}[{} + {}]", self.buffer, self.index, self.offset),
        }
    }
}

#[derive(Debug)]
pub struct BufferSlice {
    pub buffer: Buffer,

    pub range: Range<Value>,
}

impl std::fmt::Display for BufferSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "&buffers.get<{}>({})[{}..{}]",
            self.buffer.ty, self.buffer.index, self.range.start, self.range.end
        )
    }
}

#[derive(Debug)]
pub enum Op {
    /// Load data from the left buffer list into a variable
    LoadLeft(BufferValue),
    /// Load data from the right buffer list into a variable
    LoadRight(BufferValue),
    /// Compare buffers
    Compare(BufferValue, BufferValue),
    /// Compare two buffer slices
    CompareSlices(BufferSlice, BufferSlice),
    /// Compare two null buffers
    CompareNulls(BufferValue, BufferValue),
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::LoadLeft(b) => write!(f, "LoadLeft({})", b),
            Op::LoadRight(b) => write!(f, "LoadRight({})", b),
            Op::Compare(a, b) => write!(f, "Compare({}, {})", a, b),
            Op::CompareSlices(a, b) => write!(f, "CompareSlices({}, {})", a, b),
            Op::CompareNulls(a, b) => write!(f, "CompareNulls({}, {})", a, b),
        }
    }
}

#[derive(Debug)]
struct Builder {
    next_buffer: usize,
    operations: Vec<Op>,
}

impl Builder {
    fn new() -> Self {
        Self {
            next_buffer: 0,
            operations: vec![],
        }
    }

    fn visit_col(&mut self, field: &Field) {
        let is_nullable = field.is_nullable();

        match field.data_type() {
            DataType::Null => {} // Nothing to do
            DataType::Boolean => self.add_cmp_primitive(is_nullable, Type::B1),
            DataType::Int8 => self.add_cmp_primitive(is_nullable, Type::I8),
            DataType::Int16 => self.add_cmp_primitive(is_nullable, Type::I16),
            DataType::Int32 => self.add_cmp_primitive(is_nullable, Type::I32),
            DataType::Int64 => self.add_cmp_primitive(is_nullable, Type::I64),
            DataType::UInt8 => self.add_cmp_primitive(is_nullable, Type::U8),
            DataType::UInt16 => self.add_cmp_primitive(is_nullable, Type::U16),
            DataType::UInt32 => self.add_cmp_primitive(is_nullable, Type::U32),
            DataType::UInt64 => self.add_cmp_primitive(is_nullable, Type::U64),
            DataType::Float32 => self.add_cmp_primitive(is_nullable, Type::F32),
            DataType::Float64 => self.add_cmp_primitive(is_nullable, Type::F64),
            DataType::Timestamp(_, _) => self.add_cmp_primitive(is_nullable, Type::I64),
            DataType::Date32 => self.add_cmp_primitive(is_nullable, Type::I32),
            DataType::Date64 => self.add_cmp_primitive(is_nullable, Type::I64),
            DataType::Time32(_) => self.add_cmp_primitive(is_nullable, Type::I32),
            DataType::Time64(_) => self.add_cmp_primitive(is_nullable, Type::I64),
            DataType::Duration(_) => self.add_cmp_primitive(is_nullable, Type::I64),
            DataType::Binary => self.add_cmp_bytes(is_nullable, false),
            DataType::LargeBinary => self.add_cmp_bytes(is_nullable, true),
            DataType::Utf8 => self.add_cmp_bytes(is_nullable, false),
            DataType::LargeUtf8 => self.add_cmp_bytes(is_nullable, true),
            DataType::Dictionary(_, _) => todo!(),
            DataType::Struct(_) => todo!(),
            DataType::Float16
            | DataType::FixedSizeBinary(_)
            | DataType::List(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::Union(_, _, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Map(_, _)
            | DataType::Interval(_) => unimplemented!(
                "{} is not yet supported within comparators",
                field.data_type()
            ),
        }
    }

    fn push_op(&mut self, op: Op) -> Value {
        let idx = self.operations.len();
        self.operations.push(op);
        Value::Op(idx)
    }

    fn add_cmp_nulls(&mut self) {
        let buffer = Buffer {
            index: self.next_buffer,
            ty: Type::B1,
        };

        self.push_op(Op::CompareNulls(
            BufferValue {
                buffer,
                index: Value::Idx,
                offset: 0,
            },
            BufferValue {
                buffer,
                index: Value::Idx,
                offset: 0,
            },
        ));

        self.next_buffer += 1;
    }

    fn add_cmp_primitive(&mut self, nullable: bool, ty: Type) {
        if nullable {
            self.add_cmp_nulls()
        }

        let buffer = Buffer {
            index: self.next_buffer,
            ty,
        };
        self.next_buffer += 1;

        self.push_op(Op::Compare(
            BufferValue {
                buffer,
                index: Value::Idx,
                offset: 0,
            },
            BufferValue {
                buffer,
                index: Value::Idx,
                offset: 0,
            },
        ));
    }

    fn add_cmp_bytes(&mut self, nullable: bool, large: bool) {
        if nullable {
            self.add_cmp_nulls()
        }

        let ty = match large {
            true => Type::I64,
            false => Type::I32,
        };

        let offsets = Buffer {
            index: self.next_buffer,
            ty,
        };
        let values = Buffer {
            index: self.next_buffer + 1,
            ty: Type::U8,
        };

        self.next_buffer += 2;

        let a_start = self.push_op(Op::LoadLeft(BufferValue {
            buffer: offsets,
            index: Value::Idx,
            offset: 0,
        }));

        let a_end = self.push_op(Op::LoadLeft(BufferValue {
            buffer: offsets,
            index: Value::Idx,
            offset: 1,
        }));

        let b_start = self.push_op(Op::LoadRight(BufferValue {
            buffer: offsets,
            index: Value::Idx,
            offset: 0,
        }));

        let b_end = self.push_op(Op::LoadRight(BufferValue {
            buffer: offsets,
            index: Value::Idx,
            offset: 1,
        }));

        self.push_op(Op::CompareSlices(
            BufferSlice {
                buffer: values,
                range: a_start..a_end,
            },
            BufferSlice {
                buffer: values,
                range: b_start..b_end,
            },
        ));
    }

    fn build(self) -> ComparatorIr {
        ComparatorIr {
            operations: self.operations,
        }
    }
}

#[derive(Debug)]
pub struct ComparatorIr {
    pub operations: Vec<Op>,
}

impl std::fmt::Display for ComparatorIr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (idx, op) in self.operations.iter().enumerate() {
            writeln!(f, "{:04}: {}", idx, op)?
        }
        Ok(())
    }
}

pub fn generate_ir(schema: &Schema) -> ComparatorIr {
    let mut builder = Builder::new();
    for col in schema.fields() {
        builder.visit_col(col)
    }
    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::TimeUnit;

    #[test]
    fn test_ir() {
        let schema = Schema::new(vec![
            Field::new("i8", DataType::Int8, false),
            Field::new("i8", DataType::Int8, true),
            Field::new("i16_optional", DataType::Int16, false),
            Field::new("time", DataType::Time32(TimeUnit::Microsecond), true),
            Field::new("str", DataType::Utf8, false),
            Field::new("str_optional", DataType::LargeUtf8, true),
        ]);

        let ir = generate_ir(&schema);
        let ir_display = ir.to_string();
        let results: Vec<_> = ir_display.trim().split('\n').collect();

        let expected = &[
            "0000: Compare($buffers.get<I8>(0)[$idx], $buffers.get<I8>(0)[$idx])",
            "0001: CompareNulls($buffers.get<B1>(1)[$idx], $buffers.get<B1>(1)[$idx])",
            "0002: Compare($buffers.get<I8>(2)[$idx], $buffers.get<I8>(2)[$idx])",
            "0003: Compare($buffers.get<I16>(3)[$idx], $buffers.get<I16>(3)[$idx])",
            "0004: CompareNulls($buffers.get<B1>(4)[$idx], $buffers.get<B1>(4)[$idx])",
            "0005: Compare($buffers.get<I32>(5)[$idx], $buffers.get<I32>(5)[$idx])",
            "0006: LoadLeft($buffers.get<I32>(6)[$idx])",
            "0007: LoadLeft($buffers.get<I32>(6)[$idx + 1])",
            "0008: LoadRight($buffers.get<I32>(6)[$idx])",
            "0009: LoadRight($buffers.get<I32>(6)[$idx + 1])",
            "0010: CompareSlices(&buffers.get<U8>(7)[$6..$7], &buffers.get<U8>(7)[$8..$9])",
            "0011: CompareNulls($buffers.get<B1>(8)[$idx], $buffers.get<B1>(8)[$idx])",
            "0012: LoadLeft($buffers.get<I64>(9)[$idx])",
            "0013: LoadLeft($buffers.get<I64>(9)[$idx + 1])",
            "0014: LoadRight($buffers.get<I64>(9)[$idx])",
            "0015: LoadRight($buffers.get<I64>(9)[$idx + 1])",
            "0016: CompareSlices(&buffers.get<U8>(10)[$12..$13], &buffers.get<U8>(10)[$14..$15])",
        ];

        assert_eq!(&results, expected, "{:#?} vs {:#?}", &results, expected);
    }
}

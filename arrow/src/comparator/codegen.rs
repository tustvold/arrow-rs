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

use crate::comparator::ir::{
    generate_ir, BufferSlice, BufferValue, ComparatorIr, Op, Type, Value,
};
use crate::compute::eq;
use crate::datatypes::Schema;
use crate::error::Result;
use cranelift_codegen::cursor::{Cursor, CursorPosition, FuncCursor};
use cranelift_codegen::gimli::write::AttributeValue;
use cranelift_codegen::ir::{
    AbiParam, Block, InstBuilder, MemFlags, Type as CraneliftType,
    Value as CraneliftValue,
};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{FuncId, Module};

struct FunctionBuilder {
    ctx: cranelift_codegen::Context,

    /// Args:
    ///
    /// - row_left: usize
    /// - left_buffers: *const usize,
    /// - left_bit_offsets: *const u8,
    /// - right_left: usize
    /// - right_buffers: *const usize,
    /// - right_bit_offsets: *const u8,
    args: [CraneliftValue; 6],

    /// The current block
    block: Block,

    ptr_ty: CraneliftType,
}

impl FunctionBuilder {
    fn new(module: &JITModule, ret: CraneliftType) -> Self {
        let mut ctx = module.make_context();

        let ptr_ty = module.target_config().pointer_type();
        ctx.func.signature.returns = vec![AbiParam::new(ret)];
        ctx.func.signature.params = vec![AbiParam::new(ptr_ty); 6];

        // Setup first block
        let block = ctx.func.dfg.make_block();
        ctx.func.layout.append_block(block);

        let mut args = [CraneliftValue::with_number(0).unwrap(); 6];
        for val in &mut args {
            *val = ctx.func.dfg.append_block_param(block, ptr_ty);
        }

        Self {
            ctx,
            args,
            block,
            ptr_ty,
        }
    }

    fn resolve(&mut self, value: Value, right: bool) -> CraneliftValue {
        match value {
            Value::Idx => match right {
                true => self.args[3],
                false => self.args[0],
            },
            Value::Op(_) => todo!(),
        }
    }

    fn add_load(&mut self, val: &BufferValue, right: bool) -> CraneliftValue {
        let index = self.resolve(val.index, right);

        let mut cursor = FuncCursor::new(&mut self.ctx.func);
        cursor.set_position(CursorPosition::After(self.block));

        let cranelift_ty = val.buffer.ty.cranelift_type();
        let arg_buffer = match right {
            true => self.args[4],
            false => self.args[1],
        };

        match val.buffer.ty {
            Type::B1 => todo!(),
            _ => {
                let buffer_ptr_ptr = cursor.ins().load(
                    self.ptr_ty,
                    MemFlags::trusted(),
                    arg_buffer,
                    (val.buffer.index * self.ptr_ty.bytes() as usize) as i32,
                );

                let mut index_bytes =
                    cursor.ins().imul_imm(index, cranelift_ty.bytes() as i64);
                let val_ptr = cursor.ins().iadd(buffer_ptr_ptr, index_bytes);
                cursor.ins().load(
                    cranelift_ty,
                    MemFlags::trusted(),
                    val_ptr,
                    (val.offset * cranelift_ty.bytes() as usize) as i32,
                )
            }
        }
    }

    fn add_load_left(&mut self, val: &BufferValue) -> CraneliftValue {
        self.add_load(val, false)
    }

    fn add_load_right(&mut self, val: &BufferValue) -> CraneliftValue {
        self.add_load(val, true)
    }

    fn add_equal(&mut self, a: &BufferValue, b: &BufferValue) {
        // TODO
    }

    fn add_compare(&mut self, a: &BufferValue, b: &BufferValue) {
        // TODO
    }

    fn add_compare_slices(&mut self, a: &BufferSlice, b: &BufferSlice) {
        // TODO
    }

    fn add_equal_slices(&mut self, a: &BufferSlice, b: &BufferSlice) {
        // TODO
    }

    fn add_equal_nulls(&mut self, a: &BufferValue, b: &BufferValue) {
        // TODO
    }

    fn add_compare_nulls(&mut self, a: &BufferValue, b: &BufferValue) {
        // TODO
    }

    fn add_bconst_ret(&mut self, val: bool) {
        let mut cursor = self.cursor();
        let v = cursor.ins().bconst(cranelift_codegen::ir::types::B8, val);
        cursor.next_inst();
        cursor.ins().return_(&[v]);
    }

    fn add_iconst_ret(&mut self, val: i64) {
        let mut cursor = self.cursor();
        let v = cursor.ins().iconst(cranelift_codegen::ir::types::I8, val);
        cursor.next_inst();
        cursor.ins().return_(&[v]);
    }

    fn cursor(&mut self) -> FuncCursor<'_> {
        let mut cursor = FuncCursor::new(&mut self.ctx.func);
        cursor.set_position(CursorPosition::After(self.block));
        cursor
    }

    fn finalize(&mut self, module: &mut JITModule) -> FuncId {
        // TODO: Error handling
        let id = module
            .declare_anonymous_function(&self.ctx.func.signature)
            .unwrap();

        module.define_function(id, &mut self.ctx).unwrap();
        id
    }
}

pub(crate) struct CraneliftIr {
    module: JITModule,

    equal: FunctionBuilder,

    compare: FunctionBuilder,
}

impl CraneliftIr {
    pub fn new(ir: &ComparatorIr) -> Result<Self> {
        let jit_builder =
            JITBuilder::new(cranelift_module::default_libcall_names()).unwrap();
        let module = JITModule::new(jit_builder);

        let mut equal = FunctionBuilder::new(&module, cranelift_codegen::ir::types::B8);
        let mut compare = FunctionBuilder::new(&module, cranelift_codegen::ir::types::I8);

        for op in &ir.operations {
            match op {
                Op::LoadLeft(b) => {
                    equal.add_load_left(b);
                    compare.add_load_left(b);
                }
                Op::LoadRight(b) => {
                    equal.add_load_right(b);
                    compare.add_load_right(b);
                }
                Op::Compare(a, b) => {
                    equal.add_equal(a, b);
                    compare.add_compare(a, b);
                }
                Op::CompareSlices(a, b) => {
                    equal.add_equal_slices(a, b);
                    compare.add_compare_slices(a, b);
                }
                Op::CompareNulls(a, b) => {
                    equal.add_equal_nulls(a, b);
                    compare.add_compare_nulls(a, b);
                }
            }
        }

        equal.add_bconst_ret(true);
        compare.add_iconst_ret(0);

        Ok(Self {
            module,
            equal,
            compare,
        })
    }

    pub fn display_equal(&self) -> impl std::fmt::Display + '_ {
        self.equal.ctx.func.display()
    }

    pub fn display_compare(&self) -> impl std::fmt::Display + '_ {
        self.compare.ctx.func.display()
    }

    pub fn finalize_equal(&mut self) -> *const u8 {
        let id = self.equal.finalize(&mut self.module);
        self.module.get_finalized_function(id)
    }

    pub fn finalize_compare(&mut self) -> *const u8 {
        let id = self.compare.finalize(&mut self.module);
        self.module.get_finalized_function(id)
    }

    pub fn finalize(mut self) -> JITModule {
        self.module.finalize_definitions();
        self.module
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::{DataType, Field};

    fn assert_eq_str(actual: String, expected: &[&str]) {
        let a: Vec<_> = actual.trim().split('\n').collect();
        assert_eq!(a, expected, "{:#?} vs {:#?}", a, expected);
    }

    #[test]
    fn test_ir() {
        let schema = Schema::new(vec![
            Field::new("nullable_uint32", DataType::UInt32, true),
            Field::new("required_string", DataType::Utf8, false),
            Field::new("nullable_string", DataType::LargeUtf8, true),
        ]);

        let comparator_ir = generate_ir(&schema);
        assert_eq_str(comparator_ir.to_string(), &[
            "0000: CompareNulls($buffers.get<B1>(0)[$idx], $buffers.get<B1>(0)[$idx])",
            "0001: Compare($buffers.get<U32>(1)[$idx], $buffers.get<U32>(1)[$idx])",
            "0002: LoadLeft($buffers.get<I32>(2)[$idx])",
            "0003: LoadLeft($buffers.get<I32>(2)[$idx + 1])",
            "0004: LoadRight($buffers.get<I32>(2)[$idx])",
            "0005: LoadRight($buffers.get<I32>(2)[$idx + 1])",
            "0006: CompareSlices(&buffers.get<U8>(3)[$2..$3], &buffers.get<U8>(3)[$4..$5])",
            "0007: CompareNulls($buffers.get<B1>(4)[$idx], $buffers.get<B1>(4)[$idx])",
            "0008: LoadLeft($buffers.get<I64>(5)[$idx])",
            "0009: LoadLeft($buffers.get<I64>(5)[$idx + 1])",
            "0010: LoadRight($buffers.get<I64>(5)[$idx])",
            "0011: LoadRight($buffers.get<I64>(5)[$idx + 1])",
            "0012: CompareSlices(&buffers.get<U8>(6)[$8..$9], &buffers.get<U8>(6)[$10..$11])",
        ]);

        let cranelift_ir = CraneliftIr::new(&comparator_ir).unwrap();

        let display = cranelift_ir.display_equal().to_string();
        assert_eq_str(
            display,
            &[
                "function u0:0(i64, i64, i64, i64, i64, i64) -> b8 system_v {",
                "block0(v0: i64, v1: i64, v2: i64, v3: i64, v4: i64, v5: i64):",
                "    v6 = bconst.b8 true",
                "    return v6",
                "}",
            ],
        );

        let display = cranelift_ir.display_compare().to_string();
        assert_eq_str(
            display,
            &[
                "function u0:0(i64, i64, i64, i64, i64, i64) -> i8 system_v {",
                "block0(v0: i64, v1: i64, v2: i64, v3: i64, v4: i64, v5: i64):",
                "    v6 = iconst.i8 0",
                "    return v6",
                "}",
            ],
        );
    }
}

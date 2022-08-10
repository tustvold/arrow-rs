use crate::array::{Array, ArrayData};
use crate::datatypes::DataType;
use crate::record_batch::RecordBatch;

/// Encodes the various buffers into a flat array for easier indexing
///
/// Each buffer consists of
pub(crate) struct ComparatorArgs {
    buffers: Box<[usize]>,
    bit_offsets: Box<[u8]>,
}

impl ComparatorArgs {
    pub fn new(batch: &RecordBatch) -> Self {
        // Up to two buffers + null buffer per column
        // This will be an underestimate in the event of nested types, but is a good first guess
        let estimated_buf_count = 3 * 2 * batch.num_columns();

        let mut builder = ArgBuilder::with_capacity(estimated_buf_count);
        let schema = batch.schema();

        for (array, field) in batch.columns().iter().zip(schema.fields()) {
            builder.visit(array.data(), field.is_nullable());
        }
        builder.build()
    }

    pub fn buffers(&self) -> *const usize {
        self.buffers.as_ptr()
    }

    pub fn bit_offsets(&self) -> *const u8 {
        self.bit_offsets.as_ptr()
    }
}

impl std::fmt::Debug for ComparatorArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ComparatorArgs")
    }
}

struct ArgBuilder {
    buffers: Vec<usize>,
    bit_offsets: Vec<u8>,
}

impl ArgBuilder {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            buffers: Vec::with_capacity(capacity),
            bit_offsets: Vec::with_capacity(capacity),
        }
    }

    fn visit(&mut self, data: &ArrayData, nullable: bool) {
        if nullable {
            self.push_nulls(data)
        }

        match data.data_type() {
            DataType::Null => {} // Nothing to do
            DataType::Boolean => self.visit_bool(data),
            DataType::Int8 => self.visit_primitive(data, 1),
            DataType::Int16 => self.visit_primitive(data, 2),
            DataType::Int32 => self.visit_primitive(data, 4),
            DataType::Int64 => self.visit_primitive(data, 8),
            DataType::UInt8 => self.visit_primitive(data, 1),
            DataType::UInt16 => self.visit_primitive(data, 2),
            DataType::UInt32 => self.visit_primitive(data, 4),
            DataType::UInt64 => self.visit_primitive(data, 8),
            DataType::Float16 => self.visit_primitive(data, 2),
            DataType::Float32 => self.visit_primitive(data, 4),
            DataType::Float64 => self.visit_primitive(data, 8),
            DataType::Timestamp(_, _) => self.visit_primitive(data, 8),
            DataType::Date32 => self.visit_primitive(data, 4),
            DataType::Date64 => self.visit_primitive(data, 8),
            DataType::Time32(_) => self.visit_primitive(data, 4),
            DataType::Time64(_) => self.visit_primitive(data, 8),
            DataType::Binary => self.visit_bytes(data, false),
            DataType::LargeBinary => self.visit_bytes(data, true),
            DataType::Utf8 => self.visit_bytes(data, false),
            DataType::LargeUtf8 => self.visit_bytes(data, true),

            DataType::Struct(_) => todo!(),
            DataType::Dictionary(_, _) => todo!(),

            DataType::FixedSizeBinary(_)
            | DataType::List(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Union(_, _, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Map(_, _) => unimplemented!(
                "{} is not yet supported within comparators",
                data.data_type()
            ),
        }
    }

    fn push_nulls(&mut self, data: &ArrayData) {
        match data.null_buffer() {
            Some(null_buffer) => {
                let ptr = null_buffer.as_ptr() as usize;
                let bit_offset = (data.offset() % 8) as u8;
                let byte_offset = data.offset() / 8;
                self.bit_offsets.push(bit_offset);
                self.buffers.push(ptr + byte_offset);
            }
            None => {
                self.bit_offsets.push(0);
                self.buffers.push(0);
            }
        }
    }

    fn visit_bool(&mut self, data: &ArrayData) {
        let ptr = data.buffers()[0].as_ptr() as usize;
        let bit_offset = (data.offset() % 8) as u8;
        let byte_offset = data.offset() / 8;
        self.bit_offsets.push(bit_offset);
        self.buffers.push(ptr + byte_offset);
    }

    fn visit_primitive(&mut self, data: &ArrayData, bytes: usize) {
        let offset = data.offset() * bytes;
        let buffer = data.buffers()[0].as_ptr() as usize + offset;
        self.buffers.push(buffer);
        self.bit_offsets.push(0);
    }

    fn visit_bytes(&mut self, data: &ArrayData, is_large: bool) {
        let index_size = match is_large {
            true => 8,
            false => 4,
        };

        let offset = data.offset() * index_size;
        let offsets = data.buffers()[0].as_ptr() as usize + offset;
        let values = data.buffers()[1].as_ptr() as usize;

        self.buffers.push(offsets);
        self.buffers.push(values);
        self.bit_offsets.push(0);
        self.bit_offsets.push(0);
    }

    fn build(self) -> ComparatorArgs {
        ComparatorArgs {
            buffers: self.buffers.into(),
            bit_offsets: self.bit_offsets.into(),
        }
    }
}

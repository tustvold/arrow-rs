use std::marker::PhantomData;
use std::ops::Range;

use arrow::buffer::{Buffer, MutableBuffer};

pub trait RecordBuffer: Sized {
    type Output: Sized;

    type Writer: ?Sized;

    /// Split out `len` items
    fn split(&mut self, len: usize) -> Self::Output;

    /// Get a writer with `batch_size` capacity
    fn writer(&mut self, batch_size: usize) -> &mut Self::Writer;

    /// Record a write of `len` items
    fn commit(&mut self, len: usize);
}

pub struct TypedBuffer<T> {
    buffer: MutableBuffer,

    /// Length in elements of size T
    len: usize,

    /// Placeholder to allow `T` as an invariant generic parameter
    _phantom: PhantomData<*mut T>,
}

impl<T> Default for TypedBuffer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> TypedBuffer<T> {
    pub fn new() -> Self {
        Self {
            buffer: MutableBuffer::new(0),
            len: 0,
            _phantom: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn as_slice(&self) -> &[T] {
        let (prefix, buf, suffix) = unsafe { self.buffer.as_slice().align_to::<T>() };
        assert!(prefix.is_empty() && suffix.is_empty());
        buf
    }

    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut [T] {
        let (prefix, buf, suffix) =
            unsafe { self.buffer.as_slice_mut().align_to_mut::<T>() };
        assert!(prefix.is_empty() && suffix.is_empty());
        buf
    }
}

impl<T> RecordBuffer for TypedBuffer<T> {
    type Output = Buffer;

    type Writer = [T];

    fn split(&mut self, len: usize) -> Self::Output {
        assert!(len <= self.len);

        let num_bytes = len * std::mem::size_of::<T>();
        let remaining_bytes = self.buffer.len() - num_bytes;
        // TODO: Optimize to reduce the copy
        // create an empty buffer, as it will be resized below
        let mut remaining = MutableBuffer::new(0);
        remaining.resize(remaining_bytes, 0);

        let new_records = remaining.as_slice_mut();

        new_records[0..remaining_bytes]
            .copy_from_slice(&self.buffer.as_slice()[num_bytes..]);

        self.buffer.resize(num_bytes, 0);
        self.len -= len;

        std::mem::replace(&mut self.buffer, remaining).into()
    }

    fn writer(&mut self, batch_size: usize) -> &mut Self::Writer {
        self.buffer
            .resize((self.len + batch_size) * std::mem::size_of::<T>(), 0);

        let range = self.len..self.len + batch_size;
        &mut self.as_slice_mut()[range]
    }

    fn commit(&mut self, len: usize) {
        self.len = len;

        let new_bytes = self.len * std::mem::size_of::<T>();
        assert!(new_bytes <= self.buffer.len());
        self.buffer.resize(new_bytes, 0);
    }
}

impl TypedBuffer<i16> {
    /// Inspects the buffered repetition levels in `range` and returns the number of
    /// "complete" records along with the corresponding number of values
    ///
    /// A "complete" record is one where the buffer contains a subsequent repetition level of 0
    pub fn count_records(
        &self,
        range: Range<usize>,
        max_records: usize,
    ) -> (usize, usize) {
        let buf = self.as_slice();

        let start = range.start;
        let mut records_read = 0;
        let mut end_of_last_record = start;

        for current in range {
            if buf[current] == 0 && current != start {
                records_read += 1;
                end_of_last_record = current;

                if records_read == max_records {
                    break;
                }
            }
        }

        (records_read, end_of_last_record - start)
    }
}

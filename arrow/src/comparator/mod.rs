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

//! Multi-column comparator support

use crate::comparator::args::ComparatorArgs;
use crate::comparator::codegen::ComparatorImpl;
use crate::datatypes::SchemaRef;
use crate::error::Result;
use crate::record_batch::RecordBatch;
use std::cmp::Ordering;
use std::sync::Arc;

mod args;
mod codegen;
mod ir;

/// A factory that can be used to make [`BatchComparator`] for
/// [`RecordBatch`] with a given Schema
#[derive(Debug)]
pub struct ComparatorFactory {
    comparator: Arc<ComparatorImpl>,
}

impl ComparatorFactory {
    /// Creates a new [`RowAccessorFactory`] for [`RecordBatch`] with
    /// the provided `schema
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        let comparator = codegen::build_comparator(schema)?;
        Ok(Self {
            comparator: Arc::new(comparator),
        })
    }

    /// Get a [`BatchComparator`] for the provided [`RecordBatch`]
    pub fn make_comparator(&self, batch: RecordBatch) -> BatchComparator {
        assert!(self
            .comparator
            .is_schema_compatible(batch.schema().as_ref()));

        let args = ComparatorArgs::new(&batch);
        BatchComparator {
            batch,
            args,
            comparator: Arc::clone(&self.comparator),
        }
    }
}

/// A comparator for a given [`RecordBatch`]
#[derive(Debug)]
pub struct BatchComparator {
    batch: RecordBatch,
    args: ComparatorArgs,
    comparator: Arc<ComparatorImpl>,
}

impl BatchComparator {
    /// Returns the [`RowComparator`] for index `idx`
    pub fn row(&self, idx: usize) -> RowComparator<'_> {
        assert!(idx < self.batch.num_rows());
        RowComparator { batch: self, idx }
    }

    /// Returns the batch
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Unwraps this comparator into its inner [`RecordBatch`]
    pub fn into_inner(self) -> RecordBatch {
        self.batch
    }
}

/// A row within a given [`RecordBatch`]
#[derive(Debug)]
pub struct RowComparator<'a> {
    batch: &'a BatchComparator,
    idx: usize,
}

impl<'a> PartialEq for RowComparator<'a> {
    fn eq(&self, other: &Self) -> bool {
        assert!(Arc::ptr_eq(&self.batch.comparator, &other.batch.comparator));
        unsafe {
            self.batch.comparator.eq(
                &self.batch.args,
                self.idx,
                &other.batch.args,
                other.idx,
            )
        }
    }
}

impl<'a> Eq for RowComparator<'a> {}

impl<'a> PartialOrd for RowComparator<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for RowComparator<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        assert!(Arc::ptr_eq(&self.batch.comparator, &other.batch.comparator));
        unsafe {
            self.batch.comparator.cmp(
                &self.batch.args,
                self.idx,
                &other.batch.args,
                other.idx,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{ArrayRef, Float32Array, Int32Array};

    #[test]
    fn test() {
        let batch1 = RecordBatch::try_from_iter([
            (
                "int",
                Arc::new(Int32Array::from_iter([Some(1), Some(2), None, Some(5)]))
                    as ArrayRef,
            ),
            (
                "float",
                Arc::new(Float32Array::from_iter([Some(1.3), None, None, Some(5.)]))
                    as ArrayRef,
            ),
        ])
        .unwrap();

        let batch2 = RecordBatch::try_from_iter([
            (
                "int",
                Arc::new(Int32Array::from_iter([
                    None,
                    Some(1),
                    None,
                    None,
                    Some(5),
                    Some(1),
                ])) as ArrayRef,
            ),
            (
                "float",
                Arc::new(Float32Array::from_iter([
                    None,
                    Some(1.3),
                    None,
                    None,
                    Some(5.),
                    Some(4.),
                ])) as ArrayRef,
            ),
        ])
        .unwrap();

        let comparator = ComparatorFactory::try_new(batch1.schema()).unwrap();

        let a = comparator.make_comparator(batch1);
        let b = comparator.make_comparator(batch2);

        assert_eq!(a.row(0), b.row(1));
        assert_ne!(a.row(0), b.row(0));
        //TODO: Flesh out more tests
    }
}

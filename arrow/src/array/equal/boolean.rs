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

use crate::array::{data::contains_nulls, ArrayData};
use crate::util::bit_util::get_bit;

use super::utils::{equal_bits, equal_len};

/// Returns true if the value data for the arrays is equal, assuming the null masks have
/// already been checked for equality
pub(super) fn boolean_equal(
    lhs: &ArrayData,
    rhs: &ArrayData,
    mut lhs_start: usize,
    mut rhs_start: usize,
    mut len: usize,
) -> bool {
    let lhs_values = lhs.buffers()[0].as_slice();
    let rhs_values = rhs.buffers()[0].as_slice();

    let contains_nulls = contains_nulls(lhs.null_buffer(), lhs_start + lhs.offset(), len);

    if !contains_nulls {
        // Optimize performance for starting offset at u8 boundary.
        if lhs_start % 8 == 0
            && rhs_start % 8 == 0
            && lhs.offset() % 8 == 0
            && rhs.offset() % 8 == 0
        {
            let quot = len / 8;
            if quot > 0
                && !equal_len(
                    lhs_values,
                    rhs_values,
                    lhs_start / 8 + lhs.offset() / 8,
                    rhs_start / 8 + rhs.offset() / 8,
                    quot,
                )
            {
                return false;
            }

            // Calculate for suffix bits.
            let rem = len % 8;
            if rem == 0 {
                return true;
            } else {
                let aligned_bits = len - rem;
                lhs_start += aligned_bits;
                rhs_start += aligned_bits;
                len = rem
            }
        }

        equal_bits(
            lhs_values,
            rhs_values,
            lhs_start + lhs.offset(),
            rhs_start + rhs.offset(),
            len,
        )
    } else {
        // get a ref of the null buffer bytes, to use in testing for nullness
        let lhs_null_bytes = lhs.null_buffer().as_ref().unwrap().as_slice();

        let lhs_start = lhs.offset() + lhs_start;
        let rhs_start = rhs.offset() + rhs_start;

        (0..len).all(|i| {
            let lhs_pos = lhs_start + i;
            let rhs_pos = rhs_start + i;
            let lhs_is_null = !get_bit(lhs_null_bytes, lhs_pos);

            lhs_is_null || get_bit(lhs_values, lhs_pos) == get_bit(rhs_values, rhs_pos)
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{Array, BooleanArray};

    #[test]
    fn test_boolean_slice() {
        let array = BooleanArray::from(vec![true; 32]);
        let slice = array.slice(4, 12);
        assert_eq!(slice.data(), slice.data());

        let slice = array.slice(8, 12);
        assert_eq!(slice.data(), slice.data());

        let slice = array.slice(8, 24);
        assert_eq!(slice.data(), slice.data());
    }

    #[test]
    fn test_sliced_nullable_boolean_array() {
        let a = BooleanArray::from(vec![None; 32]);
        let b = BooleanArray::from(vec![true; 32]);
        let slice_a = a.slice(1, 12);
        let slice_b = b.slice(1, 12);
        assert_ne!(slice_a.data(), slice_b.data());
    }
}

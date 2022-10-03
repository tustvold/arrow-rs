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

use num::BigInt;
use std::cmp::Ordering;

/// A signed 256-bit integer
#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Default, Eq, PartialEq, Hash)]
pub struct i256 {
    low: u128,
    high: i128,
}

impl std::fmt::Debug for i256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::fmt::Display for i256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", BigInt::from_signed_bytes_le(&self.to_le_bytes()))
    }
}

impl PartialOrd for i256 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for i256 {
    fn cmp(&self, other: &Self) -> Ordering {
        // This is 25x faster than using a variable length encoding such
        // as BigInt as it avoids allocation and branching
        self.high.cmp(&other.high).then(self.low.cmp(&other.low))
    }
}

impl i256 {
    /// The additive identity for this integer type, i.e. `0`.
    pub const ZERO: Self = i256 { low: 0, high: 0 };

    /// The multiplicative identity for this integer type, i.e. `1`.
    pub const ONE: Self = i256 { low: 1, high: 0 };

    /// The multiplicative inverse for this integer type, i.e. `-1`.
    pub const MINUS_ONE: Self = i256 {
        low: u128::MAX,
        high: -1,
    };

    /// Create an integer value from its representation as a byte array in little-endian.
    #[inline]
    pub fn from_le_bytes(b: [u8; 32]) -> Self {
        Self {
            high: i128::from_le_bytes(b[16..32].try_into().unwrap()),
            low: u128::from_le_bytes(b[0..16].try_into().unwrap()),
        }
    }

    /// Create an i256 from the provided low u128 and high i128
    #[inline]
    pub fn from_parts(low: u128, high: i128) -> Self {
        Self { low, high }
    }

    /// Returns this `i256` as a low u128 and high i128
    pub fn to_parts(self) -> (u128, i128) {
        (self.low, self.high)
    }

    /// Converts this `i256` into an `i128` returning `None` if this would result
    /// in truncation/overflow
    pub fn to_i128(self) -> Option<i128> {
        let as_i128 = self.low as i128;

        let high_negative = self.high < 0;
        let low_negative = as_i128 < 0;
        let high_valid = self.high == -1 || self.high == 0;

        (high_negative == low_negative && high_valid).then_some(self.low as i128)
    }

    /// Return the memory representation of this integer as a byte array in little-endian byte order.
    #[inline]
    pub fn to_le_bytes(self) -> [u8; 32] {
        let mut t = [0; 32];
        let t_low: &mut [u8; 16] = (&mut t[0..16]).try_into().unwrap();
        *t_low = self.low.to_le_bytes();
        let t_high: &mut [u8; 16] = (&mut t[16..32]).try_into().unwrap();
        *t_high = self.high.to_le_bytes();
        t
    }

    /// Create an i256 from the provided [`BigInt`] returning a bool indicating
    /// if overflow occurred
    fn from_bigint_with_overflow(v: BigInt) -> (Self, bool) {
        let v_bytes = v.to_signed_bytes_le();
        match v_bytes.len().cmp(&32) {
            Ordering::Less => {
                let mut bytes = if num::Signed::is_negative(&v) {
                    [255_u8; 32]
                } else {
                    [0; 32]
                };
                bytes[0..v_bytes.len()].copy_from_slice(&v_bytes[..v_bytes.len()]);
                (Self::from_le_bytes(bytes), false)
            }
            Ordering::Equal => (Self::from_le_bytes(v_bytes.try_into().unwrap()), false),
            Ordering::Greater => {
                (Self::from_le_bytes(v_bytes[..32].try_into().unwrap()), true)
            }
        }
    }

    /// Performs wrapping addition
    #[inline]
    pub fn wrapping_add(self, other: Self) -> Self {
        let (low, carry) = self.low.overflowing_add(other.low);
        let high = self.high.wrapping_add(other.high).wrapping_add(carry as _);
        Self { low, high }
    }

    /// Performs checked addition
    #[inline]
    pub fn checked_add(self, other: Self) -> Option<Self> {
        let (low, carry) = self.low.overflowing_add(other.low);
        let high = self.high.checked_add(other.high)?.checked_add(carry as _)?;
        Some(Self { low, high })
    }

    /// Performs wrapping subtraction
    #[inline]
    pub fn wrapping_sub(self, other: Self) -> Self {
        let (low, carry) = self.low.overflowing_sub(other.low);
        let high = self.high.wrapping_sub(other.high).wrapping_sub(carry as _);
        Self { low, high }
    }

    /// Performs checked subtraction
    #[inline]
    pub fn checked_sub(self, other: Self) -> Option<Self> {
        let (low, carry) = self.low.overflowing_sub(other.low);
        let high = self.high.checked_sub(other.high)?.checked_sub(carry as _)?;
        Some(Self { low, high })
    }

    /// Performs wrapping multiplication
    #[inline]
    pub fn wrapping_mul(self, other: Self) -> Self {
        let (low, high) = mulx(self.low, other.low);

        // Compute the high multiples, only impacting the high 128-bits
        let hl = self.high.wrapping_mul(other.low as i128);
        let lh = (self.low as i128).wrapping_mul(other.high);

        Self {
            low,
            high: (high as i128).wrapping_add(hl).wrapping_add(lh),
        }
    }

    /// Performs checked multiplication
    #[inline]
    pub fn checked_mul(self, other: Self) -> Option<Self> {
        let (low, high) = mulx(self.low, other.low);

        // Compute the high multiples, only impacting the high 128-bits
        let hl = self.high.checked_mul(other.low as i128)?;
        let lh = (self.low as i128).checked_mul(other.high)?;

        Some(Self {
            low,
            high: (high as i128).checked_add(hl)?.checked_add(lh)?,
        })
    }

    /// Performs wrapping division
    #[inline]
    pub fn wrapping_div(self, other: Self) -> Self {
        let l = BigInt::from_signed_bytes_le(&self.to_le_bytes());
        let r = BigInt::from_signed_bytes_le(&other.to_le_bytes());
        Self::from_bigint_with_overflow(l / r).0
    }

    /// Performs checked division
    #[inline]
    pub fn checked_div(self, other: Self) -> Option<Self> {
        let l = BigInt::from_signed_bytes_le(&self.to_le_bytes());
        let r = BigInt::from_signed_bytes_le(&other.to_le_bytes());
        let (val, overflow) = Self::from_bigint_with_overflow(l / r);
        (!overflow).then(|| val)
    }

    /// Performs wrapping remainder
    #[inline]
    pub fn wrapping_rem(self, other: Self) -> Self {
        let l = BigInt::from_signed_bytes_le(&self.to_le_bytes());
        let r = BigInt::from_signed_bytes_le(&other.to_le_bytes());
        Self::from_bigint_with_overflow(l % r).0
    }

    /// Performs checked remainder
    #[inline]
    pub fn checked_rem(self, other: Self) -> Option<Self> {
        if other == Self::ZERO {
            return None;
        }

        let l = BigInt::from_signed_bytes_le(&self.to_le_bytes());
        let r = BigInt::from_signed_bytes_le(&other.to_le_bytes());
        let (val, overflow) = Self::from_bigint_with_overflow(l % r);
        (!overflow).then(|| val)
    }
}

/// Performs an unsigned multiplication of `a * b` returning a tuple of
/// `(low, high)` where `low` contains the lower 128-bits of the result
/// and `high` the higher 128-bits
///
/// This mirrors the x86 mulx instruction but for 128-bit types
#[inline]
fn mulx(a: u128, b: u128) -> (u128, u128) {
    let split = |a: u128| (a & (u64::MAX as u128), a >> 64);

    const MASK: u128 = u64::MAX as _;

    let (a_low, a_high) = split(a);
    let (b_low, b_high) = split(b);

    // Carry stores the upper 64-bits of low and lower 64-bits of high
    let (mut low, mut carry) = split(a_low * b_low);
    carry += a_high * b_low;

    // Update low and high with corresponding parts of carry
    low += carry << 64;
    let mut high = carry >> 64;

    // Update carry with overflow from low
    carry = low >> 64;
    low &= MASK;

    // Perform multiply including overflow from low
    carry += b_high * a_low;

    // Update low and high with values from carry
    low += carry << 64;
    high += carry >> 64;

    // Perform 4th multiplication
    high += a_high * b_high;

    (low, high)
}

#[cfg(test)]
mod tests {
    use super::*;
    use num::{BigInt, FromPrimitive, ToPrimitive};
    use rand::{thread_rng, Rng};

    #[test]
    fn test_signed_cmp() {
        let a = i256::from_parts(i128::MAX as u128, 12);
        let b = i256::from_parts(i128::MIN as u128, 12);
        assert!(a < b);

        let a = i256::from_parts(i128::MAX as u128, 12);
        let b = i256::from_parts(i128::MIN as u128, -12);
        assert!(a > b);
    }

    #[test]
    fn test_to_i128() {
        let vals = [
            BigInt::from_i128(-1).unwrap(),
            BigInt::from_i128(i128::MAX).unwrap(),
            BigInt::from_i128(i128::MIN).unwrap(),
            BigInt::from_u128(u128::MIN).unwrap(),
            BigInt::from_u128(u128::MAX).unwrap(),
        ];

        for v in vals {
            let (t, overflow) = i256::from_bigint_with_overflow(v.clone());
            assert!(!overflow);
            assert_eq!(t.to_i128(), v.to_i128(), "{} vs {}", v, t);
        }
    }

    #[test]
    fn test_i256() {
        let mut rng = thread_rng();

        for _ in 0..1000 {
            let mut l = [0_u8; 32];
            let len = rng.gen_range(0..32);
            l.iter_mut().take(len).for_each(|x| *x = rng.gen());

            let mut r = [0_u8; 32];
            let len = rng.gen_range(0..32);
            r.iter_mut().take(len).for_each(|x| *x = rng.gen());

            let il = i256::from_le_bytes(l);
            let ir = i256::from_le_bytes(r);

            let bl = BigInt::from_signed_bytes_le(&l);
            let br = BigInt::from_signed_bytes_le(&r);

            // Comparison
            assert_eq!(il.cmp(&ir), bl.cmp(&br), "{} cmp {}", bl, br);

            // To i128
            assert_eq!(il.to_i128(), bl.to_i128(), "{}", bl);
            assert_eq!(ir.to_i128(), br.to_i128(), "{}", br);

            // Addition
            let actual = il.wrapping_add(ir);
            let (expected, overflow) =
                i256::from_bigint_with_overflow(bl.clone() + br.clone());
            assert_eq!(actual, expected);

            let checked = il.checked_add(ir);
            match overflow {
                true => assert!(checked.is_none()),
                false => assert_eq!(checked.unwrap(), actual),
            }

            // Subtraction
            let actual = il.wrapping_sub(ir);
            let (expected, overflow) =
                i256::from_bigint_with_overflow(bl.clone() - br.clone());
            assert_eq!(actual.to_string(), expected.to_string());

            let checked = il.checked_sub(ir);
            match overflow {
                true => assert!(checked.is_none()),
                false => assert_eq!(checked.unwrap(), actual),
            }

            // Multiplication
            let actual = il.wrapping_mul(ir);
            let (expected, overflow) =
                i256::from_bigint_with_overflow(bl.clone() * br.clone());
            assert_eq!(actual.to_string(), expected.to_string());

            let checked = il.checked_mul(ir);
            match overflow {
                true => assert!(checked.is_none()),
                false => assert_eq!(checked.unwrap(), actual),
            }
        }
    }
}

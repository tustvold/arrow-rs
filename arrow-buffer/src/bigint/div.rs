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

/// Unsigned division with remainder
///
/// # Panics
///
/// Panics if divisor is zero
pub fn div_rem<const N: usize>(
    numerator: &[u64; N],
    divisor: &[u64; N],
) -> ([u64; N], [u64; N]) {
    let numerator_bits = bits(numerator);
    let divisor_bits = bits(divisor);
    assert_ne!(divisor_bits, 0, "division by zero");

    if numerator_bits < divisor_bits {
        return ([0; N], *numerator);
    }

    if divisor_bits <= 64 {
        return div_rem_small(numerator, divisor[0]);
    }

    let numerator_words = (numerator_bits + 63) / 64;
    let divisor_words = (divisor_bits + 63) / 64;
    let n = divisor_words;
    let m = numerator_words - divisor_words;

    div_rem_knuth(numerator, divisor, n, m)
}

/// Return the least number of bits needed to represent the number
fn bits(arr: &[u64]) -> usize {
    for (idx, v) in arr.iter().enumerate().rev() {
        if *v > 0 {
            return 64 - v.leading_zeros() as usize + 64 * idx;
        }
    }
    0
}

fn div_rem_small<const N: usize>(
    numerator: &[u64; N],
    divisor: u64,
) -> ([u64; N], [u64; N]) {
    let mut rem = 0u64;
    let mut numerator = *numerator;
    numerator.iter_mut().rev().for_each(|d| {
        let (q, r) = div_rem_word(rem, *d, divisor);
        *d = q;
        rem = r;
    });

    let mut rem_padded = [0; N];
    rem_padded[0] = rem;
    (numerator, rem_padded)
}

fn div_rem_knuth<const N: usize>(
    numerator: &[u64; N],
    divisor: &[u64; N],
    n: usize,
    m: usize,
) -> ([u64; N], [u64; N]) {
    assert!(n + m <= N);

    let shift = divisor[n - 1].leading_zeros();
    let divisor = shl_word(divisor, shift);
    let mut u = full_shl(numerator, shift);

    let mut q = [0; N];
    let v_n_1 = divisor[n - 1];
    let v_n_2 = divisor[n - 2];

    for j in (0..=m).rev() {
        let u_jn = u[j + n];

        let mut q_hat = if u_jn < v_n_1 {
            let (mut q_hat, mut r_hat) = div_rem_word(u_jn, u[j + n - 1], v_n_1);

            loop {
                let (lo, hi) = split_u128(u128::from(q_hat) * u128::from(v_n_2));
                if (hi, lo) <= (r_hat, u[j + n - 2]) {
                    break;
                }

                q_hat -= 1;
                let (new_r_hat, overflow) = r_hat.overflowing_add(v_n_1);
                r_hat = new_r_hat;

                if overflow {
                    break;
                }
            }
            q_hat
        } else {
            u64::MAX
        };

        let q_hat_v = full_mul_u64(&divisor, q_hat);

        let c = sub_slice(&mut u[j..], &q_hat_v[..n + 1]);

        if c {
            q_hat -= 1;

            let c = add_slice(&mut u[j..], &divisor[..n]);
            u[j + n] = u[j + n].wrapping_add(u64::from(c));
        }

        q[j] = q_hat;
    }

    let remainder = full_shr(&u, shift);
    (q, remainder)
}

fn div_rem_word(hi: u64, lo: u64, y: u64) -> (u64, u64) {
    debug_assert!(hi < y);
    let x = (u128::from(hi) << 64) + u128::from(lo);
    let y = u128::from(y);
    ((x / y) as u64, (x % y) as u64)
}

fn add_slice(a: &mut [u64], b: &[u64]) -> bool {
    binop_slice(a, b, u64::overflowing_add)
}

#[inline(always)]
fn sub_slice(a: &mut [u64], b: &[u64]) -> bool {
    binop_slice(a, b, u64::overflowing_sub)
}

fn binop_slice(
    a: &mut [u64],
    b: &[u64],
    binop: impl Fn(u64, u64) -> (u64, bool) + Copy,
) -> bool {
    let mut c = false;
    a.iter_mut().zip(b.iter()).for_each(|(x, y)| {
        let (res, carry) = binop_carry(*x, *y, c, binop);
        *x = res;
        c = carry;
    });
    c
}

fn binop_carry(
    a: u64,
    b: u64,
    c: bool,
    binop: impl Fn(u64, u64) -> (u64, bool),
) -> (u64, bool) {
    let (res1, overflow1) = b.overflowing_add(u64::from(c));
    let (res2, overflow2) = binop(a, res1);
    (res2, overflow1 || overflow2)
}

const fn split_u128(a: u128) -> (u64, u64) {
    (a as u64, (a >> 64) as _)
}

fn full_mul_u64<const N: usize>(a: &[u64; N], b: u64) -> ArrayPlusOne<u64, N> {
    let mut carry = 0;
    let mut out = [0; N];
    out.iter_mut().zip(a).for_each(|(o, v)| {
        let (res, c) = split_u128(*v as u128 * b as u128 + carry as u128);
        *o = res;
        carry = c;
    });
    ArrayPlusOne(out, carry)
}

fn shl_word<const N: usize>(v: &[u64; N], shift: u32) -> [u64; N] {
    full_shl(v, shift).0
}

fn full_shl<const N: usize>(v: &[u64; N], shift: u32) -> ArrayPlusOne<u64, N> {
    debug_assert!(shift < 64);
    // out[i] =  v[i - 1] >> (64 - shift) | v[i] << shift
    if shift == 0 {
        return ArrayPlusOne(*v, 0);
    }
    let mut out = [0u64; N];

    out.iter_mut().zip(v).for_each(|(o, a)| *o = *a << shift);
    out.iter_mut()
        .skip(1)
        .zip(v)
        .for_each(|(o, a)| *o |= *a >> (64 - shift));
    let carry = v[N - 1] >> (64 - shift);
    return ArrayPlusOne(out, carry);
}

fn full_shr<const N: usize>(a: &ArrayPlusOne<u64, N>, shift: u32) -> [u64; N] {
    debug_assert!(shift < 64);
    if shift == 0 {
        return a.0;
    }
    let mut res = [0; N];
    res.iter_mut()
        .zip(a.iter())
        .for_each(|(o, a)| *o = *a >> shift);
    res.iter_mut()
        .zip(a.iter().skip(1))
        .for_each(|(o, a)| *o |= *a << (64 - shift));
    res
}

/// An array of N + 1 elements
///
/// This is a hack around lack of support for const arithmetic
struct ArrayPlusOne<T, const N: usize>([T; N], T);

impl<T, const N: usize> std::ops::Deref for ArrayPlusOne<T, N> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        let x = self as *const Self;
        unsafe { std::slice::from_raw_parts(x as *const T, N + 1) }
    }
}

impl<T, const N: usize> std::ops::DerefMut for ArrayPlusOne<T, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let x = self as *mut Self;
        unsafe { std::slice::from_raw_parts_mut(x as *mut T, N + 1) }
    }
}

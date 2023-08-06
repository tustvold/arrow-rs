use std::cmp::Ordering;
use std::sync::Arc;
use arrow_array::{Array, ArrayRef, ArrowNativeTypeOp, PrimitiveArray};
use arrow_array::cast::AsArray;
use arrow_array::types::DecimalType;
use arrow_buffer::{ArrowNativeType, i256};
use arrow_schema::{ArrowError, DataType};
use crate::numeric::{adjust_scale, Op, try_op, try_binary};

pub(crate) trait WideArithmetic: Sized {
    fn widening_mul(a: Self, b: Self) -> (Self, Self);

    fn add_wide(a: (Self, Self), b: (Self, Self)) -> Result<(Self, Self), ArrowError>;

    fn sub_wide(a: (Self, Self), b: (Self, Self)) -> Result<(Self, Self), ArrowError>;

    fn narrowing_div_rem(
        a: (Self, Self),
        div: (Self, Self),
    ) -> Result<(Self, Self), ArrowError>;
}
impl WideArithmetic for i128 {
    fn widening_mul(a: Self, b: Self) -> (Self, Self) {
        todo!()
    }

    fn add_wide(a: (Self, Self), b: (Self, Self)) -> Result<(Self, Self), ArrowError> {
        todo!()
    }

    fn sub_wide(a: (Self, Self), b: (Self, Self)) -> Result<(Self, Self), ArrowError> {
        todo!()
    }

    fn narrowing_div_rem(
        a: (Self, Self),
        div: (Self, Self),
    ) -> Result<(Self, Self), ArrowError> {
        todo!()
    }
}

impl WideArithmetic for i256 {
    fn widening_mul(a: Self, b: Self) -> (Self, Self) {
        todo!()
    }

    fn add_wide(a: (Self, Self), b: (Self, Self)) -> Result<(Self, Self), ArrowError> {
        todo!()
    }

    fn sub_wide(a: (Self, Self), b: (Self, Self)) -> Result<(Self, Self), ArrowError> {
        todo!()
    }

    fn narrowing_div_rem(
        a: (Self, Self),
        div: (Self, Self),
    ) -> Result<(Self, Self), ArrowError> {
        todo!()
    }
}

/// Perform arithmetic operation on decimal arrays
pub(crate) fn decimal_op<T: DecimalType>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError>
    where
        T::Native: WideArithmetic,
{
    let l = l.as_primitive::<T>();
    let r = r.as_primitive::<T>();

    let (p1, s1, p2, s2) = match (l.data_type(), r.data_type()) {
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            (*p1 as i32, *s1 as i32, *p2 as i32, *s2 as i32)
        }
        (DataType::Decimal256(p1, s1), DataType::Decimal256(p2, s2)) => {
            (*p1 as i32, *s1 as i32, *p2 as i32, *s2 as i32)
        }
        _ => unreachable!(),
    };

    // Follow the Hive decimal arithmetic rules
    // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
    // And the Calcite rules
    // https://github.com/apache/arrow/blob/36ddbb531cac9b9e512dfa3776d1d64db588209f/java/gandiva/src/main/java/org/apache/arrow/gandiva/evaluator/DecimalTypeUtil.java#L46
    let array: PrimitiveArray<T> = match op {
        Op::Add | Op::AddWrapping | Op::Sub | Op::SubWrapping => {
            // max(s1, s2)
            let rs = s1.max(s2);

            // max(s1, s2) + max(p1-s1, p2-s2) + 1
            let rp = rs + (p1 - s1).max(p2 - s2) + 1;
            let (p, s) = adjust_scale::<T>(rp, rs);

            let l_mul = T::Native::usize_as(10).pow_checked((rs - s1) as _)?;
            let r_mul = T::Native::usize_as(10).pow_checked((rs - s2) as _)?;

            if s == rs {
                match op {
                    Op::Add | Op::AddWrapping => {
                        try_op!(
                            l,
                            l_s,
                            r,
                            r_s,
                            l.mul_checked(l_mul)?.add_checked(r.mul_checked(r_mul)?)
                        )
                    }
                    Op::Sub | Op::SubWrapping => {
                        try_op!(
                            l,
                            l_s,
                            r,
                            r_s,
                            l.mul_checked(l_mul)?.sub_checked(r.mul_checked(r_mul)?)
                        )
                    }
                    _ => unreachable!(),
                }
            } else {
                let div = T::Native::usize_as(10).pow_checked((rs - s) as _)?;
                match op {
                    Op::Add | Op::AddWrapping => {
                        try_op!(l, l_s, r, r_s, {
                            let (l_lo, l_hi) = T::Native::widening_mul(l, l_mul);
                            let (r_lo, r_hi) = T::Native::widening_mul(r, r_mul);
                            let (lo, hi) =
                                T::Native::add_wide((l_lo, l_hi), (r_lo, r_hi))?;
                            T::Native::narrowing_div_rem((lo, hi), (T::Native::ZERO, div))
                                .map(|x| x.0)
                        })
                    }
                    Op::Sub | Op::SubWrapping => {
                        try_op!(l, l_s, r, r_s, {
                            let (l_lo, l_hi) = T::Native::widening_mul(l, l_mul);
                            let (r_lo, r_hi) = T::Native::widening_mul(r, r_mul);
                            let (lo, hi) =
                                T::Native::sub_wide((l_lo, l_hi), (r_lo, r_hi))?;
                            T::Native::narrowing_div_rem((lo, hi), (T::Native::ZERO, div))
                                .map(|x| x.0)
                        })
                    }
                    _ => unreachable!(),
                }
            }
                .with_precision_and_scale(p, s as _)?
        }
        Op::Mul | Op::MulWrapping => {
            let rp = p1 + p2 + 1;
            let rs = s1 + s2;
            let (p, s) = adjust_scale::<T>(rp, rs);
            if s == rs {
                try_op!(l, l_s, r, r_s, l.mul_checked(r))
            } else {
                let div = T::Native::usize_as(10).pow_checked((rs - s) as _)?;
                try_op!(l, l_s, r, r_s, {
                    let (lo, hi) = T::Native::widening_mul(l, r);
                    T::Native::narrowing_div_rem((lo, hi), (T::Native::ZERO, div))
                        .map(|x| x.0)
                })
            }
                .with_precision_and_scale(p, s as _)?
        }

        Op::Div => {
            // max(6, s1 + p2 + 1)
            let rs = 6.max(s1 + p2 + 1);

            // p1 - s1 + s2 + rs
            let rp = p1 - s1 + s2 + rs;
            let (p, s) = adjust_scale::<T>(rp, rs);

            let mul_pow = s - s1 + s2;

            let (l_mul, r_mul) = match mul_pow.cmp(&0) {
                Ordering::Greater => (
                    T::Native::usize_as(10).pow_checked(mul_pow as _)?,
                    T::Native::ONE,
                ),
                Ordering::Equal => (T::Native::ONE, T::Native::ONE),
                Ordering::Less => (
                    T::Native::ONE,
                    T::Native::usize_as(10).pow_checked(mul_pow as _)?,
                ),
            };

            try_op!(l, l_s, r, r_s, {
                let (l_lo, l_hi) = T::Native::widening_mul(l_mul, l);
                let (r_lo, r_hi) = T::Native::widening_mul(r_mul, r);
                T::Native::narrowing_div_rem((l_lo, l_hi), (r_lo, r_hi)).map(|x| x.0)
            })
                .with_precision_and_scale(p, s as _)?
        }

        Op::Rem => {
            // max(s1, s2)
            let rs = s1.max(s2);
            // min(p1-s1, p2 -s2) + max( s1,s2 )
            let rp = rs + (p1 - s1) + (p2 - s2);

            let l_mul = T::Native::usize_as(10).pow_wrapping((rs - s1) as _);
            let r_mul = T::Native::usize_as(10).pow_wrapping((rs - s2) as _);

            let (p, s) = adjust_scale::<T>(rp, rs);
            try_op!(l, l_s, r, r_s, {
                let (l_lo, l_hi) = T::Native::widening_mul(l_mul, l);
                let (r_lo, r_hi) = T::Native::widening_mul(r_mul, r);
                T::Native::narrowing_div_rem((l_lo, l_hi), (r_lo, r_hi)).map(|x| x.1)
            })
                .with_precision_and_scale(p, s as _)?
        }
    };

    Ok(Arc::new(array))
}

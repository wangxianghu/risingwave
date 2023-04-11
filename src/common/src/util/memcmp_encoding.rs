// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::{Buf, BufMut};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::iter_util::{ZipEqDebug, ZipEqFast};
use crate::array::serial_array::Serial;
use crate::array::{ArrayImpl, DataChunk};
use crate::row::Row;
use crate::types::num256::Int256;
use crate::types::{DataType, Date, Datum, ScalarImpl, Time, Timestamp, ToDatumRef, F32, F64};
use crate::util::sort_util::{ColumnOrder, OrderType};

// NULL > any non-NULL value by default
const DEFAULT_NULL_TAG_NONE: u8 = 1;
const DEFAULT_NULL_TAG_SOME: u8 = 0;

pub(crate) fn serialize_datum(
    datum: impl ToDatumRef,
    order: OrderType,
    serializer: &mut memcomparable::Serializer<impl BufMut>,
) -> memcomparable::Result<()> {
    serializer.set_reverse(order.is_descending());
    let (null_tag_none, null_tag_some) = if order.nulls_are_largest() {
        (1u8, 0u8) // None > Some
    } else {
        (0u8, 1u8) // None < Some
    };
    if let Some(scalar) = datum.to_datum_ref() {
        null_tag_some.serialize(&mut *serializer)?;
        scalar.serialize(serializer)?;
    } else {
        null_tag_none.serialize(serializer)?;
    }
    Ok(())
}

pub(crate) fn serialize_datum_in_composite(
    datum: impl ToDatumRef,
    serializer: &mut memcomparable::Serializer<impl BufMut>,
) -> memcomparable::Result<()> {
    // NOTE: No need to call `serializer.set_reverse` because we are inside a
    // composite type value, we should follow the outside order, except for `NULL`s.
    if let Some(scalar) = datum.to_datum_ref() {
        DEFAULT_NULL_TAG_SOME.serialize(&mut *serializer)?;
        scalar.serialize(serializer)?;
    } else {
        DEFAULT_NULL_TAG_NONE.serialize(serializer)?;
    }
    Ok(())
}

pub(crate) fn deserialize_datum(
    ty: &DataType,
    order: OrderType,
    deserializer: &mut memcomparable::Deserializer<impl Buf>,
) -> memcomparable::Result<Datum> {
    deserializer.set_reverse(order.is_descending());
    let null_tag = u8::deserialize(&mut *deserializer)?;
    let (null_tag_none, null_tag_some) = if order.nulls_are_largest() {
        (1u8, 0u8) // None > Some
    } else {
        (0u8, 1u8) // None < Some
    };
    if null_tag == null_tag_none {
        Ok(None)
    } else if null_tag == null_tag_some {
        Ok(Some(ScalarImpl::deserialize(ty, deserializer)?))
    } else {
        Err(memcomparable::Error::InvalidTagEncoding(null_tag as _))
    }
}

pub(crate) fn deserialize_datum_in_composite(
    ty: &DataType,
    deserializer: &mut memcomparable::Deserializer<impl Buf>,
) -> memcomparable::Result<Datum> {
    // NOTE: Similar to serialization, we should follow the outside order, except for `NULL`s.
    let null_tag = u8::deserialize(&mut *deserializer)?;
    if null_tag == DEFAULT_NULL_TAG_NONE {
        Ok(None)
    } else if null_tag == DEFAULT_NULL_TAG_SOME {
        Ok(Some(ScalarImpl::deserialize(ty, deserializer)?))
    } else {
        Err(memcomparable::Error::InvalidTagEncoding(null_tag as _))
    }
}

/// Deserialize the `data_size` of `input_data_type` in `memcmp_encoding`. This function will
/// consume the offset of deserializer then return the length (without memcopy, only length
/// calculation).
pub(crate) fn calculate_encoded_size(
    ty: &DataType,
    order: OrderType,
    encoded_data: &[u8],
) -> memcomparable::Result<usize> {
    let mut deserializer = memcomparable::Deserializer::new(encoded_data);
    let (null_tag_none, null_tag_some) = if order.nulls_are_largest() {
        (1u8, 0u8) // None > Some
    } else {
        (0u8, 1u8) // None < Some
    };
    deserializer.set_reverse(order.is_descending());
    calculate_encoded_size_inner(ty, null_tag_none, null_tag_some, &mut deserializer)
}

fn calculate_encoded_size_inner(
    ty: &DataType,
    null_tag_none: u8,
    null_tag_some: u8,
    deserializer: &mut memcomparable::Deserializer<impl Buf>,
) -> memcomparable::Result<usize> {
    let base_position = deserializer.position();
    let null_tag = u8::deserialize(&mut *deserializer)?;
    if null_tag == null_tag_none {
        // deserialize nothing more
    } else if null_tag == null_tag_some {
        use std::mem::size_of;
        let len = match ty {
            DataType::Int16 => size_of::<i16>(),
            DataType::Int32 => size_of::<i32>(),
            DataType::Int64 => size_of::<i64>(),
            DataType::Serial => size_of::<Serial>(),
            DataType::Float32 => size_of::<F32>(),
            DataType::Float64 => size_of::<F64>(),
            DataType::Date => size_of::<Date>(),
            DataType::Time => size_of::<Time>(),
            DataType::Timestamp => size_of::<Timestamp>(),
            DataType::Timestamptz => size_of::<i64>(),
            DataType::Boolean => size_of::<u8>(),
            // Interval is serialized as (i32, i32, i64)
            DataType::Interval => size_of::<(i32, i32, i64)>(),
            DataType::Decimal => {
                deserializer.deserialize_decimal()?;
                0 // the len is not used since decimal is not a fixed length type
            }
            // these two types is var-length and should only be determine at runtime.
            // TODO: need some test for this case (e.g. e2e test)
            DataType::List { .. } => deserializer.skip_bytes()?,
            DataType::Struct(t) => t
                .fields
                .iter()
                .map(|field| {
                    // use default null tags inside composite type
                    calculate_encoded_size_inner(
                        field,
                        DEFAULT_NULL_TAG_NONE,
                        DEFAULT_NULL_TAG_SOME,
                        deserializer,
                    )
                })
                .try_fold(0, |a, b| b.map(|b| a + b))?,
            DataType::Jsonb => deserializer.skip_bytes()?,
            DataType::Varchar => deserializer.skip_bytes()?,
            DataType::Bytea => deserializer.skip_bytes()?,
            DataType::Int256 => Int256::size(),
        };

        // consume offset of fixed_type
        if deserializer.position() == base_position + 1 {
            // fixed type
            deserializer.advance(len);
        }
    } else {
        return Err(memcomparable::Error::InvalidTagEncoding(null_tag as _));
    }

    Ok(deserializer.position() - base_position)
}

pub fn encode_value(value: impl ToDatumRef, order: OrderType) -> memcomparable::Result<Vec<u8>> {
    let mut serializer = memcomparable::Serializer::new(vec![]);
    serialize_datum(value, order, &mut serializer)?;
    Ok(serializer.into_inner())
}

pub fn decode_value(
    ty: &DataType,
    encoded_value: &[u8],
    order: OrderType,
) -> memcomparable::Result<Datum> {
    let mut deserializer = memcomparable::Deserializer::new(encoded_value);
    deserialize_datum(ty, order, &mut deserializer)
}

pub fn encode_array(array: &ArrayImpl, order: OrderType) -> memcomparable::Result<Vec<Vec<u8>>> {
    let mut data = Vec::with_capacity(array.len());
    for datum in array.iter() {
        data.push(encode_value(datum, order)?);
    }
    Ok(data)
}

/// This function is used to accelerate the comparison of tuples. It takes datachunk and
/// user-defined order as input, yield encoded binary string with order preserved for each tuple in
/// the datachunk.
pub fn encode_chunk(
    chunk: &DataChunk,
    column_orders: &[ColumnOrder],
) -> memcomparable::Result<Vec<Vec<u8>>> {
    let encoded_columns: Vec<_> = column_orders
        .iter()
        .map(|o| encode_array(chunk.column_at(o.column_index).array_ref(), o.order_type))
        .try_collect()?;

    let mut encoded_chunk = vec![vec![]; chunk.capacity()];
    for encoded_column in encoded_columns {
        for (encoded_row, data) in encoded_chunk.iter_mut().zip_eq_fast(encoded_column) {
            encoded_row.extend(data);
        }
    }

    Ok(encoded_chunk)
}

/// Encode a row into memcomparable format.
pub fn encode_row(row: impl Row, order_types: &[OrderType]) -> memcomparable::Result<Vec<u8>> {
    let mut serializer = memcomparable::Serializer::new(vec![]);
    row.iter()
        .zip_eq_debug(order_types)
        .try_for_each(|(datum, order)| serialize_datum(datum, *order, &mut serializer))?;
    Ok(serializer.into_inner())
}

#[cfg(test)]
mod tests {
    use std::ops::Neg;

    use itertools::Itertools;
    use rand::thread_rng;

    use super::*;
    use crate::array::{DataChunk, ListValue, StructValue};
    use crate::row::{OwnedRow, RowExt};
    use crate::types::{DataType, ScalarImpl, F32};
    use crate::util::sort_util::{ColumnOrder, OrderType};

    #[test]
    fn test_memcomparable() {
        fn encode_num(num: Option<i32>, order_type: OrderType) -> Vec<u8> {
            encode_value(num.map(ScalarImpl::from), order_type).unwrap()
        }

        {
            // default ascending
            let order_type = OrderType::ascending();
            let memcmp_minus_1 = encode_num(Some(-1), order_type);
            let memcmp_3874 = encode_num(Some(3874), order_type);
            let memcmp_45745 = encode_num(Some(45745), order_type);
            let memcmp_i32_min = encode_num(Some(i32::MIN), order_type);
            let memcmp_i32_max = encode_num(Some(i32::MAX), order_type);
            let memcmp_none = encode_num(None, order_type);

            assert!(memcmp_3874 < memcmp_45745);
            assert!(memcmp_3874 < memcmp_i32_max);
            assert!(memcmp_45745 < memcmp_i32_max);

            assert!(memcmp_i32_min < memcmp_i32_max);
            assert!(memcmp_i32_min < memcmp_3874);
            assert!(memcmp_i32_min < memcmp_45745);

            assert!(memcmp_minus_1 < memcmp_3874);
            assert!(memcmp_minus_1 < memcmp_45745);
            assert!(memcmp_minus_1 < memcmp_i32_max);
            assert!(memcmp_minus_1 > memcmp_i32_min);

            assert!(memcmp_none > memcmp_minus_1);
            assert!(memcmp_none > memcmp_3874);
            assert!(memcmp_none > memcmp_i32_min);
            assert!(memcmp_none > memcmp_i32_max);
        }
        {
            // default descending
            let order_type = OrderType::descending();
            let memcmp_minus_1 = encode_num(Some(-1), order_type);
            let memcmp_3874 = encode_num(Some(3874), order_type);
            let memcmp_none = encode_num(None, order_type);

            assert!(memcmp_none < memcmp_minus_1);
            assert!(memcmp_none < memcmp_3874);
            assert!(memcmp_3874 < memcmp_minus_1);
        }
        {
            // ASC NULLS FIRST (NULLS SMALLEST)
            let order_type = OrderType::ascending_nulls_first();
            let memcmp_minus_1 = encode_num(Some(-1), order_type);
            let memcmp_3874 = encode_num(Some(3874), order_type);
            let memcmp_none = encode_num(None, order_type);
            assert!(memcmp_none < memcmp_minus_1);
            assert!(memcmp_none < memcmp_3874);
        }
        {
            // ASC NULLS LAST (NULLS LARGEST)
            let order_type = OrderType::ascending_nulls_last();
            let memcmp_minus_1 = encode_num(Some(-1), order_type);
            let memcmp_3874 = encode_num(Some(3874), order_type);
            let memcmp_none = encode_num(None, order_type);
            assert!(memcmp_none > memcmp_minus_1);
            assert!(memcmp_none > memcmp_3874);
        }
        {
            // DESC NULLS FIRST (NULLS LARGEST)
            let order_type = OrderType::descending_nulls_first();
            let memcmp_minus_1 = encode_num(Some(-1), order_type);
            let memcmp_3874 = encode_num(Some(3874), order_type);
            let memcmp_none = encode_num(None, order_type);
            assert!(memcmp_none < memcmp_minus_1);
            assert!(memcmp_none < memcmp_3874);
        }
        {
            // DESC NULLS LAST (NULLS SMALLEST)
            let order_type = OrderType::descending_nulls_last();
            let memcmp_minus_1 = encode_num(Some(-1), order_type);
            let memcmp_3874 = encode_num(Some(3874), order_type);
            let memcmp_none = encode_num(None, order_type);
            assert!(memcmp_none > memcmp_minus_1);
            assert!(memcmp_none > memcmp_3874);
        }
    }

    #[test]
    fn test_memcomparable_structs() {
        // NOTE: `NULL`s inside composite type values are always the largest.

        let struct_none = None;
        let struct_1 = Some(
            StructValue::new(vec![Some(ScalarImpl::from(1)), Some(ScalarImpl::from(2))]).into(),
        );
        let struct_2 = Some(
            StructValue::new(vec![Some(ScalarImpl::from(1)), Some(ScalarImpl::from(3))]).into(),
        );
        let struct_3 = Some(StructValue::new(vec![Some(ScalarImpl::from(1)), None]).into());

        {
            // ASC NULLS FIRST (NULLS SMALLEST)
            let order_type = OrderType::ascending_nulls_first();
            let memcmp_struct_none = encode_value(&struct_none, order_type).unwrap();
            let memcmp_struct_1 = encode_value(&struct_1, order_type).unwrap();
            let memcmp_struct_2 = encode_value(&struct_2, order_type).unwrap();
            let memcmp_struct_3 = encode_value(&struct_3, order_type).unwrap();
            assert!(memcmp_struct_none < memcmp_struct_1);
            assert!(memcmp_struct_1 < memcmp_struct_2);
            assert!(memcmp_struct_2 < memcmp_struct_3);
        }
        {
            // ASC NULLS LAST (NULLS LARGEST)
            let order_type = OrderType::ascending_nulls_last();
            let memcmp_struct_none = encode_value(&struct_none, order_type).unwrap();
            let memcmp_struct_1 = encode_value(&struct_1, order_type).unwrap();
            let memcmp_struct_2 = encode_value(&struct_2, order_type).unwrap();
            let memcmp_struct_3 = encode_value(&struct_3, order_type).unwrap();
            assert!(memcmp_struct_1 < memcmp_struct_2);
            assert!(memcmp_struct_2 < memcmp_struct_3);
            assert!(memcmp_struct_3 < memcmp_struct_none);
        }
        {
            // DESC NULLS FIRST (NULLS LARGEST)
            let order_type = OrderType::descending_nulls_first();
            let memcmp_struct_none = encode_value(&struct_none, order_type).unwrap();
            let memcmp_struct_1 = encode_value(&struct_1, order_type).unwrap();
            let memcmp_struct_2 = encode_value(&struct_2, order_type).unwrap();
            let memcmp_struct_3 = encode_value(&struct_3, order_type).unwrap();
            assert!(memcmp_struct_none < memcmp_struct_3);
            assert!(memcmp_struct_3 < memcmp_struct_2);
            assert!(memcmp_struct_2 < memcmp_struct_1);
        }
        {
            // DESC NULLS LAST (NULLS SMALLEST)
            let order_type = OrderType::descending_nulls_last();
            let memcmp_struct_none = encode_value(&struct_none, order_type).unwrap();
            let memcmp_struct_1 = encode_value(&struct_1, order_type).unwrap();
            let memcmp_struct_2 = encode_value(&struct_2, order_type).unwrap();
            let memcmp_struct_3 = encode_value(&struct_3, order_type).unwrap();
            assert!(memcmp_struct_3 < memcmp_struct_2);
            assert!(memcmp_struct_2 < memcmp_struct_1);
            assert!(memcmp_struct_1 < memcmp_struct_none);
        }
    }

    #[test]
    fn test_memcomparable_lists() {
        // NOTE: `NULL`s inside composite type values are always the largest.

        let list_none = None;
        let list_1 =
            Some(ListValue::new(vec![Some(ScalarImpl::from(1)), Some(ScalarImpl::from(2))]).into());
        let list_2 =
            Some(ListValue::new(vec![Some(ScalarImpl::from(1)), Some(ScalarImpl::from(3))]).into());
        let list_3 = Some(ListValue::new(vec![Some(ScalarImpl::from(1)), None]).into());

        {
            // ASC NULLS FIRST (NULLS SMALLEST)
            let order_type = OrderType::ascending_nulls_first();
            let memcmp_list_none = encode_value(&list_none, order_type).unwrap();
            let memcmp_list_1 = encode_value(&list_1, order_type).unwrap();
            let memcmp_list_2 = encode_value(&list_2, order_type).unwrap();
            let memcmp_list_3 = encode_value(&list_3, order_type).unwrap();
            assert!(memcmp_list_none < memcmp_list_1);
            assert!(memcmp_list_1 < memcmp_list_2);
            assert!(memcmp_list_2 < memcmp_list_3);
        }
        {
            // ASC NULLS LAST (NULLS LARGEST)
            let order_type = OrderType::ascending_nulls_last();
            let memcmp_list_none = encode_value(&list_none, order_type).unwrap();
            let memcmp_list_1 = encode_value(&list_1, order_type).unwrap();
            let memcmp_list_2 = encode_value(&list_2, order_type).unwrap();
            let memcmp_list_3 = encode_value(&list_3, order_type).unwrap();
            assert!(memcmp_list_1 < memcmp_list_2);
            assert!(memcmp_list_2 < memcmp_list_3);
            assert!(memcmp_list_3 < memcmp_list_none);
        }
        {
            // DESC NULLS FIRST (NULLS LARGEST)
            let order_type = OrderType::descending_nulls_first();
            let memcmp_list_none = encode_value(&list_none, order_type).unwrap();
            let memcmp_list_1 = encode_value(&list_1, order_type).unwrap();
            let memcmp_list_2 = encode_value(&list_2, order_type).unwrap();
            let memcmp_list_3 = encode_value(&list_3, order_type).unwrap();
            assert!(memcmp_list_none < memcmp_list_3);
            assert!(memcmp_list_3 < memcmp_list_2);
            assert!(memcmp_list_2 < memcmp_list_1);
        }
        {
            // DESC NULLS LAST (NULLS SMALLEST)
            let order_type = OrderType::descending_nulls_last();
            let memcmp_list_none = encode_value(&list_none, order_type).unwrap();
            let memcmp_list_1 = encode_value(&list_1, order_type).unwrap();
            let memcmp_list_2 = encode_value(&list_2, order_type).unwrap();
            let memcmp_list_3 = encode_value(&list_3, order_type).unwrap();
            assert!(memcmp_list_3 < memcmp_list_2);
            assert!(memcmp_list_2 < memcmp_list_1);
            assert!(memcmp_list_1 < memcmp_list_none);
        }
    }

    #[test]
    fn test_issue_legacy_2057_ordered_float_memcomparable() {
        use num_traits::*;
        use rand::seq::SliceRandom;

        fn serialize(f: F32) -> Vec<u8> {
            encode_value(&Some(ScalarImpl::from(f)), OrderType::default()).unwrap()
        }

        fn deserialize(data: Vec<u8>) -> F32 {
            decode_value(&DataType::Float32, &data, OrderType::default())
                .unwrap()
                .unwrap()
                .into_float32()
        }

        let floats = vec![
            // -inf
            F32::neg_infinity(),
            // -1
            F32::one().neg(),
            // 0, -0 should be treated the same
            F32::zero(),
            F32::neg_zero(),
            F32::zero(),
            // 1
            F32::one(),
            // inf
            F32::infinity(),
            // nan, -nan should be treated the same
            F32::nan(),
            F32::nan().neg(),
            F32::nan(),
        ];
        assert!(floats.is_sorted());

        let mut floats_clone = floats.clone();
        floats_clone.shuffle(&mut thread_rng());
        floats_clone.sort();
        assert_eq!(floats, floats_clone);

        let memcomparables = floats.clone().into_iter().map(serialize).collect_vec();
        assert!(memcomparables.is_sorted());

        let decoded_floats = memcomparables.into_iter().map(deserialize).collect_vec();
        assert!(decoded_floats.is_sorted());
        assert_eq!(floats, decoded_floats);
    }

    #[test]
    fn test_encode_row() {
        let v10 = Some(ScalarImpl::Int32(42));
        let v10_cloned = v10.clone();
        let v11 = Some(ScalarImpl::Utf8("hello".into()));
        let v11_cloned = v11.clone();
        let v12 = Some(ScalarImpl::Float32(4.0.into()));
        let v20 = Some(ScalarImpl::Int32(42));
        let v21 = Some(ScalarImpl::Utf8("hell".into()));
        let v22 = Some(ScalarImpl::Float32(3.0.into()));

        let row1 = OwnedRow::new(vec![v10, v11, v12]);
        let row2 = OwnedRow::new(vec![v20, v21, v22]);
        let order_col_indices = vec![0, 1];
        let order_types = vec![OrderType::ascending(), OrderType::descending()];

        let encoded_row1 = encode_row(row1.project(&order_col_indices), &order_types).unwrap();
        let encoded_v10 = encode_value(
            v10_cloned.as_ref().map(|x| x.as_scalar_ref_impl()),
            OrderType::ascending(),
        )
        .unwrap();
        let encoded_v11 = encode_value(
            v11_cloned.as_ref().map(|x| x.as_scalar_ref_impl()),
            OrderType::descending(),
        )
        .unwrap();
        let concated_encoded_row1 = encoded_v10
            .into_iter()
            .chain(encoded_v11.into_iter())
            .collect_vec();
        assert_eq!(encoded_row1, concated_encoded_row1);

        let encoded_row2 = encode_row(row2.project(&order_col_indices), &order_types).unwrap();
        assert!(encoded_row1 < encoded_row2);
    }

    #[test]
    fn test_encode_chunk() {
        let v10 = Some(ScalarImpl::Int32(42));
        let v11 = Some(ScalarImpl::Utf8("hello".into()));
        let v12 = Some(ScalarImpl::Float32(4.0.into()));
        let v20 = Some(ScalarImpl::Int32(42));
        let v21 = Some(ScalarImpl::Utf8("hell".into()));
        let v22 = Some(ScalarImpl::Float32(3.0.into()));

        let row1 = OwnedRow::new(vec![v10, v11, v12]);
        let row2 = OwnedRow::new(vec![v20, v21, v22]);
        let chunk = DataChunk::from_rows(
            &[row1.clone(), row2.clone()],
            &[DataType::Int32, DataType::Varchar, DataType::Float32],
        );
        let order_col_indices = vec![0, 1];
        let order_types = vec![OrderType::ascending(), OrderType::descending()];
        let column_orders = order_col_indices
            .iter()
            .zip_eq_fast(&order_types)
            .map(|(i, o)| ColumnOrder::new(*i, *o))
            .collect_vec();

        let encoded_row1 = encode_row(row1.project(&order_col_indices), &order_types).unwrap();
        let encoded_row2 = encode_row(row2.project(&order_col_indices), &order_types).unwrap();
        let encoded_chunk = encode_chunk(&chunk, &column_orders).unwrap();
        assert_eq!(&encoded_chunk, &[encoded_row1, encoded_row2]);
    }
}

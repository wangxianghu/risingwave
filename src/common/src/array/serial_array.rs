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

use risingwave_pb::data::{Array as ProstArray};

use super::{Array, ArrayBuilder, ArrayMeta};
use crate::array::{ArrayBuilderImpl, PrimitiveArray, PrimitiveArrayBuilder};
use crate::buffer::{Bitmap};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Serial {
    pub(crate) inner: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SerialArray {
    inner: PrimitiveArray<i64>,
}

impl FromIterator<Option<Serial>> for SerialArray {
    fn from_iter<I: IntoIterator<Item = Option<Serial>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for i in iter {
            builder.append(i);
        }
        builder.finish()
    }
}

impl<'a> FromIterator<&'a Option<Serial>> for SerialArray {
    fn from_iter<I: IntoIterator<Item = &'a Option<Serial>>>(iter: I) -> Self {
        iter.into_iter().cloned().collect()
    }
}

impl FromIterator<Serial> for SerialArray {
    fn from_iter<I: IntoIterator<Item = Serial>>(iter: I) -> Self {
        Self {
            inner: PrimitiveArray::<i64>::from_iter(iter.into_iter().map(|i| i.inner)),
        }
    }
}

impl Array for SerialArray {
    type Builder = SerialArrayBuilder;
    type OwnedItem = Serial;
    type RefItem<'a> = Serial;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Serial {
        Serial {
            inner: self.inner.raw_value_at_unchecked(idx),
        }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn to_protobuf(&self) -> ProstArray {
        self.inner.to_protobuf()
    }

    fn null_bitmap(&self) -> &Bitmap {
        self.inner.null_bitmap()
    }

    fn into_null_bitmap(self) -> Bitmap {
        self.inner.into_null_bitmap()
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        self.inner.set_bitmap(bitmap)
    }

    fn create_builder(&self, capacity: usize) -> ArrayBuilderImpl {
        let array_builder = SerialArrayBuilder::new(capacity);
        ArrayBuilderImpl::Serial(array_builder)
    }
}

/// `SerialArrayBuilder` constructs a `SerialArray` from `Option<Serial>`.
#[derive(Debug)]
pub struct SerialArrayBuilder {
    inner: PrimitiveArrayBuilder<i64>,
}

impl ArrayBuilder for SerialArrayBuilder {
    type ArrayType = SerialArray;

    fn with_meta(capacity: usize, _meta: ArrayMeta) -> Self {
        Self {
            inner: PrimitiveArrayBuilder::<i64>::with_meta(capacity, _meta),
        }
    }

    fn append_n(&mut self, n: usize, value: Option<Serial>) {
        self.inner.append_n(n, value.map(|i| i.inner))
    }

    fn append_array(&mut self, other: &SerialArray) {
        self.inner.append_array(&other.inner)
    }

    fn pop(&mut self) -> Option<()> {
        self.inner.pop()
    }

    fn finish(self) -> SerialArray {
        SerialArray {
            inner: self.inner.finish(),
        }
    }
}

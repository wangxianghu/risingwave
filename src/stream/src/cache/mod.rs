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

use std::alloc::{Allocator, Global};
use std::hash::{BuildHasher, Hash};
use std::ops::{Deref, DerefMut};

use lru::{DefaultHasher, LruCache};

mod managed_lru;
pub use managed_lru::*;
pub(super) use risingwave_common::buffer::cache_may_stale;

pub struct ExecutorCache<K, V, S = DefaultHasher, A: Clone + Allocator = Global> {
    /// An managed cache. Eviction depends on the node memory usage.
    cache: ManagedLruCache<K, V, S, A>,
}

impl<K: Hash + Eq, V, S: BuildHasher, A: Clone + Allocator> ExecutorCache<K, V, S, A> {
    pub fn new(cache: ManagedLruCache<K, V, S, A>) -> Self {
        Self { cache }
    }

    /// Evict epochs lower than the watermark
    pub fn evict(&mut self) {
        self.cache.evict()
    }

    /// Update the current epoch for cache. Only effective when using [`ManagedLruCache`]
    pub fn update_epoch(&mut self, epoch: u64) {
        self.cache.update_epoch(epoch)
    }

    /// An iterator visiting all values in most-recently used order. The iterator element type is
    /// &V.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        let get_val = |(_k, v)| v;
        self.cache.iter().map(get_val)
    }

    /// An iterator visiting all values mutably in most-recently used order. The iterator element
    /// type is &mut V.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        let get_val = |(_k, v)| v;
        self.cache.iter_mut().map(get_val)
    }
}

impl<K, V, S, A: Clone + Allocator> Deref for ExecutorCache<K, V, S, A> {
    type Target = LruCache<K, V, S, A>;

    fn deref(&self) -> &Self::Target {
        &self.cache.inner
    }
}

impl<K, V, S, A: Clone + Allocator> DerefMut for ExecutorCache<K, V, S, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cache.inner
    }
}

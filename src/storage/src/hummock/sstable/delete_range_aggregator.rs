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

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::key::{FullKey, UserKey};
use risingwave_hummock_sdk::HummockEpoch;

use super::DeleteRangeTombstone;
use crate::hummock::iterator::DeleteRangeIterator;
use crate::hummock::sstable_store::TableHolder;
use crate::hummock::Sstable;

pub struct SortedBoundary {
    sequence: HummockEpoch,
    user_key: UserKey<Vec<u8>>,
}

impl PartialEq<Self> for SortedBoundary {
    fn eq(&self, other: &Self) -> bool {
        self.user_key.eq(&other.user_key) && self.sequence == other.sequence
    }
}

impl PartialOrd for SortedBoundary {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ret = other
            .user_key
            .cmp(&self.user_key)
            .then_with(|| other.sequence.cmp(&self.sequence));
        Some(ret)
    }
}

impl Eq for SortedBoundary {}

impl Ord for SortedBoundary {
    fn cmp(&self, other: &Self) -> Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

#[derive(Default)]
pub struct DeleteRangeAggregatorBuilder {
    delete_tombstones: Vec<DeleteRangeTombstone>,
}

type CompactionDeleteRangeEvent = (
    UserKey<Vec<u8>>,
    Vec<(usize, HummockEpoch)>,
    Vec<(usize, HummockEpoch)>,
);
pub(crate) fn apply_event(epochs: &mut BTreeSet<HummockEpoch>, event: &CompactionDeleteRangeEvent) {
    let (_, delete, insert) = event;
    // Correct because ranges in an epoch won't intersect.
    for (_, epoch) in delete {
        epochs.remove(epoch);
    }
    for (_, epoch) in insert {
        epochs.insert(*epoch);
    }
}

pub struct CompactionDeleteRanges {
    delete_tombstones: Vec<DeleteRangeTombstone>,
    events: Vec<CompactionDeleteRangeEvent>,
    event_seek_mapping: Vec<usize>,
    watermark: HummockEpoch,
    gc_delete_keys: bool,
}

pub struct RangeTombstonesCollector {
    range_tombstone_list: Vec<DeleteRangeTombstone>,
    watermark: u64,
    gc_delete_keys: bool,
}

impl DeleteRangeAggregatorBuilder {
    pub fn add_tombstone(&mut self, data: Vec<DeleteRangeTombstone>) {
        self.delete_tombstones.extend(data);
    }

    pub fn build(self, watermark: u64, gc_delete_keys: bool) -> Arc<RangeTombstonesCollector> {
        // sort tombstones by start-key.
        let mut tombstone_index = BinaryHeap::<Reverse<DeleteRangeTombstone>>::default();
        let mut sorted_tombstones: Vec<DeleteRangeTombstone> = vec![];
        for tombstone in self.delete_tombstones {
            tombstone_index.push(Reverse(tombstone));
        }
        while let Some(Reverse(tombstone)) = tombstone_index.pop() {
            for last in sorted_tombstones.iter_mut().rev() {
                if last.end_user_key.gt(&tombstone.end_user_key) {
                    let mut new_tombstone = last.clone();
                    new_tombstone.start_user_key = tombstone.end_user_key.clone();
                    last.end_user_key = tombstone.end_user_key.clone();
                    tombstone_index.push(Reverse(new_tombstone));
                } else {
                    break;
                }
            }
            sorted_tombstones.push(tombstone);
        }

        #[cfg(debug_assertions)]
        {
            check_sorted_tombstone(&sorted_tombstones);
        }

        sorted_tombstones.sort();
        Arc::new(RangeTombstonesCollector {
            range_tombstone_list: sorted_tombstones,
            gc_delete_keys,
            watermark,
        })
    }

    pub(crate) fn build_events(
        delete_tombstones: &Vec<DeleteRangeTombstone>,
    ) -> (Vec<CompactionDeleteRangeEvent>, Vec<usize>) {
        let tombstone_len = delete_tombstones.len();
        let mut events = Vec::with_capacity(tombstone_len * 2);
        for (
            index,
            DeleteRangeTombstone {
                start_user_key,
                end_user_key,
                ..
            },
        ) in delete_tombstones.iter().enumerate()
        {
            events.push((start_user_key, 1, index));
            events.push((end_user_key, 0, index));
        }
        events.sort();

        let mut result = Vec::with_capacity(events.len());
        let mut insert_pos = vec![0; tombstone_len];
        for (user_key, group) in &events.into_iter().group_by(|(user_key, _, _)| *user_key) {
            let (mut delete, mut insert) = (vec![], vec![]);
            for (_, op, index) in group {
                match op {
                    0 => delete.push((index, delete_tombstones[index].sequence)),
                    1 => {
                        insert.push((index, delete_tombstones[index].sequence));
                        insert_pos[index] = result.len();
                    }
                    _ => unreachable!(),
                }
            }
            result.push((user_key.clone(), delete, insert));
        }

        (result, insert_pos)
    }

    pub(crate) fn build_for_compaction(
        self,
        watermark: HummockEpoch,
        gc_delete_keys: bool,
    ) -> Arc<CompactionDeleteRanges> {
        let (result, insert_pos) = Self::build_events(&self.delete_tombstones);

        let result_len = result.len();
        let mut event_seek_mapping = vec![0; result_len + 1];
        let mut hook = result_len;
        event_seek_mapping[result_len] = hook;
        for (result_idx, (_, delete, _insert)) in result.iter().enumerate().rev() {
            if result_idx < hook {
                hook = result_idx;
            }
            for (index, _) in delete {
                if insert_pos[*index] < hook {
                    hook = insert_pos[*index];
                }
            }
            event_seek_mapping[result_idx] = hook;
        }

        Arc::new(CompactionDeleteRanges {
            delete_tombstones: self.delete_tombstones,
            events: result,
            event_seek_mapping,
            watermark,
            gc_delete_keys,
        })
    }
}

#[cfg(debug_assertions)]
fn check_sorted_tombstone(sorted_tombstones: &[DeleteRangeTombstone]) {
    for idx in 1..sorted_tombstones.len() {
        assert!(sorted_tombstones[idx]
            .start_user_key
            .ge(&sorted_tombstones[idx - 1].start_user_key));
        assert!(sorted_tombstones[idx]
            .end_user_key
            .ge(&sorted_tombstones[idx - 1].end_user_key));
    }
}

impl CompactionDeleteRanges {
    pub(crate) fn for_test() -> Self {
        Self {
            delete_tombstones: vec![],
            events: vec![],
            event_seek_mapping: vec![0],
            gc_delete_keys: false,
            watermark: 0,
        }
    }

    pub(crate) fn iter(self: &Arc<Self>) -> CompactionDeleteRangeIterator {
        CompactionDeleteRangeIterator {
            events: self.clone(),
            seek_idx: 0,
            epochs: BTreeSet::default(),
        }
    }

    pub(crate) fn get_tombstone_between(
        &self,
        smallest_user_key: &UserKey<&[u8]>,
        largest_user_key: &UserKey<&[u8]>,
    ) -> Vec<DeleteRangeTombstone> {
        let (mut tombstones_above_watermark, mut tombstones_within_watermark) = (vec![], vec![]);
        for tombstone in &self.delete_tombstones {
            let mut candidate = tombstone.clone();
            if !smallest_user_key.is_empty()
                && smallest_user_key.gt(&candidate.start_user_key.as_ref())
            {
                candidate.start_user_key = smallest_user_key.to_vec();
            }
            if !largest_user_key.is_empty() && largest_user_key.lt(&candidate.end_user_key.as_ref())
            {
                candidate.end_user_key = largest_user_key.to_vec();
            }
            if candidate.start_user_key < candidate.end_user_key {
                if candidate.sequence > self.watermark {
                    tombstones_above_watermark.push(candidate);
                } else {
                    tombstones_within_watermark.push(candidate);
                }
            }
        }

        let mut ret = tombstones_above_watermark;
        if self.gc_delete_keys {
            return ret;
        }

        let (events, _) = DeleteRangeAggregatorBuilder::build_events(&tombstones_within_watermark);
        let mut epoch2index = BTreeMap::new();
        let mut is_useful = vec![false; tombstones_within_watermark.len()];
        for (_, delete, insert) in events {
            // Correct because ranges in an epoch won't intersect.
            for (_, epoch) in delete {
                epoch2index.remove(&epoch);
            }
            for (index, epoch) in insert {
                epoch2index.insert(epoch, index);
            }
            if let Some((_, index)) = epoch2index.last_key_value() {
                is_useful[*index] = true;
            }
        }

        ret.extend(
            tombstones_within_watermark
                .into_iter()
                .enumerate()
                .filter(|(index, _tombstone)| is_useful[*index])
                .map(|(_index, tombstone)| tombstone),
        );
        ret
    }
}

impl RangeTombstonesCollector {
    pub fn for_test() -> Self {
        Self {
            range_tombstone_list: vec![],
            gc_delete_keys: false,
            watermark: 0,
        }
    }

    // split ranges to make sure they locate in [smallest_user_key, largest_user_key)
    pub fn get_tombstone_between(
        &self,
        smallest_user_key: &UserKey<&[u8]>,
        largest_user_key: &UserKey<&[u8]>,
    ) -> Vec<DeleteRangeTombstone> {
        let mut delete_ranges: Vec<DeleteRangeTombstone> = vec![];
        for tombstone in &self.range_tombstone_list {
            if !largest_user_key.is_empty()
                && tombstone.start_user_key.as_ref().ge(largest_user_key)
            {
                continue;
            }

            if !smallest_user_key.is_empty()
                && tombstone.end_user_key.as_ref().le(smallest_user_key)
            {
                continue;
            }

            if tombstone.sequence <= self.watermark {
                if self.gc_delete_keys {
                    continue;
                }
                if let Some(last) = delete_ranges.last() {
                    if last.start_user_key.eq(&tombstone.start_user_key)
                        && last.end_user_key.eq(&tombstone.end_user_key)
                        && last.sequence <= self.watermark
                    {
                        assert!(last.sequence > tombstone.sequence);
                        continue;
                    }
                }
            }

            let mut ret = tombstone.clone();
            if !smallest_user_key.is_empty() && smallest_user_key.gt(&ret.start_user_key.as_ref()) {
                ret.start_user_key = smallest_user_key.to_vec();
            }
            if !largest_user_key.is_empty() && largest_user_key.lt(&ret.end_user_key.as_ref()) {
                ret.end_user_key = largest_user_key.to_vec();
            }
            delete_ranges.push(ret);
        }
        delete_ranges
    }
}

pub(crate) struct CompactionDeleteRangeIterator {
    events: Arc<CompactionDeleteRanges>,
    seek_idx: usize,
    /// The correctness of the algorithm needs to be guaranteed by "the epoch of the
    /// intervals covering each other must be different".
    epochs: BTreeSet<HummockEpoch>,
}

impl CompactionDeleteRangeIterator {
    fn apply(&mut self, idx: usize) {
        apply_event(&mut self.epochs, &self.events.events[idx]);
    }

    /// Return the earliest range-tombstone which deletes target-key.
    /// Target-key must be given in order.
    pub(crate) fn earliest_delete(
        &mut self,
        target_user_key: &UserKey<&[u8]>,
        epoch: HummockEpoch,
    ) -> HummockEpoch {
        while let Some((user_key, ..)) = self.events.events.get(self.seek_idx) && user_key.as_ref().le(target_user_key) {
            self.apply(self.seek_idx);
            self.seek_idx += 1;
        }
        self.epochs
            .range(epoch..)
            .next()
            .map_or(HummockEpoch::MAX, |ret| *ret)
    }

    pub(crate) fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) {
        self.seek_idx = self
            .events
            .events
            .partition_point(|(user_key, ..)| user_key.as_ref().le(&target_user_key));
        self.epochs.clear();
        let hook = self.events.event_seek_mapping[self.seek_idx];
        for idx in hook..self.seek_idx {
            self.apply(idx);
        }
    }

    pub(crate) fn rewind(&mut self) {
        self.seek_idx = 0;
        self.epochs.clear();
    }
}

pub struct SstableDeleteRangeIterator {
    table: TableHolder,
    next_idx: usize,
}

impl SstableDeleteRangeIterator {
    pub fn new(table: TableHolder) -> Self {
        Self { table, next_idx: 0 }
    }
}

impl DeleteRangeIterator for SstableDeleteRangeIterator {
    fn next_user_key(&self) -> UserKey<&[u8]> {
        self.table.value().monotonic_deletes[self.next_idx]
            .0
            .as_ref()
    }

    fn current_epoch(&self) -> HummockEpoch {
        if self.next_idx > 0 {
            self.table.value().monotonic_deletes[self.next_idx - 1].1
        } else {
            HummockEpoch::MAX
        }
    }

    fn next(&mut self) {
        self.next_idx += 1;
    }

    fn rewind(&mut self) {
        self.next_idx = 0;
    }

    fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) {
        self.next_idx = self
            .table
            .value()
            .monotonic_deletes
            .partition_point(|(user_key, _)| user_key.as_ref().le(&target_user_key));
    }

    fn is_valid(&self) -> bool {
        self.next_idx < self.table.value().monotonic_deletes.len()
    }
}

pub fn get_min_delete_range_epoch_from_sstable(
    table: &Sstable,
    query_user_key: &UserKey<&[u8]>,
) -> HummockEpoch {
    let idx = table
        .monotonic_deletes
        .partition_point(|(user_key, _)| user_key.as_ref().le(query_user_key));
    if idx == 0 {
        HummockEpoch::MAX
    } else {
        table.monotonic_deletes[idx - 1].1
    }
}

pub fn get_delete_range_epoch_from_sstable(
    table: &Sstable,
    full_key: &FullKey<&[u8]>,
) -> Option<HummockEpoch> {
    if table.meta.range_tombstone_list.is_empty() {
        return None;
    }
    let watermark = full_key.epoch;
    let mut idx = table
        .meta
        .range_tombstone_list
        .partition_point(|tombstone| tombstone.end_user_key.as_ref().le(&full_key.user_key));
    if idx >= table.meta.range_tombstone_list.len() {
        return None;
    }
    let mut epoch = None;
    while idx < table.meta.range_tombstone_list.len()
        && table.meta.range_tombstone_list[idx]
            .start_user_key
            .as_ref()
            .le(&full_key.user_key)
    {
        let sequence = table.meta.range_tombstone_list[idx].sequence;
        if sequence > watermark {
            idx += 1;
            continue;
        }
        if epoch
            .as_ref()
            .map(|epoch| *epoch < sequence)
            .unwrap_or(true)
        {
            epoch = Some(sequence);
        }
        idx += 1;
    }
    epoch
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rand::Rng;
    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::key::TableKey;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        gen_iterator_test_sstable_with_range_tombstones, iterator_test_key_of_epoch,
        mock_sstable_store,
    };
    use crate::hummock::test_utils::test_user_key;

    #[test]
    pub fn test_delete_range_split() {
        let table_id = TableId::default();
        let mut builder = DeleteRangeAggregatorBuilder::default();
        builder.add_tombstone(vec![
            DeleteRangeTombstone::new(table_id, b"aaaa".to_vec(), b"bbbb".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"aaaa".to_vec(), b"cccc".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"cccc".to_vec(), b"dddd".to_vec(), 10),
            DeleteRangeTombstone::new(table_id, b"cccc".to_vec(), b"eeee".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"eeee".to_vec(), b"ffff".to_vec(), 12),
        ]);
        let agg = builder.build(10, true);
        let split_ranges = agg.get_tombstone_between(
            &test_user_key(b"bbbb").as_ref(),
            &test_user_key(b"eeeeee").as_ref(),
        );
        assert_eq!(3, split_ranges.len());
        assert_eq!(test_user_key(b"bbbb"), split_ranges[0].start_user_key);
        assert_eq!(test_user_key(b"cccc"), split_ranges[0].end_user_key);
        assert_eq!(test_user_key(b"cccc"), split_ranges[1].start_user_key);
        assert_eq!(test_user_key(b"eeee"), split_ranges[1].end_user_key);
    }

    #[tokio::test]
    async fn test_delete_range_get() {
        let sstable_store = mock_sstable_store();
        // key=[idx, epoch], value
        let sstable = gen_iterator_test_sstable_with_range_tombstones(
            0,
            vec![],
            vec![(0, 2, 300), (1, 4, 150), (3, 6, 50), (5, 8, 150)],
            sstable_store,
        )
        .await;
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(0, 200).to_ref(),
        );
        assert!(ret.is_none());
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(1, 100).to_ref(),
        );
        assert!(ret.is_none());
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(1, 200).to_ref(),
        );
        assert_eq!(ret, Some(150));
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(1, 300).to_ref(),
        );
        assert_eq!(ret, Some(300));
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(3, 100).to_ref(),
        );
        assert_eq!(ret, Some(50));
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(6, 100).to_ref(),
        );
        assert!(ret.is_none());
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(6, 200).to_ref(),
        );
        assert_eq!(ret, Some(150));
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(8, 200).to_ref(),
        );
        assert!(ret.is_none());
    }

    #[test]
    pub fn test_delete_cut_range() {
        let mut builder = DeleteRangeAggregatorBuilder::default();
        let mut rng = rand::thread_rng();
        let mut origin = vec![];
        const SEQUENCE_COUNT: HummockEpoch = 5000;
        for sequence in 1..(SEQUENCE_COUNT + 1) {
            let left: u64 = rng.gen_range(0..100);
            let right: u64 = left + rng.gen_range(0..100) + 1;
            let tombstone = DeleteRangeTombstone::new(
                TableId::default(),
                left.to_be_bytes().to_vec(),
                right.to_be_bytes().to_vec(),
                sequence,
            );
            assert!(tombstone.start_user_key.lt(&tombstone.end_user_key));
            origin.push(tombstone);
        }
        builder.add_tombstone(origin.clone());
        let agg = builder.build(0, false);
        let split_ranges = agg.get_tombstone_between(
            &UserKey::new(TableId::default(), TableKey(b"")),
            &UserKey::new(TableId::default(), TableKey(b"")),
        );
        assert!(split_ranges.len() > origin.len());
        let mut sequence_index: HashMap<u64, Vec<DeleteRangeTombstone>> = HashMap::default();
        for tombstone in split_ranges {
            let data = sequence_index.entry(tombstone.sequence).or_default();
            data.push(tombstone);
        }
        assert_eq!(SEQUENCE_COUNT, sequence_index.len() as u64);
        for (sequence, mut data) in sequence_index {
            data.sort();
            for i in 1..data.len() {
                assert_eq!(data[i - 1].end_user_key, data[i].start_user_key);
            }
            assert_eq!(
                data[0].start_user_key,
                origin[sequence as usize - 1].start_user_key
            );
            assert_eq!(
                data.last().unwrap().end_user_key,
                origin[sequence as usize - 1].end_user_key
            );
        }
    }
}

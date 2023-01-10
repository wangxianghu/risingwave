// Copyright 2023 Singularity Data
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

use async_trait::async_trait;
use futures::stream::BoxStream;
use risingwave_hummock_sdk::table_stats::TableStatsMap;
use risingwave_hummock_sdk::{
    HummockEpoch, HummockSstableId, HummockVersionId, LocalSstableInfo, SstIdRange,
};
use risingwave_pb::hummock::{
    CompactTask, CompactTaskProgress, CompactionGroup, HummockSnapshot, HummockVersion, VacuumTask,
};

use crate::error::Result;

pub type CompactTaskItem =
    std::result::Result<risingwave_pb::hummock::SubscribeCompactTasksResponse, tonic::Status>;

#[async_trait]
pub trait HummockMetaClient: Send + Sync + 'static {
    async fn unpin_version_before(&mut self, unpin_version_before: HummockVersionId) -> Result<()>;
    async fn get_current_version(&mut self) -> Result<HummockVersion>;
    async fn pin_snapshot(&mut self) -> Result<HummockSnapshot>;
    async fn unpin_snapshot(&mut self) -> Result<()>;
    async fn unpin_snapshot_before(&mut self, pinned_epochs: HummockEpoch) -> Result<()>;
    async fn get_epoch(&mut self) -> Result<HummockSnapshot>;
    async fn get_new_sst_ids(&mut self, number: u32) -> Result<SstIdRange>;
    async fn report_compaction_task(
        &mut self,
        compact_task: CompactTask,
        table_stats_change: TableStatsMap,
    ) -> Result<()>;
    async fn report_compaction_task_progress(
        &mut self,
        progress: Vec<CompactTaskProgress>,
    ) -> Result<()>;
    // We keep `commit_epoch` only for test/benchmark.
    async fn commit_epoch(
        &mut self,
        epoch: HummockEpoch,
        sstables: Vec<LocalSstableInfo>,
    ) -> Result<()>;
    async fn update_current_epoch(&self, epoch: HummockEpoch) -> Result<()>;

    async fn subscribe_compact_tasks(
        &mut self,
        max_concurrent_task_number: u64,
    ) -> Result<BoxStream<'static, CompactTaskItem>>;
    async fn report_vacuum_task(&mut self, vacuum_task: VacuumTask) -> Result<()>;
    async fn get_compaction_groups(&mut self) -> Result<Vec<CompactionGroup>>;
    async fn trigger_manual_compaction(
        &mut self,
        compaction_group_id: u64,
        table_id: u32,
        level: u32,
    ) -> Result<()>;
    async fn report_full_scan_task(&mut self, sst_ids: Vec<HummockSstableId>) -> Result<()>;
    async fn trigger_full_gc(&mut self, sst_retention_time_sec: u64) -> Result<()>;
}

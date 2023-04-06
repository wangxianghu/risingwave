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

use std::sync::Arc;

use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::stream_plan::TopNNode;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::AppendOnlyTopNExecutor;

pub struct AppendOnlyTopNExecutorBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for AppendOnlyTopNExecutorBuilder {
    type Node = TopNNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let table = node.get_table()?;
        stream.streaming_metrics.actor_info_collector.add_table(
            table.id.into(),
            params.actor_context.id,
            &table.name,
        );
        let vnodes = params.vnode_bitmap.map(Arc::new);
        let state_table = StateTable::from_table_catalog(table, store, vnodes).await;
        let storage_key = table
            .get_pk()
            .iter()
            .map(ColumnOrder::from_protobuf)
            .collect();
        let order_by = node
            .order_by
            .iter()
            .map(ColumnOrder::from_protobuf)
            .collect();

        assert_eq!(&params.pk_indices, input.pk_indices());
        if node.with_ties {
            Ok(AppendOnlyTopNExecutor::new_with_ties(
                input,
                params.actor_context,
                storage_key,
                (node.offset as usize, node.limit as usize),
                order_by,
                params.executor_id,
                state_table,
            )?
            .boxed())
        } else {
            Ok(AppendOnlyTopNExecutor::new_without_ties(
                input,
                params.actor_context,
                storage_key,
                (node.offset as usize, node.limit as usize),
                order_by,
                params.executor_id,
                state_table,
            )?
            .boxed())
        }
    }
}

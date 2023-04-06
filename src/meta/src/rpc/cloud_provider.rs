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

use std::collections::HashMap;

use aws_config::retry::RetryConfig;
use aws_sdk_ec2::model::{Filter, VpcEndpointType};
use itertools::Itertools;
use risingwave_pb::catalog::connection::PrivateLinkService;

use crate::MetaResult;

const CLOUD_PROVIDER_AWS: &str = "aws";

#[derive(Clone)]
pub struct AwsEc2Client {
    client: aws_sdk_ec2::Client,
    /// `vpc_id`: The VPC of the running RisingWave instance
    vpc_id: String,
    security_group_id: String,
}

impl AwsEc2Client {
    pub async fn new(vpc_id: &str, security_group_id: &str) -> Self {
        let sdk_config = aws_config::from_env()
            .retry_config(RetryConfig::standard().with_max_attempts(4))
            .load()
            .await;
        let client = aws_sdk_ec2::Client::new(&sdk_config);

        Self {
            client,
            vpc_id: vpc_id.to_string(),
            security_group_id: security_group_id.to_string(),
        }
    }

    /// `service_name`: The name of the endpoint service we want to access
    pub async fn create_aws_private_link(
        &self,
        service_name: &str,
        az_ids: &[String],
    ) -> MetaResult<PrivateLinkService> {
        let subnet_and_azs = self.describe_subnets(&self.vpc_id, az_ids).await?;

        let subnet_ids: Vec<String> = subnet_and_azs.iter().map(|(id, _, _)| id.clone()).collect();
        let az_to_azid_map: HashMap<String, String> = subnet_and_azs
            .into_iter()
            .map(|(_, az, az_id)| (az, az_id))
            .collect();

        let (endpoint_id, endpoint_dns_names) = self
            .create_vpc_endpoint(
                &self.vpc_id,
                service_name,
                &self.security_group_id,
                &subnet_ids,
            )
            .await?;

        // The number of returned DNS names may not equal to the input AZs,
        // because some AZs may not have a subnet in the RW VPC
        let mut azid_to_dns_map = HashMap::new();
        for dns_name in &endpoint_dns_names {
            for az in az_to_azid_map.keys() {
                if dns_name.contains(az) {
                    azid_to_dns_map
                        .insert(az_to_azid_map.get(az).unwrap().clone(), dns_name.clone());
                    break;
                }
            }
        }

        Ok(PrivateLinkService {
            provider: CLOUD_PROVIDER_AWS.to_string(),
            service_name: service_name.to_string(),
            endpoint_id,
            dns_entries: azid_to_dns_map,
        })
    }

    async fn describe_subnets(
        &self,
        vpc_id: &str,
        az_ids: &[String],
    ) -> MetaResult<Vec<(String, String, String)>> {
        let vpc_filter = Filter::builder().name("vpc-id").values(vpc_id).build();
        let az_filter = Filter::builder()
            .name("availability-zone-id")
            .set_values(Some(Vec::from(az_ids)))
            .build();
        let output = self
            .client
            .describe_subnets()
            .set_filters(Some(vec![vpc_filter, az_filter]))
            .send()
            .await?;

        let subnets = output
            .subnets
            .unwrap_or_default()
            .into_iter()
            .unique_by(|s| s.availability_zone().unwrap_or_default().to_string())
            .map(|s| {
                (
                    s.subnet_id.unwrap_or_default(),
                    s.availability_zone.unwrap_or_default(),
                    s.availability_zone_id.unwrap_or_default(),
                )
            })
            .collect();
        Ok(subnets)
    }

    async fn create_vpc_endpoint(
        &self,
        vpc_id: &str,
        service_name: &str,
        security_group_id: &str,
        subnet_ids: &[String],
    ) -> MetaResult<(String, Vec<String>)> {
        let output = self
            .client
            .create_vpc_endpoint()
            .vpc_endpoint_type(VpcEndpointType::Interface)
            .vpc_id(vpc_id)
            .security_group_ids(security_group_id)
            .service_name(service_name)
            .set_subnet_ids(Some(subnet_ids.to_owned()))
            .send()
            .await?;

        let endpoint = output.vpc_endpoint().unwrap();
        let mut dns_names = Vec::new();
        if let Some(dns_entries) = endpoint.dns_entries() {
            dns_entries.iter().for_each(|e| {
                if let Some(dns_name) = e.dns_name() {
                    dns_names.push(dns_name.to_string());
                }
            });
        }

        Ok((
            endpoint.vpc_endpoint_id().unwrap_or_default().to_string(),
            dns_names,
        ))
    }
}

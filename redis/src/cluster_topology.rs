//! This module provides the functionality to refresh and calculate the cluster topology for Redis Cluster.

use crate::cluster::get_connection_addr;
use crate::cluster_routing::Slot;
use crate::cluster_slotmap::{ReadFromReplicaStrategy, SlotMap};
use crate::{cluster::TlsMode, ErrorKind, RedisError, RedisResult, Value};
use derivative::Derivative;
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// The default number of refersh topology retries
pub const DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES: usize = 3;
/// The default timeout for retrying topology refresh
pub const DEFAULT_REFRESH_SLOTS_RETRY_TIMEOUT: Duration = Duration::from_secs(1);
/// The default initial interval for retrying topology refresh
pub const DEFAULT_REFRESH_SLOTS_RETRY_INITIAL_INTERVAL: Duration = Duration::from_millis(500);

pub(crate) const SLOT_SIZE: u16 = 16384;
pub(crate) type TopologyHash = u64;

#[derive(Derivative)]
#[derivative(PartialEq, Eq)]
#[derive(Debug)]
pub(crate) struct TopologyView {
    pub(crate) hash_value: TopologyHash,
    #[derivative(PartialEq = "ignore")]
    pub(crate) nodes_count: u16,
    #[derivative(PartialEq = "ignore")]
    slots_and_count: (u16, Vec<Slot>),
}

pub(crate) fn slot(key: &[u8]) -> u16 {
    crc16::State::<crc16::XMODEM>::calculate(key) % SLOT_SIZE
}

fn get_hashtag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|v| *v == b'{');
    let open = match open {
        Some(open) => open,
        None => return None,
    };

    let close = key[open..].iter().position(|v| *v == b'}');
    let close = match close {
        Some(close) => close,
        None => return None,
    };

    let rv = &key[open + 1..open + close];
    if rv.is_empty() {
        None
    } else {
        Some(rv)
    }
}

/// Returns the slot that matches `key`.
pub fn get_slot(key: &[u8]) -> u16 {
    let key = match get_hashtag(key) {
        Some(tag) => tag,
        None => key,
    };

    slot(key)
}

// Parse slot data from raw redis value.
pub(crate) fn parse_and_count_slots(
    raw_slot_resp: &Value,
    tls: Option<TlsMode>,
    // The DNS address of the node from which `raw_slot_resp` was received.
    addr_of_answering_node: &str,
) -> RedisResult<(u16, Vec<Slot>)> {
    // Parse response.
    let mut slots = Vec::with_capacity(2);
    let mut count = 0;

    if let Value::Array(items) = raw_slot_resp {
        let mut iter = items.iter();
        while let Some(Value::Array(item)) = iter.next() {
            if item.len() < 3 {
                continue;
            }

            let start = if let Value::Int(start) = item[0] {
                start as u16
            } else {
                continue;
            };

            let end = if let Value::Int(end) = item[1] {
                end as u16
            } else {
                continue;
            };

            let mut nodes: Vec<String> = item
                .iter()
                .skip(2)
                .filter_map(|node| {
                    if let Value::Array(node) = node {
                        if node.len() < 2 {
                            return None;
                        }
                        // According to the CLUSTER SLOTS documentation:
                        // If the received hostname is an empty string or NULL, clients should utilize the hostname of the responding node.
                        // However, if the received hostname is "?", it should be regarded as an indication of an unknown node.
                        let hostname = if let Value::BulkString(ref ip) = node[0] {
                            let hostname = String::from_utf8_lossy(ip);
                            if hostname.is_empty() {
                                addr_of_answering_node.into()
                            } else if hostname == "?" {
                                return None;
                            } else {
                                hostname
                            }
                        } else if let Value::Nil = node[0] {
                            addr_of_answering_node.into()
                        } else {
                            return None;
                        };
                        if hostname.is_empty() {
                            return None;
                        }

                        let port = if let Value::Int(port) = node[1] {
                            port as u16
                        } else {
                            return None;
                        };
                        Some(
                            get_connection_addr(hostname.into_owned(), port, tls, None).to_string(),
                        )
                    } else {
                        None
                    }
                })
                .collect();

            if nodes.is_empty() {
                continue;
            }
            count += end - start;

            let replicas = nodes.split_off(1);
            slots.push(Slot::new(start, end, nodes.pop().unwrap(), replicas));
        }
    }
    if slots.is_empty() {
        return Err(RedisError::from((
            ErrorKind::ResponseError,
            "Error parsing slots: No healthy node found",
            format!("Raw slot map response: {:?}", raw_slot_resp),
        )));
    }
    // we sort the slots, because different nodes in a cluster might return the same slot view
    // in different orders, which might cause the views to be considered evaluated as not equal.
    slots.sort_unstable_by(|first, second| match first.start().cmp(&second.start()) {
        core::cmp::Ordering::Equal => first.end().cmp(&second.end()),
        ord => ord,
    });

    Ok((count, slots))
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub(crate) fn calculate_topology<'a>(
    topology_views: impl Iterator<Item = (&'a str, &'a Value)>,
    curr_retry: usize,
    tls_mode: Option<TlsMode>,
    num_of_queried_nodes: usize,
    read_from_replica: ReadFromReplicaStrategy,
) -> RedisResult<(SlotMap, TopologyHash)> {
    let mut hash_view_map = HashMap::new();
    for (host, view) in topology_views {
        if let Ok(slots_and_count) = parse_and_count_slots(view, tls_mode, host) {
            let hash_value = calculate_hash(&slots_and_count);
            let topology_entry = hash_view_map.entry(hash_value).or_insert(TopologyView {
                hash_value,
                nodes_count: 0,
                slots_and_count,
            });
            topology_entry.nodes_count += 1;
        }
    }
    let mut non_unique_max_node_count = false;
    let mut vec_iter = hash_view_map.into_values();
    let mut most_frequent_topology = match vec_iter.next() {
        Some(view) => view,
        None => {
            return Err(RedisError::from((
                ErrorKind::ResponseError,
                "No topology views found",
            )));
        }
    };
    // Find the most frequent topology view
    for curr_view in vec_iter {
        match most_frequent_topology
            .nodes_count
            .cmp(&curr_view.nodes_count)
        {
            std::cmp::Ordering::Less => {
                most_frequent_topology = curr_view;
                non_unique_max_node_count = false;
            }
            std::cmp::Ordering::Greater => continue,
            std::cmp::Ordering::Equal => {
                non_unique_max_node_count = true;
                let seen_slot_count = most_frequent_topology.slots_and_count.0;

                // We choose as the greater view the one with higher slot coverage.
                if let std::cmp::Ordering::Less = seen_slot_count.cmp(&curr_view.slots_and_count.0)
                {
                    most_frequent_topology = curr_view;
                }
            }
        }
    }

    let parse_and_built_result = |most_frequent_topology: TopologyView| {
        let slots_data = most_frequent_topology.slots_and_count.1;

        Ok((
            SlotMap::new(slots_data, read_from_replica),
            most_frequent_topology.hash_value,
        ))
    };

    if non_unique_max_node_count {
        // More than a single most frequent view was found
        // If we reached the last retry, or if we it's a 2-nodes cluster, we'll return a view with the highest slot coverage, and that is one of most agreed on views.
        if curr_retry >= DEFAULT_NUMBER_OF_REFRESH_SLOTS_RETRIES || num_of_queried_nodes < 3 {
            return parse_and_built_result(most_frequent_topology);
        }
        return Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error: Failed to obtain a majority in topology views",
        )));
    }

    // The rate of agreement of the topology view is determined by assessing the number of nodes that share this view out of the total number queried
    let agreement_rate = most_frequent_topology.nodes_count as f32 / num_of_queried_nodes as f32;
    const MIN_AGREEMENT_RATE: f32 = 0.2;
    if agreement_rate >= MIN_AGREEMENT_RATE {
        parse_and_built_result(most_frequent_topology)
    } else {
        Err(RedisError::from((
            ErrorKind::ResponseError,
            "Slot refresh error: The accuracy of the topology view is too low",
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_routing::SlotAddrs;

    #[test]
    fn test_get_hashtag() {
        assert_eq!(get_hashtag(&b"foo{bar}baz"[..]), Some(&b"bar"[..]));
        assert_eq!(get_hashtag(&b"foo{}{baz}"[..]), None);
        assert_eq!(get_hashtag(&b"foo{{bar}}zap"[..]), Some(&b"{bar"[..]));
    }

    fn slot_value(start: u16, end: u16, node: &str, port: u16) -> Value {
        Value::Array(vec![
            Value::Int(start as i64),
            Value::Int(end as i64),
            Value::Array(vec![
                Value::BulkString(node.as_bytes().to_vec()),
                Value::Int(port as i64),
            ]),
        ])
    }

    #[test]
    fn parse_slots_returns_slots_in_same_order() {
        let view1 = Value::Array(vec![
            slot_value(0, 4000, "node1", 6379),
            slot_value(4001, 8000, "node1", 6380),
            slot_value(8001, 16383, "node1", 6379),
        ]);

        let view2 = Value::Array(vec![
            slot_value(8001, 16383, "node1", 6379),
            slot_value(0, 4000, "node1", 6379),
            slot_value(4001, 8000, "node1", 6380),
        ]);

        let res1 = parse_and_count_slots(&view1, None, "foo").unwrap();
        let res2 = parse_and_count_slots(&view2, None, "foo").unwrap();
        assert_eq!(res1.0, res2.0);
        assert_eq!(res1.1.len(), res2.1.len());
        let check =
            res1.1.into_iter().zip(res2.1).all(|(first, second)| {
                first.start() == second.start() && first.end() == second.end()
            });
        assert!(check);
    }

    #[test]
    fn parse_slots_returns_slots_with_host_name_if_missing() {
        let view = Value::Array(vec![slot_value(0, 4000, "", 6379)]);

        let (slot_count, slots) = parse_and_count_slots(&view, None, "node").unwrap();
        assert_eq!(slot_count, 4000);
        assert_eq!(slots[0].master(), "node:6379");
    }

    #[test]
    fn should_parse_and_hash_regardless_of_missing_host_name_and_order() {
        let view1 = Value::Array(vec![
            slot_value(0, 4000, "", 6379),
            slot_value(4001, 8000, "node2", 6380),
            slot_value(8001, 16383, "node3", 6379),
        ]);

        let view2 = Value::Array(vec![
            slot_value(8001, 16383, "", 6379),
            slot_value(0, 4000, "node1", 6379),
            slot_value(4001, 8000, "node2", 6380),
        ]);

        let res1 = parse_and_count_slots(&view1, None, "node1").unwrap();
        let res2 = parse_and_count_slots(&view2, None, "node3").unwrap();

        assert_eq!(calculate_hash(&res1), calculate_hash(&res2));
        assert_eq!(res1.0, res2.0);
        assert_eq!(res1.1.len(), res2.1.len());
        let equality_check =
            res1.1.into_iter().zip(res2.1).all(|(first, second)| {
                first.start() == second.start() && first.end() == second.end()
            });
        assert!(equality_check);
    }

    enum ViewType {
        SingleNodeViewFullCoverage,
        SingleNodeViewMissingSlots,
        TwoNodesViewFullCoverage,
        TwoNodesViewMissingSlots,
    }
    fn get_view(view_type: &ViewType) -> (&str, Value) {
        match view_type {
            ViewType::SingleNodeViewFullCoverage => (
                "first",
                Value::Array(vec![slot_value(0, 16383, "node1", 6379)]),
            ),
            ViewType::SingleNodeViewMissingSlots => (
                "second",
                Value::Array(vec![slot_value(0, 4000, "node1", 6379)]),
            ),
            ViewType::TwoNodesViewFullCoverage => (
                "third",
                Value::Array(vec![
                    slot_value(0, 4000, "node1", 6379),
                    slot_value(4001, 16383, "node2", 6380),
                ]),
            ),
            ViewType::TwoNodesViewMissingSlots => (
                "fourth",
                Value::Array(vec![
                    slot_value(0, 3000, "node3", 6381),
                    slot_value(4001, 16383, "node4", 6382),
                ]),
            ),
        }
    }

    fn get_node_addr(name: &str, port: u16) -> SlotAddrs {
        SlotAddrs::new(format!("{name}:{port}"), Vec::new())
    }

    #[test]
    fn test_topology_calculator_4_nodes_queried_has_a_majority_success() {
        // 4 nodes queried (1 error): Has a majority, single_node_view should be chosen
        let queried_nodes: usize = 4;
        let topology_results = vec![
            get_view(&ViewType::SingleNodeViewFullCoverage),
            get_view(&ViewType::SingleNodeViewFullCoverage),
            get_view(&ViewType::TwoNodesViewFullCoverage),
        ];

        let (topology_view, _) = calculate_topology(
            topology_results.iter().map(|(addr, value)| (*addr, value)),
            1,
            None,
            queried_nodes,
            ReadFromReplicaStrategy::AlwaysFromPrimary,
        )
        .unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let node_1 = get_node_addr("node1", 6379);
        let expected: Vec<&SlotAddrs> = vec![&node_1];
        assert_eq!(res, expected);
    }

    #[test]
    fn test_topology_calculator_3_nodes_queried_no_majority_has_more_retries_raise_error() {
        // 3 nodes queried: No majority, should return an error
        let queried_nodes = 3;
        let topology_results = vec![
            get_view(&ViewType::SingleNodeViewFullCoverage),
            get_view(&ViewType::TwoNodesViewFullCoverage),
            get_view(&ViewType::TwoNodesViewMissingSlots),
        ];
        let topology_view = calculate_topology(
            topology_results.iter().map(|(addr, value)| (*addr, value)),
            1,
            None,
            queried_nodes,
            ReadFromReplicaStrategy::AlwaysFromPrimary,
        );
        assert!(topology_view.is_err());
    }

    #[test]
    fn test_topology_calculator_3_nodes_queried_no_majority_last_retry_success() {
        // 3 nodes queried:: No majority, last retry, should get the view that has a full slot coverage
        let queried_nodes = 3;
        let topology_results = vec![
            get_view(&ViewType::SingleNodeViewMissingSlots),
            get_view(&ViewType::TwoNodesViewFullCoverage),
            get_view(&ViewType::TwoNodesViewMissingSlots),
        ];
        let (topology_view, _) = calculate_topology(
            topology_results.iter().map(|(addr, value)| (*addr, value)),
            3,
            None,
            queried_nodes,
            ReadFromReplicaStrategy::AlwaysFromPrimary,
        )
        .unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let node_1 = get_node_addr("node1", 6379);
        let node_2 = get_node_addr("node2", 6380);
        let expected: Vec<&SlotAddrs> = vec![&node_1, &node_2];
        assert_eq!(res, expected);
    }

    #[test]
    fn test_topology_calculator_2_nodes_queried_no_majority_return_full_slot_coverage_view() {
        // 2 nodes queried: No majority, should get the view that has a full slot coverage
        let queried_nodes = 2;
        let topology_results = vec![
            get_view(&ViewType::TwoNodesViewFullCoverage),
            get_view(&ViewType::TwoNodesViewMissingSlots),
        ];
        let (topology_view, _) = calculate_topology(
            topology_results.iter().map(|(addr, value)| (*addr, value)),
            1,
            None,
            queried_nodes,
            ReadFromReplicaStrategy::AlwaysFromPrimary,
        )
        .unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let node_1 = get_node_addr("node1", 6379);
        let node_2 = get_node_addr("node2", 6380);
        let expected: Vec<&SlotAddrs> = vec![&node_1, &node_2];
        assert_eq!(res, expected);
    }

    #[test]
    fn test_topology_calculator_2_nodes_queried_no_majority_no_full_coverage_prefer_fuller_coverage(
    ) {
        //  2 nodes queried: No majority, no full slot coverage, should return error
        let queried_nodes = 2;
        let topology_results = vec![
            get_view(&ViewType::SingleNodeViewMissingSlots),
            get_view(&ViewType::TwoNodesViewMissingSlots),
        ];
        let (topology_view, _) = calculate_topology(
            topology_results.iter().map(|(addr, value)| (*addr, value)),
            1,
            None,
            queried_nodes,
            ReadFromReplicaStrategy::AlwaysFromPrimary,
        )
        .unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let node_1 = get_node_addr("node3", 6381);
        let node_2 = get_node_addr("node4", 6382);
        let expected: Vec<&SlotAddrs> = vec![&node_1, &node_2];
        assert_eq!(res, expected);
    }

    #[test]
    fn test_topology_calculator_3_nodes_queried_no_full_coverage_prefer_majority() {
        //  2 nodes queried: No majority, no full slot coverage, should return error
        let queried_nodes = 2;
        let topology_results = vec![
            get_view(&ViewType::SingleNodeViewMissingSlots),
            get_view(&ViewType::TwoNodesViewMissingSlots),
            get_view(&ViewType::SingleNodeViewMissingSlots),
        ];
        let (topology_view, _) = calculate_topology(
            topology_results.iter().map(|(addr, value)| (*addr, value)),
            1,
            None,
            queried_nodes,
            ReadFromReplicaStrategy::AlwaysFromPrimary,
        )
        .unwrap();
        let res: Vec<_> = topology_view.values().collect();
        let node_1 = get_node_addr("node1", 6379);
        let expected: Vec<&SlotAddrs> = vec![&node_1];
        assert_eq!(res, expected);
    }
}

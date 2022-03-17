#!./python/bin/python3
# -*- coding: UTF-8 -*-
import copy
import os
import sys

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from hyperion_client.deploy_info import DeployInfo
from hyperion_client.directory_info import DirectoryInfo
from hyperion_client.hyperion_inner_client.inner_node_info import InnerNodeInfo


def get_host_random_dirs_count(host):
    random_dirs = DirectoryInfo().get_storage_data_dir_by_hostname(host, 'random')
    random_dirs_count = len(random_dirs)
    if random_dirs_count == 0:
        raise Exception('host %s has no random dirs' % host)
    return random_dirs_count


def get_host_mem_gb(host):
    return InnerNodeInfo.get_instance().get_machine_mem_gb(host)


def get_dynamic_config_value(key, is_simplified_cluster, random_dirs_count, host_mem_gb):
    if key == "maintenance_manager_num_threads":
        return str(random_dirs_count * 3)

    if is_simplified_cluster:
        # set to 1/4 of host's memory size
        memory_limit_hard_bytes = int((int(host_mem_gb) << 30) * 0.25)
    else:
        # TODO(yingchun): it's waste to use only 12 GB on some large memory hosts, we could improve it later
        memory_limit_hard_bytes = 6 << 30 if host_mem_gb <= 70 else 12 << 30
    if key == "memory_limit_hard_bytes":
        return str(memory_limit_hard_bytes)
    elif key == "block_cache_capacity_mb":
        block_cache_capacity_mb = int((int(memory_limit_hard_bytes) >> 20) * 0.3)
        return str(block_cache_capacity_mb)
    else:
        raise Exception('key [%s] is not a dynamic config' % key)


KUDU_COMMON_CONFIG = {
    'max_log_size': '100',
    'superuser_acl': 'sa_cluster,kudu,root',
    'trusted_subnets': '0.0.0.0/0',
    'unlock_experimental_flags': 'true',
    'unlock_unsafe_flags': 'true',
    'redact': 'none',
    'rpc_authentication': 'disabled',
    'rpc_encryption': 'disabled',
    'raft_heartbeat_interval_ms': '1000'
}

KUDU_MASTER_CONFIG = {
    'auto_rebalancing_enabled': 'true',
    'auto_rebalancing_load_imbalance_threshold': '0.5',
    'master_default_reserve_trashed_table_seconds': '43200',
    'table_locations_cache_capacity_mb': '64'
}


KUDU_TSERVER_CONFIG = {
    'flush_threshold_mb': '128',
    'flush_threshold_secs': '900',
    'log_container_max_blocks': '100000',
    'log_container_max_size': '6442450944',
    'log_container_metadata_max_size': '4194304',
    'log_container_metadata_runtime_compact': 'true',
    'log_container_preallocate_bytes': '4194304',
    'log_max_segments_to_retain': '10',
    'log_min_segments_to_retain': '1',
    'log_target_replay_size_mb': '256',
    'maintenance_manager_num_flush_threads': '1' if DeployInfo().get_simplified_cluster() else '0',
    'maintenance_op_multiplier': '1.2',
    'num_tablets_to_open_simultaneously': '8',
    'rowset_metadata_store_keys': 'true',
    'scanner_ttl_ms': '180000',
    'tablet_delta_store_minor_compact_max': '200',
    'tablet_history_max_age_sec': '600',
    'maintenance_manager_num_threads': "",  # 空value项会在设置时动态获取
    'memory_limit_hard_bytes': "",
    'block_cache_capacity_mb': ""
}

CDH_COMMON_CONFIG = copy.deepcopy(KUDU_COMMON_CONFIG)
CDH_COMMON_CONFIG.update({'webserver_doc_root': os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'soku/kudu/lib/kudu/www')})
CDH_UPDATE_CONFIG = {
    'KUDU_COMMON': CDH_COMMON_CONFIG,
    'KUDU_MASTER': KUDU_MASTER_CONFIG,
    'KUDU_TSERVER': KUDU_TSERVER_CONFIG
}

MOTHERSHIP_MASTER_CONFIG = copy.deepcopy(KUDU_COMMON_CONFIG)
MOTHERSHIP_MASTER_CONFIG.update(KUDU_MASTER_CONFIG)
MOTHERSHIP_TSERVER_CONFIG = copy.deepcopy(KUDU_COMMON_CONFIG)
MOTHERSHIP_TSERVER_CONFIG.update(KUDU_TSERVER_CONFIG)
MOTHERSHIP_UPDATE_CONFIG = {
    'KUDU_MASTER': MOTHERSHIP_MASTER_CONFIG,
    'KUDU_TSERVER': MOTHERSHIP_TSERVER_CONFIG
}

#!./python/bin/python3
# -*- coding: UTF-8 -*-
import copy
import os
import sys

sys.path.append(os.environ['SENSORS_PLATFORM_HOME'])
from construction_blueprint.installer_constants import HadoopDistributionType

sys.path.append(os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '..', 'armada', 'hyperion'))
from hyperion_client.deploy_info import DeployInfo

KUDU_COMMON_CONFIG = {
    'max_log_size': '100',
    'superuser_acl': 'sa_cluster,kudu,root',
    'trusted_subnets': '0.0.0.0/0',
    'unlock_experimental_flags': 'true',
    'unlock_unsafe_flags': 'true',
    'redact': 'none',
    'rpc_authentication': 'disabled',
    'rpc_encryption': 'disabled'
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
    'tablet_history_max_age_sec': '600'
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

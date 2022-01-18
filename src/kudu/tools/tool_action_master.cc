// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <algorithm>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/master_runner.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tools/ksck.h"
#include "kudu/tools/ksck_remote.h"
#include "kudu/tools/master_rebuilder.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/init.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/string_case.h"

DECLARE_bool(force);
DECLARE_int64(timeout_ms);
DECLARE_string(columns);

using kudu::master::ConnectToMasterRequestPB;
using kudu::master::ConnectToMasterResponsePB;
using kudu::master::ListMastersRequestPB;
using kudu::master::ListMastersResponsePB;
using kudu::master::Master;
using kudu::master::MasterServiceProxy;
using kudu::master::RefreshAuthzCacheRequestPB;
using kudu::master::RefreshAuthzCacheResponsePB;
using kudu::consensus::RaftPeerPB;
using kudu::rpc::RpcController;
using std::cout;
using std::endl;
using std::map;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {
namespace {

const char* const kTabletServerAddressArg = "tserver_address";
const char* const kTabletServerAddressDesc = "Address of a Kudu tablet server "
    "of form 'hostname:port'. Port may be omitted if the tablet server is "
    "bound to the default port.";
const char* const kFlagArg = "flag";
const char* const kValueArg = "value";

Status MasterGetFlags(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kMasterAddressArg);
  return PrintServerFlags(address, Master::kDefaultPort);
}

Status MasterRun(const RunnerContext& context) {
  RETURN_NOT_OK(InitKudu());

  // Enable redaction by default. Unlike most tools, we don't want user data
  // printed to the console/log to be shown by default.
  CHECK_NE("", google::SetCommandLineOptionWithMode("redact",
      "all", google::FlagSettingMode::SET_FLAGS_DEFAULT));

  master::SetMasterFlagDefaults();
  return master::RunMasterServer();
}

Status MasterSetFlag(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kMasterAddressArg);
  const string& flag = FindOrDie(context.required_args, kFlagArg);
  const string& value = FindOrDie(context.required_args, kValueArg);
  return SetServerFlag(address, Master::kDefaultPort, flag, value);
}

Status MasterStatus(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kMasterAddressArg);
  return PrintServerStatus(address, Master::kDefaultPort);
}

Status MasterTimestamp(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kMasterAddressArg);
  return PrintServerTimestamp(address, Master::kDefaultPort);
}

Status ListMasters(const RunnerContext& context) {
  LeaderMasterProxy proxy;
  RETURN_NOT_OK(proxy.Init(context));

  ListMastersRequestPB req;
  ListMastersResponsePB resp;

  RETURN_NOT_OK((proxy.SyncRpc<ListMastersRequestPB, ListMastersResponsePB>(
      req, &resp, "ListMasters", &MasterServiceProxy::ListMastersAsync)));

  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }

  DataTable table({});

  vector<ServerEntryPB> masters;
  std::copy_if(resp.masters().begin(), resp.masters().end(), std::back_inserter(masters),
               [](const ServerEntryPB& master) {
                 if (master.has_error()) {
                   LOG(WARNING) << "Failed to retrieve info for master: "
                                << StatusFromPB(master.error()).ToString();
                   return false;
                 }
                 return true;
               });

  auto hostport_to_string = [] (const HostPortPB& hostport) {
    return Substitute("$0:$1", hostport.host(), hostport.port());
  };

  for (const auto& column : strings::Split(FLAGS_columns, ",", strings::SkipEmpty())) {
    vector<string> values;
    if (iequals(column.ToString(), "uuid")) {
      for (const auto& master : masters) {
        values.push_back(master.instance_id().permanent_uuid());
      }
    } else if (iequals(column.ToString(), "cluster_id")) {
      for (const auto& master : masters) {
        values.emplace_back(master.has_cluster_id() ? master.cluster_id() : "");
      }
    } else if (iequals(column.ToString(), "seqno")) {
      for (const auto& master : masters) {
        values.push_back(std::to_string(master.instance_id().instance_seqno()));
      }
    } else if (iequals(column.ToString(), "rpc-addresses") ||
               iequals(column.ToString(), "rpc_addresses")) {
      for (const auto& master : masters) {
        values.push_back(JoinMapped(master.registration().rpc_addresses(),
                         hostport_to_string, ","));
      }
    } else if (iequals(column.ToString(), "http-addresses") ||
               iequals(column.ToString(), "http_addresses")) {
      for (const auto& master : masters) {
        values.push_back(JoinMapped(master.registration().http_addresses(),
                                    hostport_to_string, ","));
      }
    } else if (iequals(column.ToString(), "version")) {
      for (const auto& master : masters) {
        values.push_back(master.registration().software_version());
      }
    } else if (iequals(column.ToString(), "start_time")) {
      for (const auto& master : masters) {
        values.emplace_back(StartTimeToString(master.registration()));
      }
    } else if (iequals(column.ToString(), "role")) {
      for (const auto& master : masters) {
        values.emplace_back(RaftPeerPB::Role_Name(master.role()));
      }
    } else if (iequals(column.ToString(), "member_type")) {
      for (const auto& master : masters) {
        values.emplace_back(RaftPeerPB::MemberType_Name(master.member_type()));
      }
    } else {
      return Status::InvalidArgument("unknown column (--columns)", column);
    }
    table.AddColumn(column.ToString(), std::move(values));
  }

  RETURN_NOT_OK(table.PrintTo(cout));
  return Status::OK();
}

Status MasterDumpMemTrackers(const RunnerContext& context) {
  const auto& address = FindOrDie(context.required_args, kMasterAddressArg);
  return DumpMemTrackers(address, Master::kDefaultPort);
}

// Make sure the list of master addresses specified in 'master_addresses'
// corresponds to the actual list of masters addresses in the cluster,
// as reported in ConnectToMasterResponsePB::master_addrs.
Status VerifyMasterAddressList(const vector<string>& master_addresses) {
  map<string, set<string>> addresses_per_master;
  for (const auto& address : master_addresses) {
    unique_ptr<MasterServiceProxy> proxy;
    RETURN_NOT_OK(BuildProxy(address, Master::kDefaultPort, &proxy));

    RpcController ctl;
    ctl.set_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms));
    ConnectToMasterRequestPB req;
    ConnectToMasterResponsePB resp;
    RETURN_NOT_OK(proxy->ConnectToMaster(req, &resp, &ctl));
    const auto& resp_master_addrs = resp.master_addrs();
    if (resp_master_addrs.size() != master_addresses.size()) {
      const auto addresses_provided = JoinStrings(master_addresses, ",");
      const auto addresses_cluster_config = JoinMapped(
          resp_master_addrs,
          [](const HostPortPB& pb) {
            return Substitute("$0:$1", pb.host(), pb.port());
          }, ",");
      return Status::InvalidArgument(Substitute(
          "list of master addresses provided ($0) "
          "does not match the actual cluster configuration ($1) ",
          addresses_provided, addresses_cluster_config));
    }
    set<string> addr_set;
    for (const auto& hp : resp_master_addrs) {
      addr_set.emplace(Substitute("$0:$1", hp.host(), hp.port()));
    }
    addresses_per_master.emplace(address, std::move(addr_set));
  }

  bool mismatch = false;
  if (addresses_per_master.size() > 1) {
    const auto it_0 = addresses_per_master.cbegin();
    auto it_1 = addresses_per_master.begin();
    ++it_1;
    for (auto it = it_1; it != addresses_per_master.end(); ++it) {
      if (it->second != it_0->second) {
        mismatch = true;
        break;
      }
    }
  }

  if (mismatch) {
    string err_msg = Substitute("specified: ($0);",
                                JoinStrings(master_addresses, ","));
    for (const auto& e : addresses_per_master) {
      err_msg += Substitute(" from master $0: ($1);",
                            e.first, JoinStrings(e.second, ","));
    }
    return Status::ConfigurationError(
        Substitute("master address lists mismatch: $0", err_msg));
  }

  return Status::OK();
}

Status PrintRebuildReport(const RebuildReport& rebuild_report) {
  cout << "Rebuild Report" << endl;
  cout << "Tablet Servers" << endl;
  DataTable tserver_table({ "address", "status" });
  int bad_tservers = 0;
  for (const auto& pair : rebuild_report.tservers) {
    const Status& s = pair.second;
    tserver_table.AddRow({ pair.first, s.ToString() });
    if (!s.ok()) bad_tservers++;
  }
  RETURN_NOT_OK(tserver_table.PrintTo(cout));
  cout << Substitute("Rebuilt from $0 tablet servers, of which $1 had errors",
                     rebuild_report.tservers.size(), bad_tservers)
       << endl << endl;

  cout << "Replicas" << endl;
  DataTable replica_table({ "table", "tablet", "tablet server", "status" });
  int bad_replicas = 0;
  for (const auto& entry : rebuild_report.replicas) {
    const auto& key = entry.first;
    const Status& s = entry.second;
    replica_table.AddRow({ std::get<0>(key), std::get<1>(key), std::get<2>(key), s.ToString() });
    if (!s.ok()) bad_replicas++;
  }
  RETURN_NOT_OK(replica_table.PrintTo(cout));
  cout << Substitute("Rebuilt from $0 replicas, of which $1 had errors",
                     rebuild_report.replicas.size(), bad_replicas)
       << endl;

  return Status::OK();
}

Status RefreshAuthzCacheAtMaster(const string& master_address) {
  unique_ptr<MasterServiceProxy> proxy;
  RETURN_NOT_OK(BuildProxy(master_address, Master::kDefaultPort, &proxy));

  RpcController ctl;
  ctl.set_timeout(MonoDelta::FromMilliseconds(FLAGS_timeout_ms));

  RefreshAuthzCacheRequestPB req;
  RefreshAuthzCacheResponsePB resp;
  RETURN_NOT_OK(proxy->RefreshAuthzCache(req, &resp, &ctl));
  if (resp.has_error()) {
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

Status RefreshAuthzCache(const RunnerContext& context) {
  vector<string> master_addresses;
  RETURN_NOT_OK(ParseMasterAddresses(context, &master_addresses));

  if (!FLAGS_force) {
    // Make sure the list of master addresses specified for the command
    // matches the actual list of masters in the cluster.
    RETURN_NOT_OK(VerifyMasterAddressList(master_addresses));
  }

  // It makes sense to refresh privileges cache at every master in the cluster.
  // Otherwise, the authorization provider might return inconsistent results for
  // authz requests upon master leadership change.
  vector<Status> statuses;
  statuses.reserve(master_addresses.size());
  for (const auto& address : master_addresses) {
    auto status = RefreshAuthzCacheAtMaster(address);
    statuses.emplace_back(std::move(status));
  }
  DCHECK_EQ(master_addresses.size(), statuses.size());
  string err_str;
  for (auto i = 0; i < statuses.size(); ++i) {
    const auto& s = statuses[i];
    if (s.ok()) {
      continue;
    }
    err_str += Substitute(" error from master at $0: $1",
                          master_addresses[i], s.ToString());
  }
  if (err_str.empty()) {
    return Status::OK();
  }
  return Status::Incomplete(err_str);
}

Status RebuildMaster(const RunnerContext& context) {
  MasterRebuilder master_rebuilder(context.variadic_args);
  RETURN_NOT_OK(master_rebuilder.RebuildMaster());
  PrintRebuildReport(master_rebuilder.GetRebuildReport());
  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildMasterMode() {
  ModeBuilder builder("master");
  builder.Description("Operate on a Kudu Master");

  {
    unique_ptr<Action> action_refresh =
        ClusterActionBuilder("refresh", &RefreshAuthzCache)
        .Description("Refresh the authorization policies")
        .AddOptionalParameter(
            "force", boost::none,
            string(
                "Ignore mismatches of the specified and the actual lists "
                "of master addresses in the cluster"))
        .Build();

    unique_ptr<Mode> mode_authz_cache = ModeBuilder("authz_cache")
        .Description("Operate on the authz caches of the Kudu Masters")
        .AddAction(std::move(action_refresh))
        .Build();
    builder.AddMode(std::move(mode_authz_cache));
  }
  {
    unique_ptr<Action> dump_memtrackers =
        MasterActionBuilder("dump_memtrackers", &MasterDumpMemTrackers)
        .Description("Dump the memtrackers from a Kudu Master")
        .AddOptionalParameter("format")
        .AddOptionalParameter("memtracker_output")
        .Build();
    builder.AddAction(std::move(dump_memtrackers));
  }
  {
    unique_ptr<Action> get_flags =
        MasterActionBuilder("get_flags", &MasterGetFlags)
        .Description("Get the gflags for a Kudu Master")
        .AddOptionalParameter("all_flags")
        .AddOptionalParameter("flags")
        .AddOptionalParameter("flag_tags")
        .Build();
    builder.AddAction(std::move(get_flags));
  }
  {
    unique_ptr<Action> run =
        ActionBuilder("run", &MasterRun)
        .ProgramName("kudu-master")
        .Description("Run a Kudu Master")
        .ExtraDescription("Note: The master server is started in this process and "
                          "runs until interrupted.\n\n"
                          "The most common configuration flags are described below. "
                          "For all the configuration options pass --helpfull or see "
                          "https://kudu.apache.org/docs/configuration_reference.html"
                          "#kudu-master_supported")
        .AddOptionalParameter("master_addresses")
        // Even though fs_wal_dir is required, we don't want it to be positional argument.
        // This allows it to be passed as a standard flag.
        .AddOptionalParameter("fs_wal_dir")
        .AddOptionalParameter("fs_data_dirs")
        .AddOptionalParameter("fs_metadata_dir")
        .AddOptionalParameter("log_dir")
        // Unlike most tools we don't log to stderr by default to match the
        // kudu-master binary as closely as possible.
        .AddOptionalParameter("logtostderr", string("false"))
        .Build();
    builder.AddAction(std::move(run));
  }
  {
    unique_ptr<Action> set_flag =
        MasterActionBuilder("set_flag", &MasterSetFlag)
        .Description("Change a gflag value on a Kudu Master")
        .AddRequiredParameter({ kFlagArg, "Name of the gflag" })
        .AddRequiredParameter({ kValueArg, "New value for the gflag" })
        .AddOptionalParameter("force")
        .Build();
    builder.AddAction(std::move(set_flag));
  }
  {
    unique_ptr<Action> status =
        MasterActionBuilder("status", &MasterStatus)
        .Description("Get the status of a Kudu Master")
        .Build();
    builder.AddAction(std::move(status));
  }
  {
    unique_ptr<Action> timestamp =
        MasterActionBuilder("timestamp", &MasterTimestamp)
        .Description("Get the current timestamp of a Kudu Master")
        .Build();
    builder.AddAction(std::move(timestamp));
  }
  {
    unique_ptr<Action> list_masters =
        ClusterActionBuilder("list", &ListMasters)
        .Description("List masters in a Kudu cluster")
        .AddOptionalParameter(
            "columns",
            string("uuid,rpc-addresses,role"),
            string("Comma-separated list of master info fields to "
                   "include in output.\nPossible values: uuid, cluster_id"
                   "rpc-addresses, http-addresses, version, seqno, "
                   "start_time and role"))
        .AddOptionalParameter("format")
        .Build();
    builder.AddAction(std::move(list_masters));
  }

  {
    const char* rebuild_extra_description = "Attempts to create on-disk metadata\n"
        "that can be used by a non-replicated master to recover a Kudu cluster\n"
        "that has permanently lost its masters. It has a number of limitations:\n"
        " - Security metadata like cryptographic keys are not rebuilt. Tablet servers\n"
        "   and clients must be restarted before starting the new master in order to\n"
        "   communicate with the new master.\n"
        " - Table IDs are known only by the masters. Reconstructed tables will have\n"
        "   new IDs.\n"
        " - If a create, delete, or alter table was in progress when the masters were lost,\n"
        "   it may not be possible to restore the table.\n"
        " - If all replicas of a tablet are missing, it may not be able to recover the\n"
        "   table fully. Moreover, the rebuild tool cannot detect that a tablet is\n"
        "   missing.\n"
        " - It's not possible to determine the replication factor of a table from tablet\n"
        "   server metadata. The rebuild tool sets the replication factor of each\n"
        "   table to --default_num_replicas instead.\n"
        " - It's not possible to determine the next column id for a table from tablet\n"
        "   server metadata. Instead, the rebuilt tool sets the next column id to\n"
        "   a very large number.\n"
        " - Table metadata like comments, owners, and configurations are not stored on\n"
        "   tablet servers and are thus not restored.\n"
        "WARNING: This tool is potentially unsafe. Only use it when there is no\n"
        "possibility of recovering the original masters, and you know what you\n"
        "are doing.";
    unique_ptr<Action> unsafe_rebuild =
        ActionBuilder("unsafe_rebuild", &RebuildMaster)
        .Description("Rebuild a Kudu master from tablet server metadata")
        .ExtraDescription(rebuild_extra_description)
        .AddRequiredVariadicParameter({ kTabletServerAddressArg, kTabletServerAddressDesc })
        .AddOptionalParameter("default_num_replicas")
        .AddOptionalParameter("default_schema_version")
        .AddOptionalParameter("fs_data_dirs")
        .AddOptionalParameter("fs_metadata_dir")
        .AddOptionalParameter("fs_wal_dir")
        .Build();
    builder.AddAction(std::move(unsafe_rebuild));
  }

  return builder.Build();
}

} // namespace tools
} // namespace kudu


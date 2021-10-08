package org.apache.kudu.client;

import static org.apache.kudu.master.Master.ListTabletServersRequestPB;
import static org.apache.kudu.master.Master.ListTabletServersResponsePB;

import java.util.Map;
import java.util.HashMap;

import com.google.protobuf.Message;
import io.netty.util.Timer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.util.Pair;
import org.apache.kudu.Common.HostPortPB;

@InterfaceAudience.Private
public class ListTabletServersWithUUIDRequest extends KuduRpc<ListTabletServersWithUUIDResponse> {

  public ListTabletServersWithUUIDRequest(KuduTable masterTable, Timer timer, long timeoutMillis) {
    super(masterTable, timer, timeoutMillis);
  }

  @Override
  Message createRequestPB() {
    return ListTabletServersRequestPB.getDefaultInstance();
  }

  @Override
  String serviceName() {
    return MASTER_SERVICE_NAME;
  }

  @Override
  String method() {
    return "ListTabletServers";
  }

  @Override
  Pair<ListTabletServersWithUUIDResponse, Object> deserialize(CallResponse callResponse,
                                                              String tsUUID) throws KuduException {
    final ListTabletServersResponsePB.Builder respBuilder =
        ListTabletServersResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    int serversCount = respBuilder.getServersCount();

    Map<String, HostPortPB> serversWithHP = new HashMap<>(serversCount);
    Map<String, ServerInfo> serverInfoMap = new HashMap<>(serversCount);
    for (ListTabletServersResponsePB.Entry entry : respBuilder.getServersList()) {
      HostPortPB pb = entry.getRegistration().getRpcAddresses(0);
      String uuid = entry.getInstanceId().getPermanentUuid().toStringUtf8();
      serversWithHP.put(uuid, pb);
      ServerInfo serverInfo = new ServerInfo(uuid,
              new HostAndPort(pb.getHost(), pb.getPort()), null, entry.getLocation());
      serverInfoMap.put(uuid, serverInfo);
    }
    ListTabletServersWithUUIDResponse response =
            new ListTabletServersWithUUIDResponse(timeoutTracker.getElapsedMillis(),
                    tsUUID,
                    serversCount,
                    serversWithHP,
                    serverInfoMap);
    return new Pair<ListTabletServersWithUUIDResponse, Object>(
        response, respBuilder.hasError() ? respBuilder.getError() : null);
  }
}

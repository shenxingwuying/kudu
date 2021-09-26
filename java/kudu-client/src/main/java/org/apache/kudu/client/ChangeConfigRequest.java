package org.apache.kudu.client;

import io.netty.util.Timer;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import org.apache.kudu.Common.HostPortPB;
import org.apache.kudu.Schema;
import org.apache.kudu.client.ProtobufHelper.SchemaPBConversionFlags;
import org.apache.kudu.consensus.Consensus;
import org.apache.kudu.consensus.Metadata;
import org.apache.kudu.master.Master;
import org.apache.kudu.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

/**
 * RPC call to create new NON_VOTER peer for a tablet.
 */
@InterfaceAudience.Private
class ChangeConfigRequest extends KuduRpc<ChangeConfigResponse> {

  static final String CHANGE_CONFIG = "ChangeConfig";

  // The tabletId to add new voters.
  private final String tabletId;

  // HostPort of the peer to be added.
  private final HostPortPB targetHostPortPB; 

  // Add to which ts. Respond to the uuid of targetHostPortPB.
  // In this special case, it should be always beef.
  private final String replicaId; 

  // Partition Key use for indicating the dest Tablet for rpc use.
  private final byte[] partitionKey;

  ChangeConfigRequest(KuduTable masterTable, String tabletId,
                      RemoteTablet remoteTablet, HostPortPB targetHostPortPB, 
                      String replicaId, Timer timer, long timeoutMillis) {
    super(masterTable, timer, timeoutMillis);
    this.tabletId = tabletId;
    this.replicaId = replicaId;
    this.targetHostPortPB = targetHostPortPB;
    // TODO(ningw) Test whether it works for multi servers.
    this.setTablet(remoteTablet);
    // This is kind of hack. 
    // No warranty when range schema is introduced in.
    if (remoteTablet.getPartition().getPartitionKeyStart() == null || 
        remoteTablet.getPartition().getPartitionKeyStart().length == 0) {
      byte[] arr = remoteTablet.getPartition().getPartitionKeyEnd();
      arr[arr.length-1]--;
      this.partitionKey = arr;
    } else {
      this.partitionKey = remoteTablet.getPartition().getPartitionKeyStart();
    }
  }

  @Override
  Message createRequestPB() {
    final Consensus.ChangeConfigRequestPB.Builder builder = 
        Consensus.ChangeConfigRequestPB.newBuilder();

    builder.setTabletId(ByteString.copyFromUtf8(this.tabletId));
    builder.setType(Consensus.ChangeConfigType.ADD_PEER);
    final Metadata.RaftPeerPB.Builder rBuilder = Metadata.RaftPeerPB.newBuilder();

    rBuilder.setLastKnownAddr(this.targetHostPortPB);
    rBuilder.setMemberType(Metadata.RaftPeerPB.MemberType.NON_VOTER);
    rBuilder.setPermanentUuid(ByteString.copyFromUtf8(this.replicaId));

    Metadata.RaftPeerPB thePeer = rBuilder.build();
    builder.setServer(thePeer);

    return builder.build();
  }

  @Override
  String serviceName() {
    return CONSENSUS_SERVICE_NAME;
  }

  @Override
  String method() {
    return CHANGE_CONFIG;
  }

  @Override 
  byte[] partitionKey() {
    return this.partitionKey;
  }

  @Override
  Pair<ChangeConfigResponse, Object> deserialize(final CallResponse callResponse, 
                                                 String tsUUID) throws KuduException {
    final Consensus.ChangeConfigResponsePB.Builder builder = 
        Consensus.ChangeConfigResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), builder);
    ChangeConfigResponse response = new ChangeConfigResponse(timeoutTracker.getElapsedMillis(),
                                                             tsUUID);
    return new Pair<ChangeConfigResponse, Object>(response,
                                                  builder.hasError() ? builder.getError() : null);
  }
}

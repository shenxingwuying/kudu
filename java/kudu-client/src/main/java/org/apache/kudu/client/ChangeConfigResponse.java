package org.apache.kudu.client;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ChangeConfigResponse extends KuduRpcResponse {
  ChangeConfigResponse(long elapsedMillis, String tsUUID) {
    super(elapsedMillis, tsUUID);
  }

}

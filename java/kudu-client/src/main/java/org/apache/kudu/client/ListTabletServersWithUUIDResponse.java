package org.apache.kudu.client;

import java.util.Map;
import org.apache.kudu.Common.HostPortPB;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ListTabletServersWithUUIDResponse extends KuduRpcResponse {

  private final int tabletServersCount;
  private final Map<String, HostPortPB> tabletServersMap;

  /**
   * @param elapsedMillis Time in milliseconds since RPC creation to now.
   * @param tabletServersCount How many tablet servers the master is reporting.
   * @param tabletServersList List of tablet servers.
   */
  ListTabletServersWithUUIDResponse(
    long elapsedMillis,
    String tsUUID,
    int tabletServersCount,
    Map<String, HostPortPB> tabletServersMap) {
    super(elapsedMillis, tsUUID);
    this.tabletServersCount = tabletServersCount;
    this.tabletServersMap = tabletServersMap;
  }

  /**
   * Get the count of tablet servers as reported by the master.
   * @return TS count.
   */
  public int getTabletServersCount() {
    return tabletServersCount;
  }

  /**
   * Get the list of tablet servers, as represented by their hostname.
   * @return List of hostnames, one per TS.
   */
  public Map<String, HostPortPB> getTabletServersMap() {
    return tabletServersMap;
  }
}

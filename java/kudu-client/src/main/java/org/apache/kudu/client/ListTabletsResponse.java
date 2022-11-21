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

package org.apache.kudu.client;

import java.util.List;

import org.apache.kudu.tserver.Tserver;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ListTabletsResponse extends KuduRpcResponse {

  private final List<String> tabletsList;

  private List<Tserver.ListTabletsResponsePB.StatusAndSchemaPB> tabletInfoMap;

  ListTabletsResponse(long elapsedMillis, String tsUUID,
                      List<String> tabletsList,
                      List<Tserver.ListTabletsResponsePB.StatusAndSchemaPB> tabletInfoMap) {
    super(elapsedMillis, tsUUID);
    this.tabletsList = tabletsList;
    this.tabletInfoMap = tabletInfoMap;
  }

  /**
   * Get the list of tablets as specified in the request.
   * @return a list of tablet uuids
   */
  public List<String> getTabletsList() {
    return tabletsList;
  }

  /**
   * Get the list of tablets detail Info in the request.
   * @return a list of tablet infos
   */
  public List<Tserver.ListTabletsResponsePB.StatusAndSchemaPB> getTabletInfoMap() {
    return tabletInfoMap;
  }
}

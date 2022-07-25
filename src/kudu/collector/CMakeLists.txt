# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#########################################
# collector
#########################################

set(COLLECTOR_SRCS
  collector.cc
  collector_util.cc
  metrics_collector.cc
  nodes_checker.cc
  prometheus_reporter.cc
  service_monitor.cc)

add_library(collector ${COLLECTOR_SRCS})
if ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
  target_link_libraries(collector
    ${SANITIZER_OPTIONS_OVERRIDE}
    kudu_client
    kudu_curl_util
    kudu_tools_test_util
    log
    prometheus-cpp::core
    prometheus-cpp::pull
    security
    server_process
    tablet)
else()
  target_link_libraries(collector
    ${SANITIZER_OPTIONS_OVERRIDE}
    kudu_client
    kudu_curl_util
    kudu_tools_test_util
    log
    prometheus-cpp-core
    prometheus-cpp-pull
    security
    server_process
    tablet)
endif ()

#########################################
# kudu-collector
#########################################

add_executable(kudu-collector collector_main.cc)
target_link_libraries(kudu-collector
  ${SANITIZER_OPTIONS_OVERRIDE}
  ${KRB5_REALM_OVERRIDE}
  collector
  ${KUDU_BASE_LIBS})

option(KUDU_COLLECTOR_INSTALL "Whether to install the Kudu Collector executable" ON)
if(KUDU_COLLECTOR_INSTALL)
  install(TARGETS kudu-collector RUNTIME DESTINATION ${CMAKE_INSTALL_SBINDIR})
else()
  message(STATUS "Skipping install rule for the Kudu Collector executable")
endif()

SET_KUDU_TEST_LINK_LIBS(collector)
ADD_KUDU_TEST(collector_util-test)
ADD_KUDU_TEST(metrics_collector-test)
ADD_KUDU_TEST(nodes_checker-test)

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

find_path(PROMETHEUS_CPP_CORE_INCLUDE_DIR prometheus/exposer.h
  # make sure we don't accidentally pick up a different version
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

find_library(PROMETHEUS_CPP_CORE_STATIC_LIB libprometheus-cpp-core.a
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

set(__CURRENT_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
if (APPLE)
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".dylib")
else()
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
endif()

find_library(PROMETHEUS_CPP_CORE_SHARED_LIB prometheus-cpp-core
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
set(CMAKE_FIND_LIBRARY_SUFFIXES ${__CURRENT_FIND_LIBRARY_SUFFIXES})
unset(__CURRENT_FIND_LIBRARY_SUFFIXES)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(prometheus-cpp-core REQUIRED_VARS
        PROMETHEUS_CPP_CORE_STATIC_LIB PROMETHEUS_CPP_CORE_SHARED_LIB
        PROMETHEUS_CPP_CORE_INCLUDE_DIR)
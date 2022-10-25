#!/bin/bash
KUDU_DIRNAME=${PARCEL_DIRNAME:-"KUDU_SENSORS_DATA-{autogen_version}-cdh5.12.1.p0"}
export KUDU_HOME=$PARCELS_ROOT/$KUDU_DIRNAME/lib/kudu
export LD_LIBRARY_PATH="${KUDU_HOME}/sbin-release/:${LD_LIBRARY_PATH}"
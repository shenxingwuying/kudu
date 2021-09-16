version=
od=el7

if [ $# -gt 1 ]; then
  version=$1
  od=$2
elif [ $# -gt 0 ]; then
  version=$1
else
  echo "error, lost of args"
fi

full_version='KUDU_SENSORS_DATA-'${version}'-cdh5.12.1.p0'

# copy parcel-test to parcel-release
jfrog c add --url=https://jfrog-internal.sensorsdata.cn/ --password=AKCp8ii9JXM1owdsqX7udFrZX26fisBjxVV5vsyTRFgyCDuXc7nuomHCk9BMJhb8ukgtU4WH2 --interactive=false

parcel_name=${full_version}-${od}.parcel
jfrog rt cp dragon-cdh/sdp/kudu/test/${od}/${parcel_name} dragon-cdh/sdp/kudu/release/${od}/${parcel_name} --flat
jfrog rt cp dragon-cdh/sdp/kudu/test/${od}/${parcel_name}.sha dragon-cdh/sdp/kudu/release/${od}/${parcel_name}.sha --flat
jfrog rt cp dragon-cdh/sdp/kudu/test/${od}/${parcel_name}.manifest.json dragon-cdh/sdp/kudu/release/${od}/${parcel_name}.manifest.json --flat

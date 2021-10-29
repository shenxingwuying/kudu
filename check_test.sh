#!/bin/bash

BASE_DIR=$(pwd)
BUILD_PATH=${BASE_DIR}/build/latest

cd $BUILD_PATH
ctest -j32 -O test.log

if [ $? -eq 0 ]; then
  echo "All tests passed"
  exit 0
fi

echo "Some tests failed, retry ..."
CTEST_LOG_FILE=${BUILD_PATH}/test.log
TEST_LOGS_PATH=${BUILD_PATH}/test-logs
egrep "\(Failed\)$" ${CTEST_LOG_FILE} | awk '{print $3}' > ctest_failed_tests

rm -rf retry_tests
rm -rf still_failed_tests
while read ctest_failed_test
do
  arr=(${ctest_failed_test//./ })
  failed_test=$(egrep "^\[  FAILED  \] .*[0-9a-zA-Z]$" ${TEST_LOGS_PATH}/${ctest_failed_test}.txt)
  if [ -z "$failed_test" ]; then
    egrep "^\[ RUN      \] .*[0-9a-zA-Z]$" ${TEST_LOGS_PATH}/${ctest_failed_test}.txt | tail -n 1 | awk '{print "'$BUILD_PATH'/bin/'${arr[0]}' --gtest_filter="$NF}' >> still_failed_tests
  else
    echo ${failed_test} | awk '{print "'$BUILD_PATH'/bin/'${arr[0]}' --gtest_filter="$NF}' >> retry_tests
  fi
done < ctest_failed_tests
cat retry_tests

while read retry_test
do
  for loop in 1 2 3
  do
    echo $loop
    eval ${retry_test}
    if [ $? -eq 0 ]; then
      break
    elif [ $loop -eq 3 ]; then
      echo ${retry_test} >> still_failed_tests
    fi
  done
done < retry_tests

if [ ! -f still_failed_tests ]; then
  exit 0
fi

echo
echo "========"
echo "Failed test cases are:"
cat still_failed_tests
exit 1

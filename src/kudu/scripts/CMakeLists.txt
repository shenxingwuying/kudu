configure_file(${CMAKE_CURRENT_SOURCE_DIR}/kudu-master ${CMAKE_CURRENT_BINARY_DIR}/kudu-master COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/kudu-tserver ${CMAKE_CURRENT_BINARY_DIR}/kudu-tserver COPYONLY)

install(PROGRAMS kudu-tserver kudu-master DESTINATION ${CMAKE_INSTALL_BINDIR})

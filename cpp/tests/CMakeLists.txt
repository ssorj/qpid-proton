#
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
#

add_definitions(${COMPILE_WARNING_FLAGS} ${COMPILE_PLATFORM_FLAGS})

macro (pn_add_cpp_test test)
  string (REPLACE _ - hyphenated_test_name ${test})
  set (executable_name cpp-${hyphenated_test_name})
  add_executable (${executable_name} ${test}.cpp)
  target_link_libraries (${executable_name} qpid-proton-cpp ${PLATFORM_LIBS})
  if (CMAKE_SYSTEM_NAME STREQUAL Windows)
    add_test (NAME ${executable_name}
      COMMAND ${PN_ENV_SCRIPT}
      "PATH=$<TARGET_FILE_DIR:qpid-proton>"
      $<TARGET_FILE:${executable_name}> ${ARGN})
  else ()
    add_test (NAME ${executable_name} COMMAND ${memcheck-cmd} ${CMAKE_CURRENT_BINARY_DIR}/${executable_name} ${ARGN})
  endif ()
endmacro ()

pn_add_cpp_test (codec_test)
pn_add_cpp_test (connection_driver_test)
pn_add_cpp_test (container_test)
pn_add_cpp_test (interop_test ${CMAKE_SOURCE_DIR}/tests)
pn_add_cpp_test (link_test)
pn_add_cpp_test (map_test)
pn_add_cpp_test (message_test)
pn_add_cpp_test (reconnect_test)
pn_add_cpp_test (scalar_test)
pn_add_cpp_test (url_test)
pn_add_cpp_test (value_test)

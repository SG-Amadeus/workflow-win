cmake_minimum_required(VERSION 3.6)

set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "build type")

project(workflow_test
		LANGUAGES C CXX
)

if (WIN32)
	option(WORKFLOW_TUTORIAL_BUILD_STATIC_RUNTIME "Use static runtime" ON)
endif ()

find_library(LIBRT rt)
find_package(OpenSSL REQUIRED)
find_package(workflow REQUIRED CONFIG HINTS ..)
include_directories(${OPENSSL_INCLUDE_DIR} ${WORKFLOW_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../_include/workflow)
link_directories(${WORKFLOW_LIB_DIR})

if (WIN32 AND NOT MINGW)
	set(WORKFLOW_BUILD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../build.cmake")
	if (NOT EXISTS "${WORKFLOW_BUILD_DIR}/CMakeCache.txt")
		message(FATAL_ERROR
			"The workflow library must be configured first: "
			"cmake -S . -B build.cmake -G Ninja")
	endif ()
	add_custom_target(workflow_refresh
		COMMAND ${CMAKE_COMMAND} --build "${WORKFLOW_BUILD_DIR}" --target workflow
		WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/.."
		COMMENT "Update workflow.lib before building tests"
		VERBATIM
	)
endif ()

find_program(CMAKE_MEMORYCHECK_COMMAND valgrind)
set(memcheck_command ${CMAKE_MEMORYCHECK_COMMAND} ${CMAKE_MEMORYCHECK_COMMAND_OPTIONS} --error-exitcode=1 --leak-check=full)

add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})

enable_testing()
find_package(GTest CONFIG REQUIRED)

if (WIN32 AND NOT MINGW AND WORKFLOW_TUTORIAL_BUILD_STATIC_RUNTIME)
	set(CompilerFlags
			CMAKE_CXX_FLAGS
			CMAKE_CXX_FLAGS_DEBUG
			CMAKE_CXX_FLAGS_RELEASE
			CMAKE_CXX_FLAGS_RELWITHDEBINFO
			CMAKE_CXX_FLAGS_MINSIZEREL
			CMAKE_C_FLAGS
			CMAKE_C_FLAGS_DEBUG
			CMAKE_C_FLAGS_RELEASE
			CMAKE_C_FLAGS_RELWITHDEBINFO
			CMAKE_C_FLAGS_MINSIZEREL
			)
	foreach(CompilerFlag ${CompilerFlags})
		string(REPLACE "/MD" "/MT" ${CompilerFlag} "${${CompilerFlag}}")
	endforeach ()
endif ()

if (MINGW)
	set(CMAKE_C_FLAGS   "${CMAKE_C_FLAGS}   -Wall -fPIC -pipe -std=gnu90")
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -fPIC -pipe -std=c++17 -fexceptions")
elseif (WIN32)
	set(CMAKE_C_FLAGS   "${CMAKE_C_FLAGS}   /MP /wd4200")
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /MP /wd4200 /Zc:__cplusplus /std:c++14")
else ()
	set(CMAKE_C_FLAGS   "${CMAKE_C_FLAGS}   -Wall -fPIC -pipe -std=gnu90")
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -fPIC -pipe -std=c++11 -fno-exceptions")
endif ()

set(TEST_LIST
	task_unittest
	algo_unittest
	http_unittest
	redis_unittest
	mysql_unittest
	facilities_unittest
	graph_unittest
	memory_unittest
	upstream_unittest
	dns_unittest
	resource_unittest
	uriparser_unittest
	scheduler_unittest
)

set(GTEST_LIB GTest::GTest GTest::Main)

if (APPLE)
	set(WORKFLOW_LIB workflow pthread OpenSSL::SSL OpenSSL::Crypto)
elseif (MINGW)
	set(WORKFLOW_LIB workflow ws2_32 wsock32 OpenSSL::SSL OpenSSL::Crypto iphlpapi m z)
	set(GTEST_LIB GTest::gtest GTest::gtest_main)
elseif (WIN32)
	set(WORKFLOW_LIB "${WORKFLOW_LIB_DIR}/workflow.lib" ws2_32 wsock32 mswsock mincore OpenSSL::SSL OpenSSL::Crypto)
	set(GTEST_LIB GTest::gtest GTest::gtest_main)
else ()
	set(WORKFLOW_LIB workflow ${LIBRT})
endif ()

foreach(src ${TEST_LIST})
	add_executable(${src} ${src}.cc)
	target_link_libraries(${src} ${WORKFLOW_LIB} ${GTEST_LIB})
	if (WIN32 AND NOT MINGW)
		add_dependencies(${src} workflow_refresh)
	endif ()
	if (WIN32 AND NOT MINGW)
			target_compile_definitions(
				${src} PRIVATE
				strdup=_strdup
				strcasecmp=_stricmp
				strncasecmp=_strnicmp
			)
	endif ()
	add_test(${src} ${src})
	set_tests_properties(${src} PROPERTIES TIMEOUT 30)
	add_dependencies(check ${src})
endforeach()

# Windows-specific tests: Communicator and direct async-kernel lifecycle.
set(WINDOWS_TEST_SOURCES
	windows/communicator_unittest.cc
	windows/async_core_unittest.cc
)
set(WINDOWS_TEST_NAMES
	windows_communicator_unittest
	windows_async_core_unittest
)

list(LENGTH WINDOWS_TEST_SOURCES WINDOWS_TEST_COUNT)
math(EXPR WINDOWS_TEST_COUNT_MINUS_ONE "${WINDOWS_TEST_COUNT} - 1")
foreach(i RANGE 0 ${WINDOWS_TEST_COUNT_MINUS_ONE})
	list(GET WINDOWS_TEST_SOURCES ${i} WINDOWS_TEST_SRC)
	list(GET WINDOWS_TEST_NAMES ${i} WINDOWS_TEST_NAME)
	add_executable(${WINDOWS_TEST_NAME} ${WINDOWS_TEST_SRC})
	if (WIN32 AND NOT MINGW)
		add_dependencies(${WINDOWS_TEST_NAME} workflow_refresh)
	endif ()
	target_include_directories(${WINDOWS_TEST_NAME} PRIVATE
		${CMAKE_CURRENT_SOURCE_DIR}/../src/kernel_win
	)
	target_link_libraries(${WINDOWS_TEST_NAME} ${WORKFLOW_LIB} ${GTEST_LIB})
	if (WIN32 AND NOT MINGW)
		target_compile_definitions(
			${WINDOWS_TEST_NAME} PRIVATE
			strdup=_strdup
			strcasecmp=_stricmp
			strncasecmp=_strnicmp
		)
	endif ()
	add_test(${WINDOWS_TEST_NAME} ${WINDOWS_TEST_NAME})
	set_tests_properties(${WINDOWS_TEST_NAME} PROPERTIES TIMEOUT 60)
	add_dependencies(check ${WINDOWS_TEST_NAME})
endforeach()

if (WIN32)
else ()
	foreach(src ${TEST_LIST})
		add_test(${src}-memory-check ${memcheck_command} ./${src})
	endforeach()
endif ()

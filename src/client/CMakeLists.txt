cmake_minimum_required(VERSION 3.6)
project(client)

set(SRC
	WFMySQLConnection.cc
	WFDnsClient.cc
	WFHttpChunkedClient.cc
)

add_library(${PROJECT_NAME} OBJECT ${SRC})
if (WIN32 AND NOT MINGW)
		target_compile_definitions(
			${PROJECT_NAME} PRIVATE
			strdup=_strdup
			strcasecmp=_stricmp
			strncasecmp=_strnicmp
		)
endif ()

if (KAFKA STREQUAL "y")
	set(SRC
		WFKafkaClient.cc
	)
	add_library("client_kafka" OBJECT ${SRC})
	if (WIN32 AND NOT MINGW)
			target_compile_definitions(
				"client_kafka" PRIVATE
				strdup=_strdup
				strcasecmp=_stricmp
				strncasecmp=_strnicmp
			)
	endif ()
endif ()

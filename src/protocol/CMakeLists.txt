cmake_minimum_required(VERSION 3.6)
project(protocol)

set(SRC
	dns_parser.c
	http_parser.c
	redis_parser.c
	mysql_stream.c
	mysql_parser.c
	mysql_byteorder.c
	MySQLMessage.cc
	MySQLResult.cc
	MySQLUtil.cc
	DnsMessage.cc
	DnsUtil.cc
	HttpMessage.cc
	RedisMessage.cc
	TLVMessage.cc
	HttpUtil.cc
	SSLWrapper.cc
	PackageWrapper.cc
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
		kafka_parser.c
		KafkaMessage.cc
		KafkaDataTypes.cc
		KafkaResult.cc
	)
	add_library("protocol_kafka" OBJECT ${SRC})
	if (NOT WIN32)
		set_property(SOURCE KafkaMessage.cc APPEND PROPERTY COMPILE_OPTIONS "-fno-rtti")
	endif ()
	if (WIN32 AND NOT MINGW)
			target_compile_definitions(
				"protocol_kafka" PRIVATE
				strdup=_strdup
				strcasecmp=_stricmp
				strncasecmp=_strnicmp
			)
	endif ()
endif ()

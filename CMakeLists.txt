cmake_minimum_required(VERSION 2.8)
include_directories ("${PROJECT_SOURCE_DIR}/api")

add_library(gproject 
	platanos.c
        platanos_common.c
        vertex.c
	)

cmake_minimum_required(VERSION 3.27)
project(MySQLEXample)

set(CMAKE_CXX_STANDARD 17)

add_executable(MySQLExample main.cpp
        Credentials.cpp
        Credentials.h)

set(JSON_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/lib/json/include/)
target_include_directories(MySQLExample PRIVATE ${JSON_INCLUDE_DIR})
target_include_directories(MySQLExample PRIVATE ${CMAKE_SOURCE_DIR}/lib/mysql-8.0.36/include/)
target_link_directories(MySQLExample PRIVATE ${CMAKE_SOURCE_DIR}/lib/mysql-8.0.36/lib/libmysqlclient.so)
target_link_libraries(MySQLExample PRIVATE ${CMAKE_SOURCE_DIR}/lib/lib-dynamic/libPocoFoundation.so)
target_link_libraries(MySQLExample PRIVATE ${CMAKE_SOURCE_DIR}/lib/lib-dynamic/libPocoNet.so)
target_link_libraries(MySQLExample PRIVATE ${CMAKE_SOURCE_DIR}/lib/lib-dynamic/libmysqlclient.so)

target_include_directories(MySQLExample PRIVATE ${CMAKE_SOURCE_DIR}/lib/poco/Foundation/include/)
target_include_directories(MySQLExample PRIVATE ${CMAKE_SOURCE_DIR}/lib/poco/Net/include/)
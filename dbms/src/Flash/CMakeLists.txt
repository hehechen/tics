include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(flash_service .)
add_headers_and_sources(flash_service ./Coprocessor)
add_headers_and_sources(flash_service ./Mpp)

add_library(flash_service ${flash_service_headers} ${flash_service_sources})
target_include_directories (flash_service PUBLIC ${OPENSSL_INCLUDE_DIR})
message (STATUS "cmake in flash_service=${OPENSSL_FOUND}: ${OPENSSL_INCLUDE_DIR}: ${OPENSSL_CRYPTO_LIBRARY} : ${OPENSSL_SSL_LIBRARY}")
target_link_libraries(flash_service dbms)

if (ENABLE_TESTS)
    add_subdirectory (Coprocessor/tests)
    add_subdirectory (tests)
endif ()

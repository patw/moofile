cmake_minimum_required(VERSION 3.16)
project(moofile-c-tests LANGUAGES C CXX)

# ---------------------------------------------------------------------------
# Find Rust-built shared library
# ---------------------------------------------------------------------------

# After `cargo build -p moofile-c --release`, the .so lives here:
set(MOOFILE_LIB_DIR "${CMAKE_SOURCE_DIR}/../../target/release" CACHE PATH "Path to libmoofile.so")
set(MOOFILE_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/../include" CACHE PATH "Path to moofile.h")

# ---------------------------------------------------------------------------
# C test — links against libmoofile
# ---------------------------------------------------------------------------

add_executable(test_c_api
    test_c_api.c
)

target_include_directories(test_c_api PRIVATE ${MOOFILE_INCLUDE_DIR})
target_link_directories(test_c_api PRIVATE ${MOOFILE_LIB_DIR})
target_link_libraries(test_c_api PRIVATE moofile m)

# Link libmoofile.so at runtime too — set RPATH so it's found without LD_LIBRARY_PATH
set_target_properties(test_c_api PROPERTIES
    BUILD_RPATH "${MOOFILE_LIB_DIR}"
    INSTALL_RPATH "${MOOFILE_LIB_DIR}"
)

# ---------------------------------------------------------------------------
# C++ test — links against libmoofile + nlohmann_json
# ---------------------------------------------------------------------------

# Try to find nlohmann_json via CMake, or download it
find_package(nlohmann_json QUIET)
if(NOT nlohmann_json_FOUND)
    message(STATUS "nlohmann_json not found — fetching single-include header")
    file(DOWNLOAD
        "https://github.com/nlohmann/json/releases/download/v3.11.3/json.hpp"
        "${CMAKE_BINARY_DIR}/_deps/nlohmann_json.hpp"
        STATUS download_status
    )
    list(GET download_status 0 status_code)
    if(status_code EQUAL 0)
        set(NLOHMANN_JSON_INCLUDE_DIR "${CMAKE_BINARY_DIR}/_deps")
    else()
        message(FATAL_ERROR "Failed to download nlohmann/json.hpp. "
            "Install it via your package manager or place json.hpp in /usr/local/include.")
    endif()
else()
    set(NLOHMANN_JSON_INCLUDE_DIR "${nlohmann_json_INCLUDE_DIR}")
endif()

add_executable(test_cxx_api
    test_cxx_api.cpp
)

target_include_directories(test_cxx_api PRIVATE
    ${MOOFILE_INCLUDE_DIR}
    ${NLOHMANN_JSON_INCLUDE_DIR}
)
target_link_directories(test_cxx_api PRIVATE ${MOOFILE_LIB_DIR})
target_link_libraries(test_cxx_api PRIVATE moofile m)

set_target_properties(test_cxx_api PROPERTIES
    BUILD_RPATH "${MOOFILE_LIB_DIR}"
    INSTALL_RPATH "${MOOFILE_LIB_DIR}"
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# ---------------------------------------------------------------------------
# Test target
# ---------------------------------------------------------------------------

enable_testing()

add_test(NAME test_c_api   COMMAND test_c_api)
add_test(NAME test_cxx_api COMMAND test_cxx_api)

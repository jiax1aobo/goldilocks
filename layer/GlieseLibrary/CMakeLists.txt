CMAKE_MINIMUM_REQUIRED(VERSION 2.6)
# Avoid warnings in higher versions
IF("${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}" GREATER 2.6)
 CMAKE_POLICY(VERSION 2.8)
ENDIF()

##################################################
# ODBC INI
##################################################

IF(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
ELSE()
  IF( NOT EXISTS "$ENV{HOME}/.odbc.ini" )
    ADD_CUSTOM_TARGET( odbcini ALL
                       ${CMAKE_COMMAND} -E copy ${GLIESE_HOME_CONF}/.odbc.ini $ENV{HOME}
                       COMMENT "copy .odbc.ini" )
  ENDIF()
ENDIF()

##################################################
# Compilation
##################################################

ADD_SUBDIRECTORY(${PRODUCT_LAYER_ODBC_DIR})
#ADD_SUBDIRECTORY(${PRODUCT_LAYER_JDBC_DIR})
#ADD_SUBDIRECTORY(esql)
#ADD_SUBDIRECTORY(${PRODUCT_LAYER_PYGLIESELIB_DIR})

##################################################
# SPLINT( make lintzl )
##################################################

IF (UNIX)

# check python library
INCLUDE (${CMAKE_ROOT}/Modules/FindPythonLibs.cmake)

ADD_CUSTOM_TARGET( lintzl 
                   COMMAND find */dev/src -name \"*.c\" | egrep -v "PythonWrapper" > lintfiles.txt
                   COMMAND for file in `cat lintfiles.txt`\; 
                           do  
                              echo splint ... $$file\; 
                              splint ${LINT_FLAGS} 
                                -I${PRODUCT_LAYER_STANDARD_DIR}/dev/include
                                -I${PRODUCT_LAYER_DATATYPE_DIR}/dev/include
                                -I${PRODUCT_LAYER_COMMUNICATION_DIR}/dev/include
                                -I${PRODUCT_LAYER_KERNEL_DIR}/dev/include
                                -I${PRODUCT_LAYER_SERVERCOMMUNICATION_DIR}/dev/include
                                -I${PRODUCT_LAYER_STORAGEMANAGER_DIR}/dev/include
                                -I${PRODUCT_LAYER_EXECUTIONLIBRARY_DIR}/dev/include
                                -I${PRODUCT_LAYER_SQLPROCESSOR_DIR}/dev/include
                                -I${PRODUCT_LAYER_SESSION_DIR}/dev/include
                                # -I${PRODUCT_LAYER_SESSION_DIR}/dev/include
                                -I${PRODUCT_LAYER_SERVERLIBRARY_DIR}/dev/include
                                -I${PRODUCT_LAYER_ODBC_DIR}/dev/include
                                -I${PRODUCT_LAYER_ODBC_DIR}/dev/src/include
                                -I${PYTHON_INCLUDE_PATH}
                                # -I${PRODUCT_LAYER_PYGLIESELIB_DIR}/dev/src/include
                                # -I${PRODUCT_LAYER_GOLDILOCKSESQLLIB_DIR}/dev/include
                                # -I${PRODUCT_LAYER_GOLDILOCKSESQLLIB_DIR}/dev/src/include
                                $$file\;
                           done
                   )
ENDIF (UNIX)

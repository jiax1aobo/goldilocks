/*******************************************************************************
 * ztdExecThread.c
 *
 * Copyright (c) 2012, SUNJESOFT Inc.
 *
 *
 * IDENTIFICATION & REVISION
 *        $Id$
 *
 * NOTES
 *    
 *
 ******************************************************************************/

#include <stl.h>
#include <stlDef.h>
#include <dtl.h>
#include <goldilocks.h>
#include <ztdDef.h>
#include <ztdDisplay.h>
#include <ztdBinaryMode.h>
#include <ztdBinaryConsumerThread.h>
#include <ztdFileExecute.h>
#include <ztdBufferedFile.h>

extern dtlCharacterSet    gZtdDatabaseCharacterSet;
extern stlBool            gZtdRunState;

extern stlFile                   gZtdBadFile;
stlInt8                   gZtdEndian;
SQLSMALLINT               gZtdColumnCount                        = 0;
stlInt32                  gZtdReadBlockCount                     = 0;
stlUInt32                 gZtdCompleteCount                      = 0;
stlUInt64                 gZtdCompleteCountState                 = ZTD_COUNT_STATE_RELEASE;
volatile stlUInt32        gZtdBinaryModeExecuteRunningCount      = 0; /* binary import에서 동작 중인 execute thread의 개수 */
extern stlUInt32          gZtdErrorCount;
extern stlUInt32          gZtdMaxErrorCount;
extern ztdManager         gZtdManager;

static dtlCharacterSet ztdGetDBCharSetIDFunc( void * aArgs )
{
    return gZtdDatabaseCharacterSet;
}

dtlDataType ztdGetDtlType( SQLSMALLINT aSQLType )
{
    dtlDataType sDataType = DTL_TYPE_MAX;

    switch( aSQLType )
    {
        case SQL_BOOLEAN :
            sDataType = DTL_TYPE_BOOLEAN;
            break;
        case SQL_SMALLINT :
            sDataType = DTL_TYPE_NATIVE_SMALLINT;
            break;
        case SQL_INTEGER :
            sDataType = DTL_TYPE_NATIVE_INTEGER;
            break;
        case SQL_BIGINT :
            sDataType = DTL_TYPE_NATIVE_BIGINT;
            break;
        case SQL_REAL :
            sDataType = DTL_TYPE_NATIVE_REAL;
            break;
        case SQL_DOUBLE :
            sDataType = DTL_TYPE_NATIVE_DOUBLE;
            break;
        case SQL_FLOAT :
            sDataType = DTL_TYPE_FLOAT;
            break;
        case SQL_NUMERIC :
            sDataType = DTL_TYPE_NUMBER;
            break;
        case SQL_CHAR :
            sDataType = DTL_TYPE_CHAR;
            break;
        case SQL_VARCHAR :
            sDataType = DTL_TYPE_VARCHAR;
            break;
        case SQL_LONGVARCHAR :
            sDataType = DTL_TYPE_LONGVARCHAR;
            break;
        case SQL_BINARY :
            sDataType = DTL_TYPE_BINARY;
            break;
        case SQL_VARBINARY :
            sDataType = DTL_TYPE_VARBINARY;
            break;
        case SQL_LONGVARBINARY :
            sDataType = DTL_TYPE_LONGVARBINARY;
            break;
        case SQL_TYPE_DATE :
            sDataType = DTL_TYPE_DATE;
            break;
        case SQL_TYPE_TIME :
            sDataType = DTL_TYPE_TIME;
            break;
        case SQL_TYPE_TIME_WITH_TIMEZONE :
            sDataType = DTL_TYPE_TIME_WITH_TIME_ZONE;
            break;
        case SQL_TYPE_TIMESTAMP :
            sDataType = DTL_TYPE_TIMESTAMP;
            break;
        case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE :
            sDataType = DTL_TYPE_TIMESTAMP_WITH_TIME_ZONE;
            break;
        case SQL_INTERVAL_YEAR :
        case SQL_INTERVAL_MONTH :
        case SQL_INTERVAL_YEAR_TO_MONTH :
            sDataType = DTL_TYPE_INTERVAL_YEAR_TO_MONTH;
            break;
        case SQL_INTERVAL_DAY :
        case SQL_INTERVAL_HOUR :
        case SQL_INTERVAL_MINUTE :
        case SQL_INTERVAL_SECOND :
        case SQL_INTERVAL_DAY_TO_HOUR :
        case SQL_INTERVAL_DAY_TO_MINUTE :
        case SQL_INTERVAL_DAY_TO_SECOND :
        case SQL_INTERVAL_HOUR_TO_MINUTE :
        case SQL_INTERVAL_HOUR_TO_SECOND :
        case SQL_INTERVAL_MINUTE_TO_SECOND :
            sDataType = DTL_TYPE_INTERVAL_DAY_TO_SECOND;
            break;
        case SQL_ROWID :
            sDataType = DTL_TYPE_ROWID;
            break;
        default :
            STL_ASSERT( STL_FALSE );
            break;
    }

    return sDataType;
}

/**
 * @brief gloader의 download를 binary format으로 실행한다.
 * @param [in]  aEnvHandle          SQLHENV
 * @param [in]  aDbcHandle          SQLHDBC  
 * @param [in]  ztdInputArguments   main에서 할당한 Region Memory
 * @param [in]  aControlInfo        Control File의 내용을 저장한 struct
 * @param [in]  aFileAndBuffer      File Descriptor 와 log를 담는 struct
 * @param [out] aErrorStack         error stack
 */
stlStatus ztdExportBinaryData( SQLHENV             aEnvHandle,
                               SQLHDBC             aDbcHandle,
                               ztdInputArguments   aInputArguments,
                               ztdControlInfo    * aControlInfo,
                               ztdFileAndBuffer  * aFileAndBuffer,
                               stlErrorStack     * aErrorStack )
{
    SQLHSTMT                       sStmt                                     = NULL;
    SQLHDESC                       sDesc                                     = NULL;

    stlAllocator                   sAllocator;
    stlDynamicAllocator            sDynamicAllocator;
      
    stlChar                        sSqlString[ZTD_MAX_COMMAND_BUFFER_SIZE];

    stlInt32                       sIdxColumn                                = 0;

    stlInt32                       sHeaderSize                               = 0;
    stlInt32                       sTempSize                                 = 0;
    stlInt64                       sMinFileSize                              = 0;
    
    stlInt32                       sSize                                     = 0;

    ztdColumnInfo                * sColumnInfo                               = NULL;

    ztdBinaryWriteDataQueue        sFileWriteQueue;
    
    stlInt32                       sQueueIndex                               = 0;
    stlInt32                       sBufferIndex                              = 0;
    stlInt32                       sDataValueIndex                           = 0;
    stlInt32                       sColumnIndex                              = 0;
    
    dtlValueBlockList            * sFetchValueBlockList                      = NULL;
    dtlValueBlockList            * sWriteValueBlockList                      = NULL;
    dtlValueBlockList            * sTempValueBlockList                       = NULL;
    void                         * sDataBuffer                               = NULL;
    
    stlThread                      sFetchThread;
    ztdBinaryModeWriteThreadArg    sFetchThreadArg; 
    
    stlThread                      sWriteThread;
    ztdBinaryModeWriteThreadArg    sWriteThreadArg;

    stlStatus                      sReturn                                   = STL_FAILURE;
    
    stlChar                        sSemaphoreName[STL_MAX_SEM_NAME];
   
    stlInt32                       i                                         = 0;
    
    stlInt32                       sState                                    = 0;

    dtlFuncVector                  sVector;
    dtlDataType                    sDbType;
    stlInt64                       sLength;

    stlBool                        sIsAllocStmtHandle                        = STL_FALSE;
    
    stlErrorStack                * sJoinErrorStack                           = NULL;

    stlChar                        sTypeName[STL_MAX_SQL_IDENTIFIER_LENGTH];

    /*
     * FUNCTION VECTOR 설정
     */
    stlMemset( (void*)&sVector, 0x00, STL_SIZEOF(dtlFuncVector) );

    sVector.mGetCharSetIDFunc = ztdGetDBCharSetIDFunc;

    STL_TRY( stlCreateRegionAllocator( &sAllocator,
                                       ZTD_REGION_INIT_SIZE,
                                       ZTD_REGION_NEXT_SIZE,
                                       aErrorStack )
             == STL_SUCCESS );
    sState = 1;

    STL_TRY( stlCreateDynamicAllocator( &sDynamicAllocator,
                                        ZTD_DYNAMIC_INIT_SIZE,
                                        ZTD_DYNAMIC_NEXT_SIZE,
                                        aErrorStack )
             == STL_SUCCESS );
    sState = 2;
    
    /**
     * database 연결
     */
    STL_TRY( SQLAllocHandle( SQL_HANDLE_STMT,
                             aDbcHandle,
                             &sStmt )
             == SQL_SUCCESS );
    sIsAllocStmtHandle = STL_TRUE;
    
    /**
     * Import중 Table을 삭제할 수 없도록 Lock을 잡는다. (IS Lock)
     */
    if( aControlInfo->mSchema[0] == 0x00 )
    {
        stlSnprintf( sSqlString,
                     ZTD_MAX_COMMAND_BUFFER_SIZE,
                     "LOCK TABLE %s IN ROW SHARE MODE NOWAIT",
                     aControlInfo->mTable );
    }
    else
    {
        stlSnprintf( sSqlString,
                     ZTD_MAX_COMMAND_BUFFER_SIZE,
                     "LOCK TABLE %s.%s IN ROW SHARE MODE NOWAIT",
                     aControlInfo->mSchema,
                     aControlInfo->mTable );
    }
    
    STL_TRY( SQLExecDirect( sStmt,
                            (SQLCHAR *) sSqlString,
                            SQL_NTS )
             == SQL_SUCCESS );
        
    /**
     * Table의 Column 개수를 얻는다.
     */
    if( aControlInfo->mSchema[0] == 0x00 )
    {
        stlSnprintf( sSqlString,
                     ZTD_MAX_COMMAND_BUFFER_SIZE,
                     "SELECT * FROM %s",
                     aControlInfo->mTable );
    }
    else
    {
        stlSnprintf( sSqlString,
                     ZTD_MAX_COMMAND_BUFFER_SIZE,
                     "SELECT * FROM %s.%s",
                     aControlInfo->mSchema,
                     aControlInfo->mTable );
    }

    STL_TRY( SQLPrepare( sStmt,
                         (SQLCHAR *) sSqlString,
                         SQL_NTS )
             == SQL_SUCCESS );
    
    STL_TRY( SQLGetStmtAttr( sStmt,
                             SQL_ATTR_IMP_ROW_DESC,
                             &sDesc,
                             SQL_IS_POINTER,
                             NULL )
             == SQL_SUCCESS );

    if( aControlInfo->mSchema[0] == 0x00 )
    {
        STL_TRY( SQLGetDescField( sDesc,
                                  1,
                                  SQL_DESC_SCHEMA_NAME,
                                  aControlInfo->mSchema,
                                  ZTD_MAX_SCHEMA_NAME,
                                  NULL )
                 == SQL_SUCCESS );
    }
    else
    {
        /* Do Notting */
    }
    
    STL_TRY( SQLNumResultCols( sStmt, &gZtdColumnCount )
             == SQL_SUCCESS );
    
    /**
     * column의 개수만큼 sColumnInfo를 할당받는다.
     */
    sSize = STL_SIZEOF( ztdColumnInfo ) * gZtdColumnCount;
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sColumnInfo,
                                aErrorStack )
             == STL_SUCCESS );
    
    stlMemset( (void *) sColumnInfo,
               0x00,
               sSize );

    /**
     * gZtdEndian, gZtdReadBlockCount에 값 설정
     */
    gZtdEndian = STL_PLATFORM_ENDIAN;

    gZtdReadBlockCount = ZTD_DEFAULT_BLOCK_READ_COUNT;
    
    /**
     * column의 정보를 구성한다.
     */
    for( sIdxColumn = 0; sIdxColumn < gZtdColumnCount; sIdxColumn++ )
    {
        STL_TRY( SQLDescribeCol( sStmt,
                                 sIdxColumn + 1,
                                 (SQLCHAR*)sColumnInfo[sIdxColumn].mName,
                                 ZTD_MAX_COLUMN_NAME,
                                 &sColumnInfo[sIdxColumn].mNameLength,
                                 &sColumnInfo[sIdxColumn].mSQLType,
                                 &sColumnInfo[sIdxColumn].mDataSize,
                                 &sColumnInfo[sIdxColumn].mDecimalDigits,
                                 &sColumnInfo[sIdxColumn].mNullable )
                 == SQL_SUCCESS );

        STL_TRY( SQLGetDescField( sDesc,
                                  sIdxColumn + 1,
                                  SQL_DESC_TYPE_NAME,
                                  sTypeName,
                                  STL_MAX_SQL_IDENTIFIER_LENGTH,
                                  NULL )
                 == SQL_SUCCESS );
        
        sDbType = ztdGetDtlType( sColumnInfo[sIdxColumn].mSQLType );

        /**
         * LONG VARCHAR, LONG VARBINARY를 포함한 경우 block count를 20으로 한다.
         * block count를 40으로 할 경우 fetch받을 때, 할당할 수 있는 dynamic memory가 부족하게 된다.
         */

        /**
         * @todo 서버로부터 정확한 STRING_LENGTH_UNIT를 갖고와 설정하도록 한다.
         */
        switch( sDbType )
        {
            case DTL_TYPE_CHAR :
            case DTL_TYPE_VARCHAR :
                STL_TRY( dtlGetDataValueBufferSize( sDbType,
                                                    sColumnInfo[sIdxColumn].mDataSize,
                                                    DTL_STRING_LENGTH_UNIT_CHARACTERS,
                                                    &sLength,
                                                    &sVector,
                                                    NULL,
                                                    aErrorStack ) == STL_SUCCESS );
                break;
            case DTL_TYPE_LONGVARCHAR :
                gZtdReadBlockCount = 20;

                STL_TRY( dtlGetDataValueBufferSize( sDbType,
                                                    sColumnInfo[sIdxColumn].mDataSize,
                                                    DTL_STRING_LENGTH_UNIT_OCTETS,
                                                    &sLength,
                                                    &sVector,
                                                    NULL,
                                                    aErrorStack ) == STL_SUCCESS );
                break;
            case DTL_TYPE_LONGVARBINARY :
                gZtdReadBlockCount = 20;

                STL_TRY( dtlGetDataValueBufferSize( sDbType,
                                                    sColumnInfo[sIdxColumn].mDataSize,
                                                    DTL_STRING_LENGTH_UNIT_NA,
                                                    &sLength,
                                                    &sVector,
                                                    NULL,
                                                    aErrorStack ) == STL_SUCCESS );
                break;
            case DTL_TYPE_FLOAT :
                if( stlStrcmp( sTypeName, "NUMBER" ) == 0 )
                {
                    sDbType = DTL_TYPE_NUMBER;
                    
                    STL_TRY( dtlGetDataValueBufferSize( sDbType,
                                                        DTL_NUMERIC_MAX_PRECISION,
                                                        DTL_STRING_LENGTH_UNIT_NA,
                                                        &sLength,
                                                        &sVector,
                                                        NULL,
                                                        aErrorStack ) == STL_SUCCESS );
                }
                else
                {
                    STL_TRY( dtlGetDataValueBufferSize( sDbType,
                                                        sColumnInfo[sIdxColumn].mDataSize,
                                                        DTL_STRING_LENGTH_UNIT_NA,
                                                        &sLength,
                                                        &sVector,
                                                        NULL,
                                                        aErrorStack ) == STL_SUCCESS );
                }
                break;
            case DTL_TYPE_TIMESTAMP :
                STL_TRY( dtlGetDataValueBufferSize( sDbType,
                                                    sColumnInfo[sIdxColumn].mDataSize,
                                                    DTL_STRING_LENGTH_UNIT_NA,
                                                    &sLength,
                                                    &sVector,
                                                    NULL,
                                                    aErrorStack ) == STL_SUCCESS );
                
                if( stlStrcmp( sTypeName, "DATE" ) == 0 )
                {
                    sDbType = DTL_TYPE_DATE;
                }
                else
                {
                    /* Do Nothing */
                }
                break;
            default :
                STL_TRY( dtlGetDataValueBufferSize( sDbType,
                                                    sColumnInfo[sIdxColumn].mDataSize,
                                                    DTL_STRING_LENGTH_UNIT_NA,
                                                    &sLength,
                                                    &sVector,
                                                    NULL,
                                                    aErrorStack ) == STL_SUCCESS );
                break;
        }
        
        sColumnInfo[sIdxColumn].mDbType = sDbType;
        sColumnInfo[sIdxColumn].mBufferSize = sLength;
    }

    sIsAllocStmtHandle = STL_FALSE;
    STL_TRY( SQLFreeHandle( SQL_HANDLE_STMT,
                            sStmt )
             == SQL_SUCCESS );
        
    /**
     * data file의 header size 구하기
     */
    sTempSize = ( ZTD_FILE_INFORMATION_SIZE + ( ZTD_COLUMN_INFORMATION_SIZE * gZtdColumnCount ) );

    ZTD_ALIGN( sHeaderSize, sTempSize, ZTD_BINARY_HEADER_SIZE );
    
    /**
     * file의 최소 size를 구한다.
     * 최소 size는 한 개의 value block list의 데이터를 저장할 수 있는 크기이다.
     */
    sMinFileSize = sHeaderSize;

    sTempSize = 0;
    for( sIdxColumn = 0; sIdxColumn < gZtdColumnCount; sIdxColumn++ )
    {
        sTempSize += ( sColumnInfo[sIdxColumn].mBufferSize * gZtdReadBlockCount );
    }

    ZTD_ALIGN( sMinFileSize, sTempSize, ZTD_WRITE_BUFFER_SIZE );

    sMinFileSize += sHeaderSize;

    if( (aInputArguments.mMaxFileSize > 0) &&
        (aInputArguments.mMaxFileSize < sMinFileSize ) )
    {
        STL_THROW( RAMP_ERR_FILE_SIZE );
    }
    else
    {
        /* Do Nothing */
    }

    ZTD_ALIGN( sMinFileSize, sMinFileSize, ZTD_WRITE_BUFFER_SIZE );
    
    /**
     * fetch thread에서 사용할 sValueBlockList를 할당받는다.
     */
    sSize = STL_SIZEOF( dtlValueBlockList );
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sFetchValueBlockList,
                                aErrorStack )
             == STL_SUCCESS );

    stlMemset( (void *) sFetchValueBlockList,
               0x00,
               sSize );

    sTempValueBlockList = sFetchValueBlockList;
    for( sColumnIndex = 0; sColumnIndex < gZtdColumnCount; sColumnIndex++ )
    {
        sSize = STL_SIZEOF( dtlValueBlock );
        STL_TRY( stlAllocRegionMem( &sAllocator,
                                    sSize,
                                    (void **) &sTempValueBlockList->mValueBlock,
                                    aErrorStack )
                 == STL_SUCCESS );

        stlMemset( (void *) sTempValueBlockList->mValueBlock,
                   0x00,
                   sSize );

        sSize = STL_SIZEOF( dtlDataValue ) * gZtdReadBlockCount;
        STL_TRY( stlAllocRegionMem( &sAllocator,
                                    sSize,
                                    (void **) &sTempValueBlockList->mValueBlock->mDataValue,
                                    aErrorStack )
                 == STL_SUCCESS );

        stlMemset( (void *) sTempValueBlockList->mValueBlock->mDataValue,
                   0x00,
                   sSize );

        /**
         * 각 column의 data가 저장될 buffer를 할당받는다.
         */
        if( (sColumnInfo[sColumnIndex].mDbType == DTL_TYPE_LONGVARCHAR) ||
            (sColumnInfo[sColumnIndex].mDbType == DTL_TYPE_LONGVARBINARY) )
        {
            /**
             * LONG VARCHAR, LONG VARBINARY인 경우 각 work thread에서의 재할당을 고려하여 메모리를 각 각 할당받는다.
             */
            sSize = ZTD_LONG_DATA_INIT_SIZE;
            
            for( sDataValueIndex = 0; sDataValueIndex < gZtdReadBlockCount; sDataValueIndex++ )
            {
                STL_TRY( stlAllocDynamicMem( &sDynamicAllocator,
                                             sSize,
                                             (void **) &sTempValueBlockList->mValueBlock->mDataValue[sDataValueIndex].mValue,
                                             aErrorStack )
                         == STL_SUCCESS );

                stlMemset( (void *) sTempValueBlockList->mValueBlock->mDataValue[sDataValueIndex].mValue,
                           0x00,
                           sSize );

                sTempValueBlockList->mValueBlock->mDataValue[sDataValueIndex].mBufferSize = sSize;
            }
        }
        else
        {
            /**
             * LONG VARCHAR, LONGVARBINARY가 아닌 경우 alloc의 횟수를 줄이기 위해서 한 번의 할당 후에 필요한 메모리의 주소를 분배한다.
             */
            sSize = sColumnInfo[sColumnIndex].mBufferSize * gZtdReadBlockCount;

            STL_TRY( stlAllocDynamicMem( &sDynamicAllocator,
                                         sSize,
                                         (void **) &sDataBuffer,
                                         aErrorStack )
                     == STL_SUCCESS );

            stlMemset( sDataBuffer,
                       0x00,
                       sSize );

            for( sDataValueIndex = 0; sDataValueIndex < gZtdReadBlockCount; sDataValueIndex++ )
            {
                sTempValueBlockList->mValueBlock->mDataValue[sDataValueIndex].mBufferSize = sColumnInfo[sColumnIndex].mBufferSize;
                sTempValueBlockList->mValueBlock->mDataValue[sDataValueIndex].mValue = sDataBuffer;
                sDataBuffer = (stlChar*)sDataBuffer + sColumnInfo[sColumnIndex].mBufferSize;
            }
        }

        /**
         * 마지막 column에서는 다음 column을 위한 메모리를 할당할 필요가 없다.
         */
        if( sColumnIndex < (gZtdColumnCount - 1) )
        {
            sSize = STL_SIZEOF( dtlValueBlockList );
            STL_TRY( stlAllocRegionMem( &sAllocator,
                                        sSize,
                                        (void **) &sTempValueBlockList->mNext,
                                        aErrorStack )
                     == STL_SUCCESS );

            stlMemset( (void *) sTempValueBlockList->mNext,
                       0x00,
                       sSize );
            
            sTempValueBlockList = sTempValueBlockList->mNext;
        }
        else
        {
            /* Do Nothing */
        }
    }
    
    /**
     * write thread에서 사용할 sValueBlockList를 할당받는다.
     */
    sSize = STL_SIZEOF( dtlValueBlockList );
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sWriteValueBlockList,
                                aErrorStack )
             == STL_SUCCESS);

    stlMemset( (void *) sWriteValueBlockList,
               0x00,
               sSize );

    sTempValueBlockList = sWriteValueBlockList;
    for( sColumnIndex = 0; sColumnIndex < gZtdColumnCount; sColumnIndex++ )
    {
        sSize = STL_SIZEOF( dtlValueBlock );
        STL_TRY( stlAllocRegionMem( &sAllocator,
                                    sSize,
                                    (void **) &sTempValueBlockList->mValueBlock,
                                    aErrorStack )
                 == STL_SUCCESS );

        stlMemset( (void *) sTempValueBlockList->mValueBlock,
                   0x00,
                   sSize );

        sSize = STL_SIZEOF( dtlDataValue ) * gZtdReadBlockCount;
        STL_TRY( stlAllocRegionMem( &sAllocator,
                                    sSize,
                                    (void **) &sTempValueBlockList->mValueBlock->mDataValue,
                                    aErrorStack )
                 == STL_SUCCESS );

        stlMemset( (void *) sTempValueBlockList->mValueBlock->mDataValue,
                   0x00,
                   sSize );

        /**
         * 각 column의 data가 저장될 buffer를 할당받는다.
         */
        if( (sColumnInfo[sColumnIndex].mDbType == DTL_TYPE_LONGVARCHAR) ||
            (sColumnInfo[sColumnIndex].mDbType == DTL_TYPE_LONGVARBINARY) )
        {
            /**
             * LONG VARCHAR, LONG VARBINARY인 경우 각 work thread에서의 재할당을 고려하여 메모리를 각 각 할당받는다.
             */
            sSize = ZTD_LONG_DATA_INIT_SIZE;
            
            for( sDataValueIndex = 0; sDataValueIndex < gZtdReadBlockCount; sDataValueIndex++ )
            {
                STL_TRY( stlAllocDynamicMem( &sDynamicAllocator,
                                             sSize,
                                             (void **) &sTempValueBlockList->mValueBlock->mDataValue[sDataValueIndex].mValue,
                                             aErrorStack )
                         == STL_SUCCESS );
                
                stlMemset( (void *) sTempValueBlockList->mValueBlock->mDataValue[sDataValueIndex].mValue,
                           0x00,
                           sSize );
                
                sTempValueBlockList->mValueBlock->mDataValue[sDataValueIndex].mBufferSize = sSize;
            }
        }
        else
        {
            /**
             * LONG VARCHAR, LONGVARBINARY가 아닌 경우 alloc의 횟수를 줄이기 위해서 한 번의 할당 후에 필요한 메모리의 주소를 분배한다.
             */
            sSize = sColumnInfo[sColumnIndex].mBufferSize * gZtdReadBlockCount;
            
            STL_TRY( stlAllocDynamicMem( &sDynamicAllocator,
                                         sSize,
                                         (void **) &sDataBuffer,
                                         aErrorStack )
                     == STL_SUCCESS );
            
            stlMemset( sDataBuffer,
                       0x00,
                       sSize );
            
            for( sDataValueIndex = 0; sDataValueIndex < gZtdReadBlockCount; sDataValueIndex++ )
            {
                sTempValueBlockList->mValueBlock->mDataValue[sDataValueIndex].mValue = sDataBuffer;
                sDataBuffer = (stlChar*)sDataBuffer + sColumnInfo[sColumnIndex].mBufferSize;
            }
        }

        /**
         * 마지막 column에서는 다음 column을 위한 메모리를 할당할 필요가 없다.
         */
        if( sColumnIndex < (gZtdColumnCount - 1) )
        {
            sSize = STL_SIZEOF( dtlValueBlockList );
            STL_TRY( stlAllocRegionMem( &sAllocator,
                                        sSize,
                                        (void **) &sTempValueBlockList->mNext,
                                        aErrorStack )
                     == STL_SUCCESS );

            stlMemset( (void *) sTempValueBlockList->mNext,
                       0x00,
                       sSize );
            
            sTempValueBlockList = sTempValueBlockList->mNext;
        }
        else
        {
            /* Do Nothing */
        }
    }
    
    /**
     * sValueBlockListWriteQueue와 각 buffere에 메모리를 할당 받는다.
     */
    sFileWriteQueue.mPutNumber = 0;
    sFileWriteQueue.mGetNumber = 0;
    
    for( sBufferIndex = 0; sBufferIndex < ZTD_VALUE_BLOCK_LIST_WRITE_QUEUE_SIZE; sBufferIndex++ )
    {
        sSize = STL_SIZEOF( ztdExportValueBlockList );
        STL_TRY( stlAllocRegionMem( &sAllocator,
                                    sSize,
                                    (void **) &sFileWriteQueue.mBuffer[sBufferIndex],
                                    aErrorStack )
                 == STL_SUCCESS );

        stlMemset( (void *) sFileWriteQueue.mBuffer[sBufferIndex],
                   0x00,
                   sSize );

        sSize = STL_SIZEOF( dtlValueBlockList );
        STL_TRY( stlAllocRegionMem( &sAllocator,
                                    sSize,
                                    (void **) &sFileWriteQueue.mBuffer[sBufferIndex]->mValueBlockList,
                                    aErrorStack )
                 == STL_SUCCESS );

        stlMemset( (void *) sFileWriteQueue.mBuffer[sBufferIndex]->mValueBlockList,
                   0x00,
                   sSize );

        sTempValueBlockList = sFileWriteQueue.mBuffer[sBufferIndex]->mValueBlockList;
        
        for( sColumnIndex = 0; sColumnIndex < gZtdColumnCount; sColumnIndex++ )
        {
            sSize = STL_SIZEOF( dtlValueBlock );
            STL_TRY( stlAllocRegionMem( &sAllocator,
                                        sSize,
                                        (void **) &sTempValueBlockList->mValueBlock,
                                        aErrorStack )
                     == STL_SUCCESS );

            stlMemset( (void *) sTempValueBlockList->mValueBlock,
                       0x00,
                       sSize );

            sSize = STL_SIZEOF( dtlDataValue ) * gZtdReadBlockCount;
            STL_TRY( stlAllocRegionMem( &sAllocator,
                                        STL_SIZEOF( dtlDataValue ) * gZtdReadBlockCount,
                                        (void **) &sTempValueBlockList->mValueBlock->mDataValue,
                                        aErrorStack )
                     == STL_SUCCESS );

            stlMemset( (void *) sTempValueBlockList->mValueBlock->mDataValue,
                       0x00,
                       sSize );

            /**
             * 각 column의 data가 저장될 buffer를 할당받는다.
             */
            if( (sColumnInfo[sColumnIndex].mDbType == DTL_TYPE_LONGVARCHAR) ||
                (sColumnInfo[sColumnIndex].mDbType == DTL_TYPE_LONGVARBINARY) )
            {
                /**
                 * LONG VARCHAR, LONG VARBINARY인 경우 각 work thread에서의 재할당을 고려하여 메모리를 각 각 할당받는다.
                 */
                sSize = ZTD_LONG_DATA_INIT_SIZE;
            
                for( sDataValueIndex = 0; sDataValueIndex < gZtdReadBlockCount; sDataValueIndex++ )
                {
                    STL_TRY( stlAllocDynamicMem( &sDynamicAllocator,
                                                 sSize,
                                                 (void **) &sTempValueBlockList->mValueBlock->mDataValue[sDataValueIndex].mValue,
                                                 aErrorStack )
                             == STL_SUCCESS );

                    stlMemset( (void *) sTempValueBlockList->mValueBlock->mDataValue[sDataValueIndex].mValue,
                               0x00,
                               sSize );

                    sTempValueBlockList->mValueBlock->mDataValue[sDataValueIndex].mBufferSize = sSize;
                }
            }
            else
            {
                /**
                 * LONG VARCHAR, LONGVARBINARY가 아닌 경우 alloc의 횟수를 줄이기 위해서 한 번의 할당 후에 필요한 메모리의 주소를 분배한다.
                 */
                sSize = sColumnInfo[sColumnIndex].mBufferSize * gZtdReadBlockCount;

                STL_TRY( stlAllocDynamicMem( &sDynamicAllocator,
                                             sSize,
                                             (void **) &sDataBuffer,
                                             aErrorStack )
                         == STL_SUCCESS );
            
                stlMemset( sDataBuffer,
                           0x00,
                           sSize );
            
                for( sDataValueIndex = 0; sDataValueIndex < gZtdReadBlockCount; sDataValueIndex++ )
                {
                    sTempValueBlockList->mValueBlock->mDataValue[sDataValueIndex].mValue = sDataBuffer;
                    sDataBuffer = (stlChar*)sDataBuffer  + sColumnInfo[sColumnIndex].mBufferSize;
                }
            }

            /**
             * 마지막 column에서는 다음 column을 위한 메모리를 할당할 필요가 없다.
             */
            if( sColumnIndex < (gZtdColumnCount - 1) )
            {
                sSize = STL_SIZEOF( dtlValueBlockList );
                STL_TRY( stlAllocRegionMem( &sAllocator,
                                            sSize,
                                            (void **) &sTempValueBlockList->mNext,
                                            aErrorStack )
                         == STL_SUCCESS );

                stlMemset( (void *) sTempValueBlockList->mNext,
                           0x00,
                           sSize );
                
                sTempValueBlockList = sTempValueBlockList->mNext;
            }
        }
    }
    
    /**
     * semaphore를 생성
     */
    stlSnprintf( sSemaphoreName,
                 STL_MAX_SEM_NAME,
                 "wItem%d",
                 sQueueIndex );

    STL_TRY( stlCreateSemaphore( &sFileWriteQueue.mItem,
                                 sSemaphoreName,
                                 ZTD_VALUE_BLOCK_LIST_WRITE_QUEUE_SIZE,
                                 aErrorStack )
             == STL_SUCCESS );

    /**
     * 초기에는 item이 없으므로 모든 semaphore를 획득해줘야 한다.
     */
    for( i = 0; i < ZTD_VALUE_BLOCK_LIST_WRITE_QUEUE_SIZE; i++ )
    {
        STL_TRY( stlAcquireSemaphore( &sFileWriteQueue.mItem,
                                      aErrorStack )
                 == STL_SUCCESS );
    }

    stlSnprintf( sSemaphoreName,
                 STL_MAX_SEM_NAME,
                 "wEmp%d",
                 sQueueIndex );
        
    STL_TRY( stlCreateSemaphore( &sFileWriteQueue.mEmpty,
                                 sSemaphoreName,
                                 ZTD_VALUE_BLOCK_LIST_WRITE_QUEUE_SIZE,
                                 aErrorStack )
             == STL_SUCCESS );
    sState = 3;
    
    /**
     * 각 thread의 생성 (fetch thread)
     */
    sFetchThreadArg.mEnv              = aEnvHandle;
    sFetchThreadArg.mDynamicAllocator = &sDynamicAllocator;
    sFetchThreadArg.mColumnCount      = gZtdColumnCount;
    sFetchThreadArg.mInputArguments   = &aInputArguments;
    sFetchThreadArg.mControlInfo      = aControlInfo;
    sFetchThreadArg.mValueBlockList   = sFetchValueBlockList;
    sFetchThreadArg.mBinaryWriteQueue = &sFileWriteQueue;
    sFetchThreadArg.mColumnInfo       = sColumnInfo;
    sFetchThreadArg.mFileAndBuffer    = aFileAndBuffer;
    sFetchThreadArg.mHeaderSize       = sHeaderSize;
    
    STL_INIT_ERROR_STACK( &sFetchThreadArg.mErrorStack );

    STL_TRY( stlCreateThread( &sFetchThread,
                              NULL,
                              ztdFetchValueBlockList,
                              (void *) &sFetchThreadArg,
                              aErrorStack )
             == STL_SUCCESS );
    sState = 4;

    /**
     * 각 thread의 생성 (write thread)
     */
    sWriteThreadArg.mEnv              = aEnvHandle;
    sWriteThreadArg.mColumnCount      = gZtdColumnCount;
    sWriteThreadArg.mColumnInfo       = sColumnInfo;
    sWriteThreadArg.mInputArguments   = &aInputArguments;
    sWriteThreadArg.mControlInfo      = aControlInfo;
    sWriteThreadArg.mValueBlockList   = sWriteValueBlockList;
    sWriteThreadArg.mBinaryWriteQueue = &sFileWriteQueue;
    sWriteThreadArg.mAllocator        = &sAllocator;
    sWriteThreadArg.mHeaderSize       = sHeaderSize;

    STL_INIT_ERROR_STACK( &sWriteThreadArg.mErrorStack );
    
    STL_TRY( stlCreateThread( &sWriteThread,
                              NULL,
                              ztdWriteValueBlockList,
                              (void *) &sWriteThreadArg,
                              aErrorStack )
             == STL_SUCCESS );
    sState = 5;

    /**
     * thread join
     */
    sState = 4;
    sJoinErrorStack = &sFetchThreadArg.mErrorStack;
    STL_TRY( stlJoinThread( &sFetchThread,
                            &sReturn,
                            aErrorStack )
             == STL_SUCCESS );

    STL_TRY_THROW( sReturn == STL_SUCCESS,
                   RAMP_ERR_CONSUMER_THREAD );

    sState = 3;
    sJoinErrorStack = &sWriteThreadArg.mErrorStack;
    STL_TRY( stlJoinThread( &sWriteThread,
                            &sReturn,
                            aErrorStack )
             == STL_SUCCESS );
    
    STL_TRY_THROW( sReturn == STL_SUCCESS,
                   RAMP_ERR_CONSUMER_THREAD );

    
    /* Log File log 기록 */
    stlSnprintf( aFileAndBuffer->mLogBuffer,
                 ZTD_MAX_LOG_STRING_SIZE,
                 "COMPLETED IN EXPORTING TABLE: %s.%s, TOTAL %ld RECORDS ",
                 aControlInfo->mSchema,
                 aControlInfo->mTable,
                 gZtdCompleteCount );
    
    /**
     * semaphore의 반환
     */
    
    sState = 2;
    STL_TRY( stlDestroySemaphore( &sFileWriteQueue.mItem,
                                  aErrorStack )
             == STL_SUCCESS );
    
    STL_TRY( stlDestroySemaphore( &sFileWriteQueue.mEmpty,
                                  aErrorStack )
             == STL_SUCCESS );
    
    /**
     * free dynamic memory
     */
    sState = 1;
    STL_TRY( stlDestroyDynamicAllocator( &sDynamicAllocator,
                                         aErrorStack )
             == STL_SUCCESS );
    
    /**
     * free region memory
     */
    sState = 0;
    STL_TRY( stlDestroyRegionAllocator( &sAllocator,
                                        aErrorStack )
             == STL_SUCCESS );

    return STL_SUCCESS;

    STL_CATCH( RAMP_ERR_FILE_SIZE )
    {
        stlPushError( STL_ERROR_LEVEL_ABORT,
                      ZTD_ERRCODE_INVALID_FILE_SIZE,
                      NULL,
                      aErrorStack,
                      sMinFileSize );
    }

    STL_CATCH( RAMP_ERR_CONSUMER_THREAD )
    {
        stlAppendErrors( aErrorStack, sJoinErrorStack );
    }

    STL_FINISH;

    gZtdRunState = STL_FALSE;

    if( sIsAllocStmtHandle == STL_TRUE )
    {
        (void) ztdCheckError( aEnvHandle,
                              aDbcHandle,
                              sStmt,
                              0,
                              aFileAndBuffer,
                              NULL,
                              aErrorStack );
                              
        (void) SQLFreeHandle( SQL_HANDLE_STMT,
                              sStmt );
    }
    
    switch( sState )
    {
        case 5:
            (void) stlJoinThread( &sFetchThread,
                                  &sReturn,
                                  aErrorStack );
            if( sReturn == STL_FAILURE )
            {
                stlAppendErrors( aErrorStack, &sFetchThreadArg.mErrorStack );
            }
        case 4:
            (void) stlJoinThread( &sWriteThread,
                                  &sReturn,
                                  aErrorStack );
            if( sReturn == STL_FAILURE )
            {
                stlAppendErrors( aErrorStack, &sWriteThreadArg.mErrorStack );
            }
        case 3:
            (void) stlDestroySemaphore( &sFileWriteQueue.mItem,
                                        aErrorStack );
            
            (void) stlDestroySemaphore( &sFileWriteQueue.mEmpty,
                                        aErrorStack );
        case 2:
            (void) stlDestroyDynamicAllocator( &sDynamicAllocator,
                                               aErrorStack );
        case 1:
            (void) stlDestroyRegionAllocator( &sAllocator,
                                              aErrorStack );
        default :
            break;
    }

    return STL_FAILURE;
}

stlStatus ztdAllocValueBlockList( stlAllocator         * aAllocator,
                                  stlDynamicAllocator  * aDynamicAllocator,
                                  dtlValueBlockList   ** aValueBlockList,
                                  ztdColumnInfo        * aColumnInfo,
                                  SQLSMALLINT            aColumnCount,
                                  stlInt32               aReadBlockCount,
                                  stlErrorStack        * aErrorStack )
{
    dtlValueBlockList ** sValueBlockList;
    stlInt32             sColumnIndex;
    dtlValueBlock      * sValueBlock = NULL;
    dtlDataValue       * sDataValue = NULL;
    void               * sDataBuffer = NULL;
    dtlDataType          sDbType;
    stlInt32             sAlignSize;
    stlInt32             i;
    
    /**
     * dtlValueBlockList* array를 할당한다.
     */
    STL_TRY( stlAllocRegionMem( aAllocator,
                                STL_SIZEOF( dtlValueBlockList* ) * aColumnCount,
                                (void **) &sValueBlockList,
                                aErrorStack )
             == STL_SUCCESS );

    stlMemset( sValueBlockList,
               0x00,
               STL_SIZEOF( dtlValueBlockList* ) * aColumnCount );

    for( sColumnIndex = 0; sColumnIndex < aColumnCount; sColumnIndex++ )
    {
        /**
         * dtlValueBlockList를 할당한다.
         */
        STL_TRY( stlAllocRegionMem( aAllocator,
                                    STL_SIZEOF( dtlValueBlockList ),
                                    (void **) & sValueBlockList[sColumnIndex],
                                    aErrorStack )
                 == STL_SUCCESS );

        stlMemset( (void *) sValueBlockList[sColumnIndex],
                   0x00,
                   STL_SIZEOF( dtlValueBlockList ) );

        /**
         * dtlValueBlock을 할당한다.
         */
        STL_TRY( stlAllocRegionMem( aAllocator,
                                    STL_SIZEOF( dtlValueBlock ),
                                    (void **) &sValueBlock,
                                    aErrorStack )
                 == STL_SUCCESS );
        
        stlMemset( (void *) sValueBlock, 0x00, STL_SIZEOF( dtlValueBlock ) );

        sValueBlockList[sColumnIndex]->mValueBlock = sValueBlock;

        /**
         * dtlDataValue를 할당한다.
         */
        STL_TRY( stlAllocRegionMem( aAllocator,
                                    STL_SIZEOF( dtlDataValue ) * gZtdReadBlockCount,
                                    (void **) &sDataValue,
                                    aErrorStack )
                 == STL_SUCCESS );
        
        stlMemset( (void *) sDataValue,
                   0x00,
                   STL_SIZEOF( dtlDataValue ) * aReadBlockCount );

        sValueBlock->mDataValue = sDataValue;
        
        sDbType = aColumnInfo[sColumnIndex].mDbType;

        /**
         * 각 column의 data가 저장될 buffer를 할당받는다.
         */
        if( (sDbType == DTL_TYPE_LONGVARCHAR) || (sDbType == DTL_TYPE_LONGVARBINARY) )
        {
            /**
             * LONG VARCHAR, LONG VARBINARY인 경우 각 work thread에서의
             * 재할당을 고려하여 메모리를 각 각 할당받는다.
             */
            
            sAlignSize = STL_ALIGN_DEFAULT( ZTD_LONG_DATA_INIT_SIZE );

            for( i = 0; i < aReadBlockCount; i++ )
            {
                STL_TRY( stlAllocDynamicMem( aDynamicAllocator,
                                             sAlignSize,
                                             (void **) &sDataValue[i].mValue,
                                             aErrorStack )
                         == STL_SUCCESS );

                sDataValue[i].mType        = sDbType;
                sDataValue[i].mLength      = 0;
                sDataValue[i].mBufferSize  = sAlignSize;
            }
        }
        else
        {
            /**
             * LONG VARCHAR, LONGVARCHAR가 아닌 경우 alloc의 횟수를 줄이기 위해서
             * 한 번의 할당 후에 필요한 메모리의 주소를 분배한다.
             */

            sAlignSize = STL_ALIGN_DEFAULT( aColumnInfo[sColumnIndex].mBufferSize );
            
            STL_TRY( stlAllocDynamicMem( aDynamicAllocator,
                                         sAlignSize * aReadBlockCount,
                                         (void **) &sDataBuffer,
                                         aErrorStack )
                     == STL_SUCCESS );

            stlMemset( sDataBuffer,
                       0x00,
                       sAlignSize * aReadBlockCount );

            for( i = 0; i < aReadBlockCount; i++ )
            {
                sDataValue[i].mType        = sDbType;
                sDataValue[i].mValue       = sDataBuffer;
                sDataValue[i].mLength      = 0;
                sDataValue[i].mBufferSize  = sAlignSize;
                sDataBuffer                = (stlChar*)sDataBuffer + sAlignSize;
            }
        }
    }

    *aValueBlockList = (dtlValueBlockList*)sValueBlockList;

    return STL_SUCCESS;

    STL_FINISH;

    return STL_FAILURE;
}

/**
 * @brief binary format의 데이터를 gliese에 uploader한다.
 * @param [in]  aEnvHandle          SQLHENV
 * @param [in]  aDbcHandle          SQLHDBC  
 * @param [in]  ztdInputArguments   main에서 할당한 Region Memory
 * @param [in]  aControlInfo        Control File의 내용을 저장한 struct
 * @param [in]  aFileAndBuffer      File Descriptor 와 log를 담는 struct
 * @param [out] aErrorStack         error stack
 */
stlStatus ztdImportBinaryData( SQLHENV             aEnvHandle,
                               SQLHDBC             aDbcHandle,
                               ztdInputArguments   aInputArguments,
                               ztdControlInfo    * aControlInfo,
                               ztdFileAndBuffer  * aFileAndBuffer,
                               stlErrorStack     * aErrorStack )
{
    SQLHSTMT                     sStmt                              = NULL;
    SQLHDESC                     sDesc                              = NULL;

    ztdColumnInfo              * sColumnInfo                        = NULL;
    stlChar                      sSqlString[ZTD_MAX_COMMAND_BUFFER_SIZE];
    stlInt32                     sIdxColumn                         = 0;
    
    stlAllocator                 sAllocator;
    stlDynamicAllocator          sDynamicAllocator;

    ztdBinaryReadDataQueue     * sExecuteValueBlockListQueue        = NULL;
    ztdBinaryReadDataQueue     * sWriteBadValueBlockListQueue       = NULL;
    
    stlInt32                     sQueueIndex                        = 0;

    stlThread                  * sReadValueBlockThread              = NULL;
    stlThread                  * sExecuteValueBlockThread           = NULL;
    stlThread                  * sWriteBadValueBlockThread          = NULL;
    
    ztdBinaryModeReadThreadArg * sReadValueBlockListArg             = NULL;
    ztdBinaryModeReadThreadArg * sExecuteValueBlockListArg          = NULL;
    ztdBinaryModeReadThreadArg * sWriteBadValueBlockListArg         = NULL;

    stlInt32                     sSize                              = 0;
    
    ztdImportValueBlockList    * sReadValueBlockList                = NULL;
    ztdImportValueBlockList    * sExecuteValueBlockList             = NULL;
    ztdImportValueBlockList    * sWriteBadValueBlockList            = NULL;

    ztdValueBlockColumnInfo    * sValueBlockColumnInfo              = NULL;

    stlChar                      sSemaphoreName[STL_MAX_SEM_NAME];
    
    stlInt32                     i                                  = 0;

    stlInt32                     sHeaderSize                        = 0;
    stlInt32                     sReadHeaderSize                    = 0;

    stlInt32                     sThreadUnit                        = 0;
    
    stlInt32                     sPerm                              = 0;
    stlStatus                    sReturn                            = STL_FAILURE;
    
    stlInt32                     sState                             = 0;
    
    dtlFuncVector                sVector;
    dtlDataType                  sDbType;
    stlInt64                     sLength;

    stlBool                      sIsAllocStmtHandle                 = STL_FALSE;

    stlErrorStack              * sJoinErrorStack                    = NULL;
    
    stlChar                      sTypeName[STL_MAX_SQL_IDENTIFIER_LENGTH];
    /*
     * FUNCTION VECTOR 설정
     */
    stlMemset( (void*)&sVector, 0x00, STL_SIZEOF(dtlFuncVector) );

    sVector.mGetCharSetIDFunc = ztdGetDBCharSetIDFunc;

    STL_TRY( stlCreateRegionAllocator( &sAllocator,
                                       ZTD_REGION_INIT_SIZE,
                                       ZTD_REGION_NEXT_SIZE,
                                       aErrorStack )
             == STL_SUCCESS );
    sState = 1;

    STL_TRY( stlCreateDynamicAllocator( &sDynamicAllocator,
                                        ZTD_DYNAMIC_INIT_SIZE,
                                        ZTD_DYNAMIC_NEXT_SIZE,
                                        aErrorStack )
             == STL_SUCCESS );
    sState = 2;

    /**
     * database 연결
     */
    STL_TRY( SQLAllocHandle( SQL_HANDLE_STMT,
                             aDbcHandle,
                             &sStmt )
             == SQL_SUCCESS );
    sIsAllocStmtHandle = STL_TRUE;

    /**
     * Import중 Table을 삭제할 수 없도록 Lock을 잡는다. (IX Lock)
     */
    if( aControlInfo->mSchema[0] == 0x00 )
    {
        stlSnprintf( sSqlString,
                     ZTD_MAX_COMMAND_BUFFER_SIZE,
                     "LOCK TABLE %s IN ROW EXCLUSIVE MODE NOWAIT",
                     aControlInfo->mTable );
    }
    else
    {
        stlSnprintf( sSqlString,
                     ZTD_MAX_COMMAND_BUFFER_SIZE,
                     "LOCK TABLE %s.%s IN ROW EXCLUSIVE MODE NOWAIT",
                     aControlInfo->mSchema,
                     aControlInfo->mTable );
    }

    STL_TRY( SQLExecDirect( sStmt,
                            (SQLCHAR *) sSqlString,
                            SQL_NTS )
             == SQL_SUCCESS );

    /**
     * table의 column 개수를 얻는다.
     */
    if( aControlInfo->mSchema[0] == 0x00 )
    {
        stlSnprintf( sSqlString,
                     ZTD_MAX_COMMAND_BUFFER_SIZE,
                     "SELECT * FROM %s",
                     aControlInfo->mTable );
    }
    else
    {
        stlSnprintf( sSqlString,
                     ZTD_MAX_COMMAND_BUFFER_SIZE,
                     "SELECT * FROM %s.%s",
                     aControlInfo->mSchema,
                     aControlInfo->mTable );
    }

    STL_TRY( SQLPrepare( sStmt,
                         (SQLCHAR *) sSqlString,
                         SQL_NTS )
             == SQL_SUCCESS );
    
    STL_TRY( SQLNumResultCols( sStmt, &gZtdColumnCount )
             == SQL_SUCCESS );

    STL_TRY( SQLGetStmtAttr( sStmt,
                             SQL_ATTR_IMP_ROW_DESC,
                             &sDesc,
                             SQL_IS_POINTER,
                             NULL )
             == SQL_SUCCESS );

    if( aControlInfo->mSchema[0] == 0x00 )
    {
        STL_TRY( SQLGetDescField( sDesc,
                                  1,
                                  SQL_DESC_SCHEMA_NAME,
                                  aControlInfo->mSchema,
                                  ZTD_MAX_SCHEMA_NAME,
                                  NULL )
                 == SQL_SUCCESS );
    }
    else
    {
        /* Do Notting */
    }

    /**
     * sColumnInformat을 할당한다.
     */
    sSize = STL_SIZEOF( ztdValueBlockColumnInfo ) * gZtdColumnCount;
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sValueBlockColumnInfo,
                                aErrorStack )
             == STL_SUCCESS );

    stlMemset( (void *) sValueBlockColumnInfo,
               0x00,
               sSize );
    
    /**
     * column의 개수 만큼 sColumnInfo를 할당받는다.
     */
    sSize = STL_SIZEOF( ztdColumnInfo ) * gZtdColumnCount;
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sColumnInfo,
                                aErrorStack )
             == STL_SUCCESS );

    stlMemset( (void *) sColumnInfo,
               0x00,
               sSize );

    /**
     * gZtdEndian, gZtdReadBlockCount에 값 설정
     */
    gZtdEndian = STL_PLATFORM_ENDIAN;

    gZtdReadBlockCount = ZTD_DEFAULT_BLOCK_READ_COUNT;
    
    /**
     * 각 column의 size를 구한다.
     */
    for( sIdxColumn = 0; sIdxColumn < gZtdColumnCount; sIdxColumn++ )
    {
        STL_TRY( SQLDescribeCol( sStmt,
                                 sIdxColumn + 1,
                                 (SQLCHAR*)sColumnInfo[sIdxColumn].mName,
                                 ZTD_MAX_COLUMN_NAME,
                                 &sColumnInfo[sIdxColumn].mNameLength,
                                 &sColumnInfo[sIdxColumn].mSQLType,
                                 &sColumnInfo[sIdxColumn].mDataSize,
                                 &sColumnInfo[sIdxColumn].mDecimalDigits,
                                 &sColumnInfo[sIdxColumn].mNullable )
                 == SQL_SUCCESS );

        STL_TRY( SQLGetDescField( sDesc,
                                  sIdxColumn + 1,
                                  SQL_DESC_TYPE_NAME,
                                  sTypeName,
                                  STL_MAX_SQL_IDENTIFIER_LENGTH,
                                  NULL )
                 == SQL_SUCCESS );

        STL_TRY( SQLGetDescField( sDesc,
                                  sIdxColumn + 1,
                                  SQL_DESC_DISPLAY_SIZE,
                                  &sColumnInfo[sIdxColumn].mDisplaySize,
                                  STL_SIZEOF( SQLLEN ),
                                  NULL )
                 == SQL_SUCCESS );
        
        sDbType = ztdGetDtlType( sColumnInfo[sIdxColumn].mSQLType );

        /**
         * LONG VARCHAR, LONG VARBINARY를 포함한 경우 block count를 20으로 한다.
         * block count를 40으로 할 경우 fetch받을 때, 할당할 수 있는 dynamic memory가 부족하게 된다.
         */
        switch( sDbType )
        {
            case DTL_TYPE_CHAR :
            case DTL_TYPE_VARCHAR :
                STL_TRY( dtlGetDataValueBufferSize( sDbType,
                                                    sColumnInfo[sIdxColumn].mDataSize,
                                                    DTL_STRING_LENGTH_UNIT_CHARACTERS,
                                                    &sLength,
                                                    &sVector,
                                                    NULL,
                                                    aErrorStack ) == STL_SUCCESS );
                break;
            case DTL_TYPE_LONGVARCHAR :
                gZtdReadBlockCount = 20;
                
                STL_TRY( dtlGetDataValueBufferSize( sDbType,
                                                    sColumnInfo[sIdxColumn].mDataSize,
                                                    DTL_STRING_LENGTH_UNIT_OCTETS,
                                                    &sLength,
                                                    &sVector,
                                                    NULL,
                                                    aErrorStack ) == STL_SUCCESS );
                break;
            case DTL_TYPE_LONGVARBINARY :
                gZtdReadBlockCount = 20;
                
                STL_TRY( dtlGetDataValueBufferSize( sDbType,
                                                    sColumnInfo[sIdxColumn].mDataSize,
                                                    DTL_STRING_LENGTH_UNIT_NA,
                                                    &sLength,
                                                    &sVector,
                                                    NULL,
                                                    aErrorStack ) == STL_SUCCESS );
                break;
            case DTL_TYPE_FLOAT :
                if( stlStrcmp( sTypeName, "NUMBER" ) == 0 )
                {
                    sDbType = DTL_TYPE_NUMBER;
                    
                    STL_TRY( dtlGetDataValueBufferSize( sDbType,
                                                        DTL_NUMERIC_MAX_PRECISION,
                                                        DTL_STRING_LENGTH_UNIT_NA,
                                                        &sLength,
                                                        &sVector,
                                                        NULL,
                                                        aErrorStack ) == STL_SUCCESS );
                }
                else
                {
                    STL_TRY( dtlGetDataValueBufferSize( sDbType,
                                                        sColumnInfo[sIdxColumn].mDataSize,
                                                        DTL_STRING_LENGTH_UNIT_NA,
                                                        &sLength,
                                                        &sVector,
                                                        NULL,
                                                        aErrorStack ) == STL_SUCCESS );
                }
                break;
            case DTL_TYPE_TIMESTAMP :
                STL_TRY( dtlGetDataValueBufferSize( sDbType,
                                                    sColumnInfo[sIdxColumn].mDataSize,
                                                    DTL_STRING_LENGTH_UNIT_NA,
                                                    &sLength,
                                                    &sVector,
                                                    NULL,
                                                    aErrorStack ) == STL_SUCCESS );
                
                if( stlStrcmp( sTypeName, "DATE" ) == 0 )
                {
                    sDbType = DTL_TYPE_DATE;
                }
                else
                {
                    /* Do Nothing */
                }
                break;
            default :
                STL_TRY( dtlGetDataValueBufferSize( sDbType,
                                                    sColumnInfo[sIdxColumn].mDataSize,
                                                    DTL_STRING_LENGTH_UNIT_NA,
                                                    &sLength,
                                                    &sVector,
                                                    NULL,
                                                    aErrorStack ) == STL_SUCCESS );
                break;
        }

        sColumnInfo[sIdxColumn].mDbType = sDbType;
        sColumnInfo[sIdxColumn].mBufferSize = sLength;
    }

    sIsAllocStmtHandle = STL_FALSE;
    STL_TRY( SQLFreeHandle( SQL_HANDLE_STMT,
                            sStmt )
             == SQL_SUCCESS );

    /**
     * open bad file.
     */
    sPerm = STL_FPERM_UREAD | STL_FPERM_UWRITE |
        STL_FPERM_GREAD | STL_FPERM_GWRITE |
        STL_FPERM_WREAD | STL_FPERM_WWRITE ;
    
    STL_TRY( stlOpenFile( &gZtdBadFile,
                          aInputArguments.mBad,
                          STL_FOPEN_WRITE | STL_FOPEN_CREATE | STL_FOPEN_TRUNCATE | STL_FOPEN_LARGEFILE,
                          sPerm,
                          aErrorStack )
             == STL_SUCCESS );
    sState = 3;

    /**
     * data file의 size 구하기
     */
    sHeaderSize = ( ZTD_FILE_INFORMATION_SIZE +
                    ( ZTD_COLUMN_INFORMATION_SIZE * gZtdColumnCount ) );

    ZTD_ALIGN( sReadHeaderSize, sHeaderSize, ZTD_BINARY_HEADER_SIZE );

    /**
     * execute thread의 개수를 설정한다.
     */
    sThreadUnit                        = aInputArguments.mThreadUnit;
    gZtdBinaryModeExecuteRunningCount  = sThreadUnit;

    /**
     * read thread에서 사용할 sValueBlockList를 할당받는다.
     */
    sSize = STL_SIZEOF( ztdImportValueBlockList );

    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sReadValueBlockList,
                                aErrorStack )
             == STL_SUCCESS );
    stlMemset( (void *) sReadValueBlockList, 0x00, sSize );

    STL_TRY( ztdAllocValueBlockList( &sAllocator,
                                     &sDynamicAllocator,
                                     (dtlValueBlockList**)&sReadValueBlockList->mValueBlockList,
                                     sColumnInfo,
                                     gZtdColumnCount,
                                     gZtdReadBlockCount,
                                     aErrorStack )
             == STL_SUCCESS );
    
    /**
     * thread unit만큼 execute thread에서 사용할 sValueBlockList를 할당받는다.
     */
    sSize = STL_SIZEOF( ztdImportValueBlockList ) * sThreadUnit;
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sExecuteValueBlockList,
                                aErrorStack )
             == STL_SUCCESS );
    stlMemset( (void *) sExecuteValueBlockList, 0x00, sSize );

    for( i = 0; i < sThreadUnit; i++ )
    {
        STL_TRY( ztdAllocValueBlockList( &sAllocator,
                                         &sDynamicAllocator,
                                         (dtlValueBlockList**)&sExecuteValueBlockList[i].mValueBlockList,
                                         sColumnInfo,
                                         gZtdColumnCount,
                                         gZtdReadBlockCount,
                                         aErrorStack )
                 == STL_SUCCESS );
    }
    
    /**
     * write bad thread에서 사용할 sValueBlockList를 할당받는다.
     */
    sSize = STL_SIZEOF( ztdImportValueBlockList );
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sWriteBadValueBlockList,
                                aErrorStack )
             == STL_SUCCESS );
    stlMemset( (void *) sWriteBadValueBlockList, 0x00, sSize );

    STL_TRY( ztdAllocValueBlockList( &sAllocator,
                                     &sDynamicAllocator,
                                     (dtlValueBlockList**)&sWriteBadValueBlockList->mValueBlockList,
                                     sColumnInfo,
                                     gZtdColumnCount,
                                     gZtdReadBlockCount,
                                     aErrorStack )
             == STL_SUCCESS );
    
    /**
     * execute queue의 자원을 할당한다.
     */
    sSize = STL_SIZEOF( ztdBinaryReadDataQueue ) * sThreadUnit;
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sExecuteValueBlockListQueue,
                                aErrorStack )
             == STL_SUCCESS );

    stlMemset( (void *) sExecuteValueBlockListQueue,
               0x00,
               sSize );
    
    for( sQueueIndex = 0; sQueueIndex < sThreadUnit; sQueueIndex++ )
    {
        /**
         * buffer의 각 공간에 메모리를 할당한다.
         */
        sSize = STL_SIZEOF( ztdImportValueBlockList ) * ZTD_EXECUTE_QUEUE_SIZE;
        STL_TRY( stlAllocRegionMem( &sAllocator,
                                    sSize,
                                    (void **) &sExecuteValueBlockListQueue[sQueueIndex].mBuffer,
                                    aErrorStack )
                 == STL_SUCCESS );
        stlMemset( (void *) sExecuteValueBlockListQueue[sQueueIndex].mBuffer, 0x00, sSize );

        for( i = 0; i < ZTD_EXECUTE_QUEUE_SIZE; i++ )
        {
            STL_TRY( ztdAllocValueBlockList( &sAllocator,
                                             &sDynamicAllocator,
                                             (dtlValueBlockList**)&sExecuteValueBlockListQueue[sQueueIndex].mBuffer[i],
                                             sColumnInfo,
                                             gZtdColumnCount,
                                             gZtdReadBlockCount,
                                             aErrorStack )
                     == STL_SUCCESS );
        }
    }

    /**
     * write bad queue의 자원을 할당한다.
     */
    sSize = STL_SIZEOF( ztdBinaryReadDataQueue );
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sWriteBadValueBlockListQueue,
                                aErrorStack )
             == STL_SUCCESS );

    stlMemset( (void *) sWriteBadValueBlockListQueue,
               0x00,
               sSize );
    
    /**
     * buffer의 각 공간에 메모리를 할당한다.
     */
    sSize = STL_SIZEOF( ztdImportValueBlockList ) * ZTD_WRITE_BAD_QUEUE_SIZE;
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sWriteBadValueBlockListQueue->mBuffer,
                                aErrorStack )
             == STL_SUCCESS );
    stlMemset( (void *) sWriteBadValueBlockListQueue->mBuffer, 0x00, sSize );

    for( i = 0; i < ZTD_WRITE_BAD_QUEUE_SIZE; i++ )
    {
        STL_TRY( ztdAllocValueBlockList( &sAllocator,
                                         &sDynamicAllocator,
                                         (dtlValueBlockList**)&sWriteBadValueBlockListQueue->mBuffer[i],
                                         sColumnInfo,
                                         gZtdColumnCount,
                                         gZtdReadBlockCount,
                                         aErrorStack )
                 == STL_SUCCESS );
    }

    /**
     * semaphore를 생성
     */
    for( sQueueIndex = 0; sQueueIndex < sThreadUnit; sQueueIndex++ )
    {
        stlSnprintf( sSemaphoreName,
                     STL_MAX_SEM_NAME,
                     "exIt%d",
                     sQueueIndex );
        
        STL_TRY( stlCreateSemaphore( &sExecuteValueBlockListQueue[sQueueIndex].mItem,
                                     sSemaphoreName,
                                     ZTD_EXECUTE_QUEUE_SIZE,
                                     aErrorStack )
                 == STL_SUCCESS );

        /**
         * 초기에 item은 존재하지 않으므로 모두 acquire한다.
         */
        for( i = 0; i < ZTD_EXECUTE_QUEUE_SIZE; i++ )
        {
            STL_TRY( stlAcquireSemaphore( &sExecuteValueBlockListQueue[sQueueIndex].mItem,
                                          aErrorStack )
                     == STL_SUCCESS );
        }

        stlSnprintf( sSemaphoreName,
                     STL_MAX_SEM_NAME,
                     "exEm%d",
                     sQueueIndex );
        
        STL_TRY( stlCreateSemaphore( &sExecuteValueBlockListQueue[sQueueIndex].mEmpty,
                                     sSemaphoreName,
                                     ZTD_EXECUTE_QUEUE_SIZE,
                                     aErrorStack )
                 == STL_SUCCESS );
        
        sState = 4;
    }
    
    stlSnprintf( sSemaphoreName,
                 STL_MAX_SEM_NAME,
                 "wbIt",
                 sQueueIndex );
        
    STL_TRY( stlCreateSemaphore( &sWriteBadValueBlockListQueue->mItem,
                                 sSemaphoreName,
                                 ZTD_WRITE_BAD_QUEUE_SIZE,
                                 aErrorStack )
             == STL_SUCCESS );
    sState = 5;
    /**
     * 초기 item은 존재하지 않으므로 모두 acquire한다.
     */
    for( i = 0; i < ZTD_WRITE_BAD_QUEUE_SIZE; i++ )
    {
        STL_TRY( stlAcquireSemaphore( &sWriteBadValueBlockListQueue->mItem,
                                      aErrorStack )
                 == STL_SUCCESS );
    }

    stlSnprintf( sSemaphoreName,
                 STL_MAX_SEM_NAME,
                 "wbEm",
                 sQueueIndex );
        
    STL_TRY( stlCreateSemaphore( &sWriteBadValueBlockListQueue->mEmpty,
                                 sSemaphoreName,
                                 ZTD_WRITE_BAD_QUEUE_SIZE,
                                 aErrorStack )
             == STL_SUCCESS );
    sState = 6;

    /**
     * 각 thread의 생성 (read ValueBlockList thread)
     */
    sSize = STL_SIZEOF( stlThread );
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sReadValueBlockThread,
                                aErrorStack )
             == STL_SUCCESS );

    stlMemset( (void *) sReadValueBlockThread,
               0x00,
               sSize );

    sSize = STL_SIZEOF( ztdBinaryModeReadThreadArg );
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sReadValueBlockListArg,
                                aErrorStack )
             == STL_SUCCESS );

    stlMemset( (void *) sReadValueBlockListArg,
               0x00,
               sSize );
    
    sReadValueBlockListArg->mInputArguments             = &aInputArguments;
    sReadValueBlockListArg->mControlInfo                = aControlInfo;
    sReadValueBlockListArg->mColumnCount                = gZtdColumnCount;
    sReadValueBlockListArg->mValueBlockList             = sReadValueBlockList->mValueBlockList;
    sReadValueBlockListArg->mExecuteValueBlockList      = (ztdImportValueBlockList *)sExecuteValueBlockList;
    sReadValueBlockListArg->mWriteBadValueBlockList     = (ztdImportValueBlockList *)sWriteBadValueBlockList;
    sReadValueBlockListArg->mBinaryExecuteQueue         = sExecuteValueBlockListQueue;
    sReadValueBlockListArg->mBinaryWriteBadQueue        = sWriteBadValueBlockListQueue;
    sReadValueBlockListArg->mColumnInfo                 = sColumnInfo;  
    sReadValueBlockListArg->mValueBlockColumnInfo       = sValueBlockColumnInfo;
    sReadValueBlockListArg->mHeaderSize                 = sReadHeaderSize;
    sReadValueBlockListArg->mThreadUnit                 = sThreadUnit;
    sReadValueBlockListArg->mAllocator                  = &sAllocator;
    sReadValueBlockListArg->mDynamicAllocator           = &sDynamicAllocator;
    sReadValueBlockListArg->mFileAndBuffer              = aFileAndBuffer;

    STL_INIT_ERROR_STACK( &sReadValueBlockListArg->mErrorStack );
    
    STL_TRY( stlCreateThread( sReadValueBlockThread,
                              NULL,
                              ztdReadValueBlockList,
                              (void *) sReadValueBlockListArg,
                              aErrorStack )
             == STL_SUCCESS );
    sState = 7;
    
    /**
     * 각 thread의 생성 (execute thread)
     */
    sSize = ( STL_SIZEOF( stlThread ) * sThreadUnit );
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sExecuteValueBlockThread,
                                aErrorStack )
             == STL_SUCCESS );

    stlMemset( (void *) sExecuteValueBlockThread,
               0x00,
               sSize );

    sSize = ( STL_SIZEOF( ztdBinaryModeReadThreadArg ) * sThreadUnit );
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sExecuteValueBlockListArg,
                                aErrorStack )
             == STL_SUCCESS );

    stlMemset( (void *) sExecuteValueBlockListArg,
               0x00,
               sSize );
    
    for( i = 0; i < sThreadUnit; i++ )
    {
        sExecuteValueBlockListArg[i].mEnv                     = aEnvHandle;
        sExecuteValueBlockListArg[i].mInputArguments          = &aInputArguments;
        sExecuteValueBlockListArg[i].mControlInfo             = aControlInfo;
        sExecuteValueBlockListArg[i].mColumnCount             = gZtdColumnCount;
        sExecuteValueBlockListArg[i].mValueBlockList          = sExecuteValueBlockList[i].mValueBlockList;
        sExecuteValueBlockListArg[i].mBinaryExecuteQueue      = sExecuteValueBlockListQueue;
        sExecuteValueBlockListArg[i].mBinaryWriteBadQueue     = sWriteBadValueBlockListQueue;
        sExecuteValueBlockListArg[i].mMyQueueIndex            = i;
        sExecuteValueBlockListArg[i].mThreadUnit              = sThreadUnit;
        sExecuteValueBlockListArg[i].mFileAndBuffer           = aFileAndBuffer;

        STL_INIT_SPIN_LOCK( sExecuteValueBlockListQueue[i].mPushLock );
        
        STL_INIT_ERROR_STACK( &sExecuteValueBlockListArg[i].mErrorStack );
        
        STL_TRY( stlCreateThread( &sExecuteValueBlockThread[i],
                                  NULL,
                                  ztdExecuteValueBlockList,
                                  (void *) &sExecuteValueBlockListArg[i],
                                  aErrorStack )
                 == STL_SUCCESS );
        sState = 8;
    }

    /**
     * 각 thread의 생성 (write bad thread)
     */
    sSize = STL_SIZEOF( stlThread );
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sWriteBadValueBlockThread,
                                aErrorStack )
             == STL_SUCCESS );
    
    stlMemset( (void *) sWriteBadValueBlockThread,
               0x00,
               sSize );

    sSize = STL_SIZEOF( ztdBinaryModeReadThreadArg );
    STL_TRY( stlAllocRegionMem( &sAllocator,
                                sSize,
                                (void **) &sWriteBadValueBlockListArg,
                                aErrorStack )
             == STL_SUCCESS );

    stlMemset( (void *) sWriteBadValueBlockListArg,
               0x00,
               sSize );

    STL_INIT_SPIN_LOCK( sWriteBadValueBlockListQueue->mPushLock );
    
    sWriteBadValueBlockListArg->mEnv                     = aEnvHandle;
    sWriteBadValueBlockListArg->mInputArguments          = &aInputArguments;
    sWriteBadValueBlockListArg->mControlInfo             = aControlInfo;
    sWriteBadValueBlockListArg->mColumnCount             = gZtdColumnCount;
    sWriteBadValueBlockListArg->mValueBlockList          = sWriteBadValueBlockList->mValueBlockList;
    sWriteBadValueBlockListArg->mBinaryWriteBadQueue     = sWriteBadValueBlockListQueue;
    sWriteBadValueBlockListArg->mColumnInfo              = sColumnInfo;
    
    STL_INIT_ERROR_STACK( &sWriteBadValueBlockListArg->mErrorStack );
    
    STL_TRY( stlCreateThread( sWriteBadValueBlockThread,
                              NULL,
                              ztdWriteBadValueBlockList,
                              (void *) sWriteBadValueBlockListArg,
                              aErrorStack )
             == STL_SUCCESS );
    sState = 9;
    
    /**
     * thread join
     */
    sState = 8;
    sJoinErrorStack = &sWriteBadValueBlockListArg->mErrorStack;
    STL_TRY( stlJoinThread( sWriteBadValueBlockThread,
                            &sReturn,
                            aErrorStack )
             == STL_SUCCESS );
    
    STL_TRY_THROW( sReturn == STL_SUCCESS,
                   RAMP_ERR_WRITE_BAD_THREAD );
    
    sState = 7;
    for( i = 0; i < sThreadUnit; i++ )
    {
        sJoinErrorStack = &sExecuteValueBlockListArg[i].mErrorStack;
        STL_TRY( stlJoinThread( &sExecuteValueBlockThread[i],
                                &sReturn,
                                aErrorStack )
                 == STL_SUCCESS );

        STL_TRY_THROW( sReturn == STL_SUCCESS,
                       RAMP_ERR_EXECUTE_THREAD );
    }

    sState = 6;
    sJoinErrorStack = &sReadValueBlockListArg->mErrorStack;
    STL_TRY( stlJoinThread( sReadValueBlockThread,
                            &sReturn,
                            aErrorStack )
             == STL_SUCCESS );

    STL_TRY_THROW( sReturn == STL_SUCCESS,
                   RAMP_ERR_READ_THREAD );

    /* Log File에 기록할 Log 설정 */
    if( (gZtdErrorCount > gZtdMaxErrorCount) &&
        (gZtdMaxErrorCount > 0) )
    {
        stlSnprintf( aFileAndBuffer->mLogBuffer,
                     ZTD_MAX_LOG_STRING_SIZE,
                     "INCOMPLETED IN IMPORTING TABLE: %s.%s, SUCCEEDED %u RECORDS, ERRORED %u RECORDS ",
                     aControlInfo->mSchema,
                     aControlInfo->mTable,
                     gZtdMaxErrorCount,
                     gZtdCompleteCount,
                     gZtdErrorCount );
    }
    else
    {
        stlSnprintf( aFileAndBuffer->mLogBuffer,
                     ZTD_MAX_LOG_STRING_SIZE,
                     "COMPLETED IN IMPORTING TABLE: %s.%s, TOTAL %u RECORDS, SUCCEEDED %u RECORDS, ERRORED %u RECORDS ",
                     aControlInfo->mSchema,
                     aControlInfo->mTable,
                     (gZtdCompleteCount + gZtdErrorCount),
                     gZtdCompleteCount,
                     gZtdErrorCount );
    }
    
    /**
     * semaphore를 해지한다.
     */
    sState = 5;
    STL_TRY( stlDestroySemaphore( &sWriteBadValueBlockListQueue->mItem,
                                  aErrorStack )
             == STL_SUCCESS );

    sState = 4;
    STL_TRY( stlDestroySemaphore( &sWriteBadValueBlockListQueue->mEmpty,
                                  aErrorStack )
             == STL_SUCCESS );

    sState = 3;
    for( i = 0; i < sThreadUnit; i++ )
    {
        STL_TRY( stlDestroySemaphore( &sExecuteValueBlockListQueue[i].mItem,
                                      aErrorStack )
                 == STL_SUCCESS );

        STL_TRY( stlDestroySemaphore( &sExecuteValueBlockListQueue[i].mEmpty,
                                      aErrorStack )
                 == STL_SUCCESS );
    }

    /**
     * close bad file.
     */
    sState = 2;
    STL_TRY( stlCloseFile( &gZtdBadFile,
                           aErrorStack )
             == STL_SUCCESS );
    
    /**
     * free dynamic memory
     */
    sState = 1;
    STL_TRY( stlDestroyDynamicAllocator( &sDynamicAllocator,
                                         aErrorStack )
             == STL_SUCCESS );
    
    /**
     * free region memory
     */
    sState = 0;
    STL_TRY( stlDestroyRegionAllocator( &sAllocator,
                                        aErrorStack )
             == STL_SUCCESS );

    return STL_SUCCESS;

    STL_CATCH( RAMP_ERR_WRITE_BAD_THREAD )
    {
        stlAppendErrors( aErrorStack, sJoinErrorStack );
    }

    STL_CATCH( RAMP_ERR_EXECUTE_THREAD )
    {
        stlAppendErrors( aErrorStack, sJoinErrorStack );

        /**
         * Join하지 않은 ztdExecuteValueBlockList 쓰레드를 Join한다.
         */
        for( i = i + 1; i < sThreadUnit; i++ )
        {
            (void) stlJoinThread( &sExecuteValueBlockThread[i],
                                  &sReturn,
                                  aErrorStack );
            
            if( sReturn == STL_FAILURE )
            {
                stlAppendErrors( aErrorStack, &sExecuteValueBlockListArg[i].mErrorStack );
            }
        }
    }

    STL_CATCH( RAMP_ERR_READ_THREAD )
    {
        stlAppendErrors( aErrorStack, sJoinErrorStack );
    }
    
    STL_FINISH;

    gZtdRunState = STL_FALSE;

    if( sIsAllocStmtHandle == STL_TRUE )
    {
        (void) ztdCheckError( aEnvHandle,
                              aDbcHandle,
                              sStmt,
                              0,
                              aFileAndBuffer,
                              NULL,
                              aErrorStack );

        (void) SQLFreeHandle( SQL_HANDLE_STMT,
                              sStmt );
    }
    
    switch( sState )
    {
        case 9:
            (void) stlJoinThread( sWriteBadValueBlockThread,
                                  &sReturn,
                                  aErrorStack );

            if( sReturn == STL_FAILURE )
            {
                stlAppendErrors( aErrorStack, &sWriteBadValueBlockListArg->mErrorStack );
            }
        case 8:
            for( i = 0; i < sThreadUnit; i++ )
            {
                (void) stlJoinThread( &sExecuteValueBlockThread[i],
                                      &sReturn,
                                      aErrorStack );
                if( sReturn == STL_FAILURE )
                {
                    stlAppendErrors( aErrorStack, &sExecuteValueBlockListArg[i].mErrorStack );
                }
            }
        case 7:
            (void) stlJoinThread( sReadValueBlockThread,
                                  &sReturn,
                                  aErrorStack );
            if( sReturn == STL_FAILURE )
            {
                stlAppendErrors( aErrorStack, &sReadValueBlockListArg->mErrorStack );
            }
        case 6:
            (void) stlDestroySemaphore( &sWriteBadValueBlockListQueue->mEmpty,
                                        aErrorStack );
        case 5:
            (void) stlDestroySemaphore( &sWriteBadValueBlockListQueue->mItem,
                                        aErrorStack );
        case 4:
            for( i = 0; i < sQueueIndex; i++ )
            {
                (void) stlDestroySemaphore( &sExecuteValueBlockListQueue[i].mItem,
                                            aErrorStack );

                (void) stlDestroySemaphore( &sExecuteValueBlockListQueue[i].mEmpty,
                                            aErrorStack );
            }
        case 3:
            (void) stlCloseFile( &gZtdBadFile,
                                 aErrorStack );
        case 2:
            (void) stlDestroyDynamicAllocator( &sDynamicAllocator,
                                               aErrorStack );
        case 1:
            (void) stlDestroyRegionAllocator( &sAllocator,
                                              aErrorStack );
        default :
            break;
    }

    return STL_FAILURE;
}

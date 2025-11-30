/*******************************************************************************
 * ztpwPrecomp.c
 *
 * Copyright (c) 2011, SUNJESOFT Inc.
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
#include <dtl.h>
#include <goldilocks.h>
#include <sqlext.h>
#include <ztpDef.h>
#include <ztpMisc.h>
#include <ztpSqlLex.h>
#include <ztpSqlParser.h>
#include <ztpdType.h>
#include <ztppMacro.h>
#include <ztpcHostVar.h>
#include <ztpuFile.h>
#include <ztpuString.h>
#include <ztpwPrecomp.h>

/**
 * @file ztpwPrecomp.c
 * @brief Gliese Embedded SQL in C precompiler write C code functions
 */

extern stlChar gErrMsgBuffer[STL_MAX_ERROR_MESSAGE];


/**
 * @addtogroup ztpwPrecomp
 * @{
 */

/************************************************************************
 * Write C code
 ************************************************************************/

stlStatus ztpwSendStringToOutFile(ztpCParseParam *aParam,
                                  stlChar        *aStr)
{
    if( aParam->mNeedPrint == STL_TRUE )
    {
        STL_TRY(ztpuSendStringToOutFile(aParam->mOutFile,
                                        aStr,
                                        aParam->mErrorStack)
                == STL_SUCCESS);
    }

    return STL_SUCCESS;

    STL_FINISH;

    aParam->mErrorStatus = STL_FAILURE;

    return STL_FAILURE;
}

stlStatus ztpwBypassCCode(ztpCParseParam *aParam)
{
    if( aParam->mNeedPrint == STL_TRUE )
    {
        while( aParam->mBuffer[aParam->mCCodeStartLoc] == '\0' )
        {
            if( aParam->mCCodeEndLoc <= aParam->mCCodeStartLoc )
            {
                break;
            }
            aParam->mCCodeStartLoc ++;
        }

        STL_TRY(ztpuWriteFile(aParam->mOutFile,
                              aParam->mBuffer + aParam->mCCodeStartLoc,
                              aParam->mCCodeEndLoc - aParam->mCCodeStartLoc,
                              aParam->mErrorStack)
                == STL_SUCCESS);
    }

    return STL_SUCCESS;

    STL_FINISH;

    aParam->mErrorStatus = STL_FAILURE;

    return STL_FAILURE;
}

stlStatus ztpwBypassCCodeByLoc(ztpCParseParam *aParam,
                               stlInt32        aStartLoc,
                               stlInt32        aEndLoc)
{
    stlChar   *sStartPos;

    if( aParam->mNeedPrint == STL_TRUE )
    {
        /* convert comment mark to other string */
        stlMemset(aParam->mTempBuffer, 0x00, aParam->mFileLength);
        stlMemcpy(aParam->mTempBuffer, aParam->mBuffer + aStartLoc, aEndLoc - aStartLoc);

        sStartPos = aParam->mTempBuffer;
        while((sStartPos = stlStrstr(sStartPos, "/*")) != NULL)
        {
            *(sStartPos + 1) = 'g';
        }

        sStartPos = aParam->mTempBuffer;
        while((sStartPos = stlStrstr(sStartPos, "*/")) != NULL)
        {
            *sStartPos = 'g';
        }

        STL_TRY(ztpuWriteFile(aParam->mOutFile,
                              aParam->mTempBuffer,
                              aEndLoc - aStartLoc,
                              aParam->mErrorStack)
                == STL_SUCCESS);
    }

    return STL_SUCCESS;

    STL_FINISH;

    aParam->mErrorStatus = STL_FAILURE;

    return STL_FAILURE;
}

stlStatus ztpwPrecompSourceLine(ztpCParseParam *aParam,
                                stlInt32        aLine,
                                stlChar        *aInFileName,
                                stlChar        *aOutFileName)
{
    stlChar   sLineBuffer[ZTP_LINE_BUF_SIZE];

    if( gNoLineInfo == STL_FALSE )
    {
        stlMemset(sLineBuffer, 0x00, ZTP_LINE_BUF_SIZE);

        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "#line %d \"%s\"\n", aLine, aInFileName);
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    }

    return STL_SUCCESS;

    STL_FINISH;

    aParam->mErrorStatus = STL_FAILURE;

    return STL_FAILURE;
}

stlStatus ztpwPrecompBlockBegin(ztpCParseParam *aParam)
{
    STL_TRY(ztpwSendStringToOutFile(aParam, "{\n") == STL_SUCCESS);

    return STL_SUCCESS;

    STL_FINISH;

    return STL_FAILURE;
}

stlStatus ztpwPrecompBlockEnd(ztpCParseParam *aParam)
{
    STL_TRY(ztpwSendStringToOutFile(aParam, "}\n") == STL_SUCCESS);

    return STL_SUCCESS;

    STL_FINISH;

    return STL_FAILURE;
}

stlStatus ztpwPrecompException(ztpCParseParam *aParam,
                               stlBool         aApplyNotFound)
{
    stlInt32  i;
    stlChar   sLineBuffer[ZTP_LINE_BUF_SIZE];

    stlMemset(sLineBuffer, 0x00, ZTP_LINE_BUF_SIZE);

    for(i = 0; i < ZTP_EXCEPTION_COND_MAX; i ++)
    {
        if( aApplyNotFound != STL_TRUE )
        {
            if( i == ZTP_EXCEPTION_COND_NOT_FOUND )
            {
                continue;
            }
        }

        sLineBuffer[0] = '\0';
        /* Exception handling code */
        STL_TRY(ztpMakeExceptionString(aParam, i, sLineBuffer) == STL_SUCCESS);
        if(stlStrlen(sLineBuffer) != 0)
        {
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
        }
    }

    return STL_SUCCESS;

    STL_FINISH;

    return STL_FAILURE;
}

stlStatus ztpwDeclareSqlargs(ztpCParseParam    * aParam )
{
    stlChar           sLineBuffer[ZTP_LINE_BUF_SIZE];
    stlInt32          sLineCount;

    stlMemset(sLineBuffer, 0x00, ZTP_LINE_BUF_SIZE);

    /**
     * Initialize SQL Args member
     */

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs_t sqlargs;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    if( gNoLineInfo == STL_FALSE )
    {
        STL_TRY(ztpwCalcFileLineCount(aParam, aParam->mOutFile, &sLineCount)
                == STL_SUCCESS);
        sLineCount += 2;

        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "#line %d \"%s\"\n", sLineCount, aParam->mOutFileName);
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    }

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sqltype    = 0;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sql_ca     = NULL;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.conn       = NULL;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sql_state  = NULL;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sqlstmt    = NULL;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sqlfn      = NULL;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sqlln      = 0;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sqlcursor  = NULL;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sqlfetch   = NULL;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.hvc        = 0;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sqlhv      = NULL;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.dynusing   = NULL;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.dyninto    = NULL;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    return STL_SUCCESS;

    STL_FINISH;

    return STL_FAILURE;
}

stlStatus ztpwSetSqlargs( ztpCParseParam    * aParam,
                          stlChar           * aSqlca,
                          stlChar           * aSqlState,
                          stlChar           * aSqlStmt,
                          zlplStatementType   aStatementType )
{
    stlChar  sLineBuffer[ZTP_LINE_BUF_SIZE];
    stlChar *sSqlBuffer = NULL;
    stlSize   sSqlStmtLen;

    stlMemset(sLineBuffer, 0x00, ZTP_LINE_BUF_SIZE);

    if( aParam->mConnStr != NULL)
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    sqlargs.conn = %s;\n", aParam->mConnStr );
    }
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sql_ca = %s;\n", aSqlca);
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sql_state = %s;\n", aSqlState);
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sqltype = %d;\n", aStatementType);
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sqlfn = (char *)__FILE__;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sqlln = __LINE__;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    if(aSqlStmt != NULL)
    {
        /*
         * string 내부에 " 가 존재할 수 있으므로, "는 \" 로 변환한다.
         * 모든 string의 내용이 " 문자일 경우를 가정하여,
         * 주어진 string의 길이 * 2 + 1(string 끝의 null 문자)
         * 만큼 memory를 할당한다.
         *
         * notice: 주어진 string이 매우 긴 sql 문이 될 수 있으므로,
         * 다른 문장들처럼 sLineBuffer를 구성한 후에 write 하지 않고,
         * string 요소를 따로따로 write 한다.
         * 1. sqlargs.sqlstmt = \"
         * 2. SQL 문장
         * 3. \";\n
         */
        sSqlStmtLen = stlStrlen( aSqlStmt ) * 2 + 1;

        STL_TRY( stlAllocHeap( (void**)&sSqlBuffer,
                               sSqlStmtLen,
                               aParam->mErrorStack )
                 == STL_SUCCESS );
        stlMemset(sSqlBuffer, 0x00, sSqlStmtLen);

        ztpuTrimSqlStmt(aSqlStmt, sSqlBuffer);

        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    sqlargs.sqlstmt = (char *)\"");
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

        STL_TRY(ztpwSendStringToOutFile(aParam, sSqlBuffer) == STL_SUCCESS);

        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "\";\n", sSqlBuffer);
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
        stlFreeHeap(sSqlBuffer);
    }
    else
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    sqlargs.sqlstmt = NULL;\n");
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    }

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.atomic = %d;\n", aParam->mIsAtomic );
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.unsafenull = %d;\n", gUnsafeNull );
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    if( aParam->mIterationValue != NULL)
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    sqlargs.iters = %s;\n", aParam->mIterationValue );
    }
    else
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    sqlargs.iters = 0;\n" );
    }
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    return STL_SUCCESS;

    STL_FINISH;

    return STL_FAILURE;
}


/**
 * @param Host Variable 을 위한 C code 를 선언한다.
 * @param[in] aParam            Parser Parameter
 * @param[in] aCHostArrayName   C Code 작성시 Host Variable 의 이름
 * @param[in] aHostVarList      Host Variable List
 * @remarks
 */
stlStatus ztpwDeclareHostVariables( ztpCParseParam  * aParam,
                                    stlChar         * aCHostArrayName,
                                    ztpCVariable    * aHostVarList )
{
    stlChar           sLineBuffer[ZTP_LINE_BUF_SIZE];

    stlInt32          sHostVarCount;
    ztpCVariable     *sHostVar;

    stlMemset(sLineBuffer, 0x00, ZTP_LINE_BUF_SIZE);

    /**
     * parameter validation
     */

    STL_DASSERT( aCHostArrayName != NULL );
    STL_DASSERT( aHostVarList != NULL );

    /**
     * Host 변수를 위한 C 변수 선언
     */

    /* Host 변수의 개수를 센다. */
    sHostVarCount = 0;
    for(sHostVar = aHostVarList;
        sHostVar != NULL;
        sHostVar = sHostVar->mNext)
    {
        sHostVarCount ++;
    }

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlhv_t %s[%d];\n",
                aCHostArrayName,
                sHostVarCount);
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sqlhv = %s;\n",
                aCHostArrayName );
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.hvc = %d;\n", sHostVarCount);
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    return STL_SUCCESS;

    STL_FINISH;

    return STL_FAILURE;
}



/**
 * @param Dynamic Parameter 을 위한 C code 를 선언한다.
 * @param[in] aParam            Parser Parameter
 * @param[in] aIsUsing          is USING dynamic parameter
 * @param[in] aCHostArrayName   C Code 작성시 Host Variable 의 이름
 * @param[in] aDynParam         Dynamic Parameter
 * @remarks
 */
stlStatus ztpwDeclareDynamicParameter( ztpCParseParam  * aParam,
                                       stlBool           aIsUsing,
                                       stlChar         * aCHostArrayName,
                                       ztpDynamicParam * aDynParam )
{
    stlChar           sLineBuffer[ZTP_LINE_BUF_SIZE];

    stlInt32          sHostVarCount;
    ztpCVariable     *sHostVar;

    stlMemset(sLineBuffer, 0x00, ZTP_LINE_BUF_SIZE);

    /**
     * parameter validation
     */

    STL_DASSERT( aCHostArrayName != NULL );
    STL_DASSERT( aDynParam != NULL );

    /**
     * Dynamic Parameter 를 위한 C 변수 선언
     */

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqldynhv_t hv_%s;\n",
                aCHostArrayName );    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    if ( aIsUsing == STL_TRUE )
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    sqlargs.dynusing = & hv_%s;\n",
                    aCHostArrayName );
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    }
    else
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    sqlargs.dyninto = & hv_%s;\n",
                    aCHostArrayName );
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    }

    /**
     * Descriptor 여부
     */

    if ( aDynParam->mIsDesc == STL_TRUE )
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    hv_%s.isdesc = 1;\n",
                    aCHostArrayName );
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    }
    else
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    hv_%s.isdesc = 0;\n",
                    aCHostArrayName );
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    }

    /**
     * Descriptor Name
     */

    if ( aDynParam->mDescName == NULL )
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    hv_%s.descname = NULL;\n",
                    aCHostArrayName );
    }
    else
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    hv_%s.descname = \"%s\";\n",
                    aCHostArrayName,
                    aDynParam->mDescName );
    }

    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    /**
     * Host 변수를 위한 C 변수 선언
     */

    /* Host 변수의 개수를 센다. */
    sHostVarCount = 0;
    for(sHostVar = aDynParam->mHostVar;
        sHostVar != NULL;
        sHostVar = sHostVar->mNext)
    {
        sHostVarCount ++;
    }

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    hv_%s.hvc = %d;\n",
                aCHostArrayName,
                sHostVarCount );
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);


    if ( sHostVarCount == 0 )
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    hv_%s.dynhv = NULL;\n",
                    aCHostArrayName );
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    }
    else
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    sqlhv_t %s[%d];\n",
                    aCHostArrayName,
                    sHostVarCount);
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    hv_%s.dynhv = %s;\n",
                    aCHostArrayName,
                    aCHostArrayName );
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    }

    return STL_SUCCESS;

    STL_FINISH;

    return STL_FAILURE;
}

/**
 * @param Host Variable 을 위한 C code 를 내용을 작성한다.
 * @param[in] aParam            Parser Parameter
 * @param[in] aCHostArrayName   C Code 작성시 Host Variable 의 이름
 * @param[in] aHostVarList      Host Variable List
 * @remarks
 */
stlStatus ztpwPrecompHostVariable(ztpCParseParam  * aParam,
                                  stlChar         * aCHostArrayName,
                                  ztpCVariable    * aHostVarList)
{
    stlChar          sLineBuffer[ZTP_LINE_BUF_SIZE];
    ztpCVariable    *sHostVar;
    stlInt32         i;
    stlBool          sIsArray       = STL_FALSE;
    stlBool          sUseAmpersand  = STL_FALSE;

    stlMemset(sLineBuffer, 0x00, ZTP_LINE_BUF_SIZE);

    /**
     * parameter validation
     */

    STL_DASSERT( aCHostArrayName != NULL );
    STL_DASSERT( aHostVarList != NULL );

    /**
     * 각 Host 변수에 대한 C Code 정보 설정
     */

    for(sHostVar = aHostVarList, i = 0;
        sHostVar != NULL;
        sHostVar = sHostVar->mNext, i ++)
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    %s[%d].io_type = %d;\n",
                    aCHostArrayName,
                    i,
                    sHostVar->mParamIOType);
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    %s[%d].data_type = %d;\n",
                    aCHostArrayName,
                    i,
                    sHostVar->mDataType);
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

        if( sHostVar->mArrayValue == NULL )
        {
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    %s[%d].arr_size = 0;\n",
                        aCHostArrayName,
                        i);
            sIsArray = STL_FALSE;
        }
        else
        {
            if( stlStrcmp( sHostVar->mArrayValue, "" ) == 0 )
            {
                stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                            "    %s[%d].arr_size = 0;\n",
                            aCHostArrayName,
                            i);
            }
            else
            {
                stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    %s[%d].arr_size = %s;\n",
                        aCHostArrayName,
                        i,
                        sHostVar->mArrayValue);
            }
            sIsArray = STL_TRUE;
        }
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

        if( sHostVar->mCharLength != NULL )
        {
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    %s[%d].buflen = %s;\n",
                        aCHostArrayName,
                        i,
                        sHostVar->mCharLength);
        }
        else
        {
            if( sHostVar->mDataType == ZLPL_C_DATATYPE_CHAR )
            {
                stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                            "    %s[%d].buflen = 0;\n",
                            aCHostArrayName,
                            i);
            }
            else
            {
                stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                            "    %s[%d].buflen = sizeof(%s);\n",
                            aCHostArrayName,
                            i,
                            sHostVar->mName);
            }
        }
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

        if( sHostVar->mPrecision == NULL )
        {
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    %s[%d].precision = %d;\n",
                        aCHostArrayName,
                        i,
                        ztpdGetPrecision(sHostVar->mDataType));
        }
        else
        {
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    %s[%d].precision = %s;\n",
                        aCHostArrayName,
                        i,
                        sHostVar->mPrecision);
        }
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

        if( sHostVar->mScale == NULL )
        {
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    %s[%d].scale = %d;\n",
                        aCHostArrayName,
                        i,
                        ztpdGetScale(sHostVar->mDataType));
        }
        else
        {
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    %s[%d].scale = %s;\n",
                        aCHostArrayName,
                        i,
                        sHostVar->mScale);
        }
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

        if( sIsArray == STL_TRUE )
        {
            if( sHostVar->mIsStructMember == STL_TRUE )
            {
                stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                            "    %s[%d].el_size = sizeof(%s[0]);\n",
                            aCHostArrayName,
                            i,
                            sHostVar->mParentStructName);
            }
            else
            {
                stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                            "    %s[%d].el_size = sizeof(%s[0]);\n",
                            aCHostArrayName,
                            i,
                            sHostVar->mName);
            }
        }
        else
        {
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    %s[%d].el_size = sizeof(%s);\n",
                        aCHostArrayName,
                        i,
                        sHostVar->mName);
        }

        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

        if( (sHostVar->mDataType == ZLPL_C_DATATYPE_CHAR)
            || (sHostVar->mDataType == ZLPL_C_DATATYPE_BINARY) )
        {
            sUseAmpersand = STL_FALSE;
        }
        else if( (sIsArray == STL_TRUE) && (sHostVar->mIsStructMember != STL_TRUE) )
        {
            sUseAmpersand = STL_FALSE;
        }
        else
        {
            sUseAmpersand = STL_TRUE;
        }

        if( sUseAmpersand == STL_TRUE )
        {
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    %s[%d].value = (void *)&%s;\n",
                        aCHostArrayName,
                        i,
                        sHostVar->mName);
        }
        else
        {
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    %s[%d].value = (void *)%s;\n",
                        aCHostArrayName,
                        i,
                        sHostVar->mName);
        }
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

        if(sHostVar->mIndicator != NULL)
        {
            STL_TRY_THROW(sHostVar->mIndicator->mDataType <= ZLPL_C_DATATYPE_ULONGLONG,
                          ERR_INVALID_INDICATOR_TYPE);

            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    %s[%d].ind_type = %d;\n",
                        aCHostArrayName,
                        i,
                        sHostVar->mIndicator->mDataType);
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

            if( sIsArray == STL_TRUE )
            {
                if( sHostVar->mIndicator->mIsStructMember == STL_TRUE )
                {
                    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                                "    %s[%d].ind_el_size = sizeof(%s[0]);\n",
                                aCHostArrayName,
                                i,
                                sHostVar->mIndicator->mParentStructName);
                }
                else
                {
                    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                                "    %s[%d].ind_el_size = sizeof(%s[0]);\n",
                                aCHostArrayName,
                                i,
                                sHostVar->mIndicator->mName);
                }
                STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
            }
            else
            {
                stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                            "    %s[%d].ind_el_size = sizeof(%s);\n",
                            aCHostArrayName,
                            i,
                            sHostVar->mIndicator->mName);
                STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
            }

            if( (sIsArray == STL_TRUE)
                && (sHostVar->mIndicator->mIsStructMember != STL_TRUE) )
            {
                sUseAmpersand = STL_FALSE;
            }
            else
            {
                sUseAmpersand = STL_TRUE;
            }

            if( sUseAmpersand == STL_TRUE )
            {
                stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                            "    %s[%d].indicator = (void *)&%s;\n",
                            aCHostArrayName,
                            i,
                            sHostVar->mIndicator->mName);
            }
            else
            {
                stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                            "    %s[%d].indicator = (void *)%s;\n",
                            aCHostArrayName,
                            i,
                            sHostVar->mIndicator->mName);
            }
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
        }
        else
        {
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    %s[%d].ind_type = -1;\n",
                        aCHostArrayName,
                        i);
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    %s[%d].ind_el_size = 0;\n",
                        aCHostArrayName,
                        i);
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    %s[%d].indicator = NULL;\n",
                        aCHostArrayName,
                        i);
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
        }
    }

    return STL_SUCCESS;

    STL_CATCH(ERR_INVALID_INDICATOR_TYPE)
    {
        stlPushError( STL_ERROR_LEVEL_ABORT,
                      ZTP_ERRCODE_INVALID_INDICATOR_TYPE,
                      NULL,
                      aParam->mErrorStack,
                      sHostVar->mIndicator->mName );
    }

    STL_FINISH;

    return STL_FAILURE;
}


stlStatus ztpwSetOpenCursor( ztpCParseParam          * aParam,
                             stlChar                 * aCursorName,
                             zlplCursorOriginType      aOriginType,
                             stlChar                 * aStmtName,
                             zlplCursorStandardType    aStandardType,
                             zlplCursorSensitivity     aSensitivity,
                             zlplCursorScrollability   aScrollability,
                             zlplCursorHoldability     aHoldability,
                             zlplCursorUpdatability    aUpdatability,
                             zlplCursorReturnability   aReturnability )
{
    stlChar  sLineBuffer[ZTP_LINE_BUF_SIZE];
    stlInt32 sLineCount;

    stlMemset(sLineBuffer, 0x00, ZTP_LINE_BUF_SIZE);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlcurprops_t sqlcurprops;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlcursor_t   sqlcursor;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    if( gNoLineInfo == STL_FALSE )
    {
        STL_TRY(ztpwCalcFileLineCount(aParam, aParam->mOutFile, &sLineCount)
                == STL_SUCCESS);
        sLineCount += 2;

        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "#line %d \"%s\"\n", sLineCount, aParam->mOutFileName);
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    }

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlcursor.sqlcurprops = &sqlcurprops;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sqlcursor = &sqlcursor;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlcursor.sqlcname = \"%s\";\n",
                aCursorName );
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    if ( aStmtName == NULL )
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    sqlcursor.sqldynstmt = NULL;\n",
                    aCursorName );
    }
    else
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    sqlcursor.sqldynstmt = \"%s\";\n",
                    aStmtName );
    }
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlcurprops.org_type = %d;\n", aOriginType);
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlcurprops.cur_type = %d;\n", aStandardType);
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlcurprops.sensitivity = %d;\n", aSensitivity);
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlcurprops.scrollability = %d;\n", aScrollability);
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlcurprops.holdability = %d;\n", aHoldability);
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlcurprops.updatability = %d;\n", aUpdatability);
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlcurprops.returnability = %d;\n", aReturnability);
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    return STL_SUCCESS;

    STL_FINISH;

    return STL_FAILURE;
}

stlStatus ztpwSetFetchCursor( ztpCParseParam * aParam,
                              stlChar        * aCursorName,
                              ztpFetchOrient * aFetchOrient )
{
    stlChar     sLineBuffer[ZTP_LINE_BUF_SIZE];

    stlMemset(sLineBuffer, 0x00, ZTP_LINE_BUF_SIZE);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlfetch_t   sqlfetch;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlfetch.sqlfph = 0;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlfetch.sqlfpl = 0;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlfetch.offsethv = NULL;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sqlfetch = &sqlfetch;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlfetch.sqlcname = \"%s\";\n",
                aCursorName );
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    if(aFetchOrient == NULL)
    {
        stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                    "    sqlfetch.sqlfo = %d;\n", ZLPL_FETCH_NA);
        STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
    }
    else
    {
        if ( aFetchOrient->mFetchOffsetHost == NULL )
        {
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    sqlfetch.sqlfo = %d;\n", aFetchOrient->mFetchOrient);
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    sqlfetch.sqlfph = (%sL >> 32) & 0xFFFFFFFF;\n",
                        aFetchOrient->mFetchPos.mName);
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    sqlfetch.sqlfpl = %sL & 0xFFFFFFFF;\n",
                        aFetchOrient->mFetchPos.mName);
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    sqlfetch.offsethv = NULL;\n");
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
        }
        else
        {
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    sqlfetch.sqlfo = %d;\n", aFetchOrient->mFetchOrient);
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    sqlfetch.sqlfph = 0;\n" );
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    sqlfetch.sqlfpl = 0;\n" );
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    sqlhv_t %s[1];\n",
                        ZTP_C_HOSTVAR_ARRAY_NAME_FETCH_OFFSET );
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

            STL_TRY( ztpwPrecompHostVariable( aParam,
                                              ZTP_C_HOSTVAR_ARRAY_NAME_FETCH_OFFSET,
                                              aFetchOrient->mFetchOffsetHost )
                     == STL_SUCCESS);

            stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                        "    sqlfetch.offsethv = %s;\n",
                        ZTP_C_HOSTVAR_ARRAY_NAME_FETCH_OFFSET );
            STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);
        }
    }

    return STL_SUCCESS;

    STL_FINISH;

    return STL_FAILURE;
}

stlStatus ztpwSetCloseCursor( ztpCParseParam * aParam,
                              stlChar        * aCursorName )
{
    stlChar  sLineBuffer[ZTP_LINE_BUF_SIZE];

    STL_DASSERT( aCursorName != NULL );

    stlMemset(sLineBuffer, 0x00, ZTP_LINE_BUF_SIZE);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlcursor_t   sqlcursor;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlargs.sqlcursor = &sqlcursor;\n");
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlcursor.sqlcname = \"%s\";\n", aCursorName);
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    stlSnprintf(sLineBuffer, ZTP_LINE_BUF_SIZE,
                "    sqlcursor.sqlcurprops = NULL;\n", aCursorName);
    STL_TRY(ztpwSendStringToOutFile(aParam, sLineBuffer) == STL_SUCCESS);

    return STL_SUCCESS;

    STL_FINISH;

    return STL_FAILURE;
}

stlStatus ztpwSetPositionedCursor( ztpCParseParam * aParam,
                                   stlChar        * aCursorName )
{
    return ztpwSetCloseCursor( aParam, aCursorName );
}

stlStatus ztpwCalcFileLineCount(ztpCParseParam *aParam,
                                stlFile        *aFile,
                                stlInt32       *aLineCount)
{
    stlInt32     sLineCount = 0;
    stlOffset    sFileOffset = 0;
    stlChar      sLineBuffer[ZTP_LINE_BUF_SIZE];

    *aLineCount = 0;
    STL_TRY(stlSeekFile(aFile,
                        STL_FSEEK_SET,
                        &sFileOffset,
                        aParam->mErrorStack)
            == STL_SUCCESS);

    stlMemset(sLineBuffer, 0x00, ZTP_LINE_BUF_SIZE);

    while(stlGetStringFile(sLineBuffer,
                           ZTP_LINE_BUF_SIZE,
                           aFile,
                           aParam->mErrorStack)
          == STL_SUCCESS)
    {
        sLineCount ++;
    }

    STL_TRY(stlGetLastErrorCode(aParam->mErrorStack)
            == STL_ERRCODE_EOF);
    (void)stlPopError(aParam->mErrorStack);

    *aLineCount = sLineCount;

    return STL_SUCCESS;

    STL_FINISH;

    aParam->mErrorStatus = STL_FAILURE;

    return STL_FAILURE;
}

/** @} */

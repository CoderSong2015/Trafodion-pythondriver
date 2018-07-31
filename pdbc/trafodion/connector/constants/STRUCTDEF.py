odbc_Dcs_GetObjRefHdl_ASParamError_exn_ = 1
odbc_Dcs_GetObjRefHdl_ASTimeout_exn_ = 2
odbc_Dcs_GetObjRefHdl_ASNoSrvrHdl_exn_ = 3
odbc_Dcs_GetObjRefHdl_ASTryAgain_exn_ = 4
odbc_Dcs_GetObjRefHdl_ASNotAvailable_exn_ = 5
odbc_Dcs_GetObjRefHdl_DSNotAvailable_exn_ = 6
odbc_Dcs_GetObjRefHdl_PortNotAvailable_exn_ = 7
odbc_Dcs_GetObjRefHdl_InvalidUser_exn_ = 8
odbc_Dcs_GetObjRefHdl_LogonUserFailure_exn_ = 9
odbc_Dcs_GetObjRefHdl_TenantName_exn_ = 10

#
# out context
#
OUTCONTEXT_OPT1_ENFORCE_ISO88591 = 1  # (2^0)
OUTCONTEXT_OPT1_IGNORE_SQLCANCEL = 1073741824  # (2^30)
OUTCONTEXT_OPT1_EXTRA_OPTIONS = 2147483648  # (2^31)
OUTCONTEXT_OPT1_DOWNLOAD_CERTIFICATE = 536870912  # (2^29)

#
# InitializeDialogue
#
odbc_SQLSvc_InitializeDialogue_ParamError_exn_ = 1
odbc_SQLSvc_InitializeDialogue_InvalidConnection_exn_ = 2
odbc_SQLSvc_InitializeDialogue_SQLError_exn_ = 3
odbc_SQLSvc_InitializeDialogue_SQLInvalidHandle_exn_ = 4
odbc_SQLSvc_InitializeDialogue_SQLNeedData_exn_ = 5
odbc_SQLSvc_InitializeDialogue_InvalidUser_exn_ = 6

SQL_PASSWORD_EXPIRING = 8857
SQL_PASSWORD_GRACEPERIOD = 8837

#
# FETCH_REPLY
#
SQLTYPECODE_CHAR = 1
# NUMERIC * /
SQLTYPECODE_NUMERIC = 2
SQLTYPECODE_NUMERIC_UNSIGNED = -201
# DECIMAL * /
SQLTYPECODE_DECIMAL = 3
SQLTYPECODE_DECIMAL_UNSIGNED = -301
SQLTYPECODE_DECIMAL_LARGE = -302
SQLTYPECODE_DECIMAL_LARGE_UNSIGNED = -303
# INTEGER / INT * /
SQLTYPECODE_INTEGER = 4
SQLTYPECODE_INTEGER_UNSIGNED = -401
SQLTYPECODE_LARGEINT = -402
SQLTYPECODE_LARGEINT_UNSIGNED = -405
# SMALLINT
SQLTYPECODE_SMALLINT = 5
SQLTYPECODE_SMALLINT_UNSIGNED = -502
SQLTYPECODE_BPINT_UNSIGNED = -503

    # TINYINT */
SQLTYPECODE_TINYINT                = -403
SQLTYPECODE_TINYINT_UNSIGNED       = -404

# DOUBLE depending on precision
SQLTYPECODE_FLOAT = 6
SQLTYPECODE_REAL = 7
SQLTYPECODE_DOUBLE = 8

# DATE,TIME,TIMESTAMP */
SQLTYPECODE_DATETIME = 9

# TIMESTAMP */
SQLTYPECODE_INTERVAL = 10

# no ANSI value 11 */

# VARCHAR/CHARACTER VARYING */
SQLTYPECODE_VARCHAR = 12
# SQL/MP stype VARCHAR with length prefix:
SQLTYPECODE_VARCHAR_WITH_LENGTH = -601
SQLTYPECODE_BLOB = -602
SQLTYPECODE_CLOB = -603
# LONG VARCHAR/ODBC CHARACTER VARYING */
SQLTYPECODE_VARCHAR_LONG = -1 # ## NEGATIVE??? */

# no ANSI value 13 */

# BIT */
SQLTYPECODE_BIT = 14 # not supported */

# BIT VARYING */
SQLTYPECODE_BITVAR = 15 # not supported */

# NCHAR -- CHAR(n) CHARACTER SET s -- where s uses two bytes per char */
SQLTYPECODE_CHAR_DBLBYTE = 16
# NCHAR VARYING -- VARCHAR(n) CHARACTER SET s -- s uses 2 bytes per char */
SQLTYPECODE_VARCHAR_DBLBYTE = 17
# BOOLEAN TYPE */
SQLTYPECODE_BOOLEAN = -701

# Date/Time/TimeStamp related constants */
SQLDTCODE_DATE = 1
SQLDTCODE_TIME = 2
SQLDTCODE_TIMESTAMP = 3
SQLDTCODE_MPDATETIME = 4


#
# TerminateReply
#
odbc_SQLSvc_TerminateDialogue_ParamError_exn_ = 1
odbc_SQLSvc_TerminateDialogue_InvalidConnection_exn_ = 2
odbc_SQLSvc_TerminateDialogue_SQLError_exn_ = 3

#
# SetConnectionOption
#
odbc_SQLSvc_SetConnectionOption_ParamError_exn_ = 1
odbc_SQLSvc_SetConnectionOption_InvalidConnection_exn_ = 2
odbc_SQLSvc_SetConnectionOption_SQLError_exn_ = 3
odbc_SQLSvc_SetConnectionOption_SQLInvalidHandle_exn_ = 4

#
# EndTransactionReply
#
odbc_SQLSvc_EndTransaction_ParamError_exn_ = 1
odbc_SQLSvc_EndTransaction_InvalidConnection_exn_ = 2
odbc_SQLSvc_EndTransaction_SQLError_exn_ = 3
odbc_SQLSvc_EndTransaction_SQLInvalidHandle_exn_ = 4
odbc_SQLSvc_EndTransaction_TransactionError_exn_ = 5

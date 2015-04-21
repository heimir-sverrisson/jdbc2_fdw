/* ---------------------------------------------
 * jq.c
 *    Implementation of Low level JDBC based functions replacing the libpq-fe functions
 *
 * Heimir Sverrisson, 2015-04-13
 *
 * ---------------------------------------------
 */
#include "postgres.h"
#include "jdbc2_fdw.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "commands/defrem.h"
#include "libpq-fe.h"

#include "jni.h"

#define Str(arg) #arg
#define StrValue(arg) Str(arg)
#define STR_PKGLIBDIR StrValue(PKG_LIB_DIR)

/*
 * Local housekeeping functions and Java objects
 */

static JNIEnv *Jenv;
static JavaVM *jvm;
jobject java_call;
static bool InterruptFlag;   /* Used for checking for SIGINT interrupt */
/*
 * Describes the valid options for objects that use this wrapper.
 */
struct jdbcFdwOption
{
    const char  *optname;
    Oid     optcontext; /* Oid of catalog in which option may appear */
};

/* Structure holding options from the foreign server
   and user mapping definitions */
typedef struct JserverOptions{
    char *url;
    char *drivername;
    char *username;
    char *password;
    int querytimeout;
    char *jarfile;
    int maxheapsize;
} JserverOptions;

static JserverOptions opts;

/* Local function prototypes */
static int connectDBComplete(Jconn *conn);
static void JVMInit(const ForeignServer *server, const UserMapping *user);
static void jdbcGetServerOptions(JserverOptions *opts, const ForeignServer *f_server, const UserMapping *f_mapping);
static Jconn * createJDBCConnection(const ForeignServer *server, const UserMapping *user);
/*
 * Uses a String object's content to create an instance of C String
 */
static char* ConvertStringToCString(jobject);
/*
 * JVM destroy function
 */
static void DestroyJVM();
/*
 * SIGINT interrupt check and process function
 */
static void SIGINTInterruptCheckProcess();

/*
 * SIGINTInterruptCheckProcess
 *      Checks and processes if SIGINT interrupt occurs
 */
static void
SIGINTInterruptCheckProcess()
{

	if (InterruptFlag == true) {
		jclass JDBCUtilsClass;
		jmethodID id_cancel;
		jstring cancel_result = NULL;
		char *cancel_result_cstring = NULL;

		JDBCUtilsClass = (*Jenv)->FindClass(Jenv, "JDBCUtils");
		if (JDBCUtilsClass == NULL) {
			elog(ERROR, "JDBCUtilsClass is NULL");
		}
		id_cancel = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "cancel",
				"()Ljava/lang/String;");
		if (id_cancel == NULL) {
			elog(ERROR, "id_cancel is NULL");
		}
		cancel_result = (*Jenv)->CallObjectMethod(Jenv, java_call, id_cancel);
		if (cancel_result != NULL) {
			cancel_result_cstring = ConvertStringToCString(
					(jobject) cancel_result);
			elog(ERROR, "%s", cancel_result_cstring);
		}
		InterruptFlag = false;
		elog(ERROR, "Query has been cancelled");
		(*Jenv)->ReleaseStringUTFChars(Jenv, cancel_result,
				cancel_result_cstring);
		(*Jenv)->DeleteLocalRef(Jenv, cancel_result);
	}
}

/*
 * ConvertStringToCString
 *              Uses a String object passed as a jobject to the function to 
 *              create an instance of C String.
 */
static char*
ConvertStringToCString(jobject java_cstring) {
	jclass JavaString;
	char *StringPointer;

	SIGINTInterruptCheckProcess();

	JavaString = (*Jenv)->FindClass(Jenv, "java/lang/String");
	if (!((*Jenv)->IsInstanceOf(Jenv, java_cstring, JavaString))) {
		elog(ERROR, "Object not an instance of String class");
	}

	if (java_cstring != NULL) {
		StringPointer = (char*) (*Jenv)->GetStringUTFChars(Jenv,
				(jstring) java_cstring, 0);
	} else {
		StringPointer = NULL;
	}
	return (StringPointer);
}

/*
 * DestroyJVM
 *      Shuts down the JVM.
 */
static void
DestroyJVM()
{

    (*jvm)->DestroyJavaVM(jvm);
}

/*
 * JVMInit
 *      Create the JVM which will be used for calling the Java routines
 *          that use JDBC to connect and access the foreign database.
 *
 */
void
JVMInit(const ForeignServer *server, const UserMapping *user)
{
    static bool FunctionCallCheck = false;   /* This flag safeguards against multiple calls of JVMInit() */

    jint res = -5;/* Set to a negative value so we can see whether JVM has been correctly created or not */
    JavaVMInitArgs  vm_args;
    JavaVMOption    *options;
    char strpkglibdir[] = STR_PKGLIBDIR;
    char *classpath;
    char *maxheapsizeoption = NULL;
    opts.maxheapsize = 0;

    jdbcGetServerOptions(&opts, server, user); // Get the maxheapsize value (if set)

    SIGINTInterruptCheckProcess();

    if (FunctionCallCheck == false)
    {
        classpath = (char*)palloc(strlen(strpkglibdir) + 19);
        snprintf(classpath, strlen(strpkglibdir) + 19, "-Djava.class.path=%s", strpkglibdir);

        if (opts.maxheapsize != 0){   /* If the user has given a value for setting the max heap size of the JVM */
            options = (JavaVMOption*)palloc(sizeof(JavaVMOption)*2);
            maxheapsizeoption = (char*)palloc(sizeof(int) + 6);
            snprintf(maxheapsizeoption, sizeof(int) + 6, "-Xmx%dm", opts.maxheapsize);
            options[0].optionString = classpath;
            options[1].optionString = maxheapsizeoption;
            vm_args.nOptions = 2;
        } else {
            options = (JavaVMOption*)palloc(sizeof(JavaVMOption));
            options[0].optionString = classpath;
            vm_args.nOptions = 1;
        }
        vm_args.version = 0x00010002;
        vm_args.options = options;
        vm_args.ignoreUnrecognized = JNI_FALSE;

        /* Create the Java VM */
        res = JNI_CreateJavaVM(&jvm, (void**)&Jenv, &vm_args);
        if (res < 0) {
            ereport(ERROR,
                 (errmsg("Failed to create Java VM")
                 ));
        }
        ereport(DEBUG3, (errmsg("Successfully created a JVM with %d MB heapsize", opts.maxheapsize)));
        InterruptFlag = false;
        /* Register an on_proc_exit handler that shuts down the JVM.*/
        on_proc_exit(DestroyJVM, 0);
        FunctionCallCheck = true;
    }
}

/*
 * Create an actual JDBC connection to the foreign server.
 * Precondition: JVMInit() has been successfully called.
 * Returns:
 *      Jconn.status = CONNECTION_OK and a valid reference to a JDBCUtils class
 * Error return:
 *      Jconn.status = CONNECTION_BAD
 */
static Jconn *
createJDBCConnection(const ForeignServer *server, const UserMapping *user)
{
    jmethodID idCreate;
    jstring stringArray[6];
    jclass javaString;
    jobjectArray argArray;
    jstring connResult;
    jclass JDBCUtilsClass;
    char *querytimeout_string;
    char *cString = NULL;
    int i;
    int numParams = sizeof(stringArray)/sizeof(jstring); //Number of parameters to Java
    int intSize = 10; // The string size to allocate for an integer value

    // pfree() when connection is discarded in JQfinish()
    Jconn *conn = (Jconn *)palloc(sizeof(Jconn));
    conn->status = CONNECTION_BAD; // Be pessimistic
    conn->festate = (jdbcFdwExecutionState *) palloc(sizeof(jdbcFdwExecutionState));
    conn->festate->query = NULL;
    conn->festate->NumberOfRows = 0;
    conn->festate->NumberOfColumns = 0;
    JDBCUtilsClass = (*Jenv)->FindClass(Jenv, "JDBCUtils");
    if(JDBCUtilsClass == NULL){
        ereport(ERROR, (errmsg("Failed to find the JDBCUtils class!")));
    }
    idCreate = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "createConnection",
                                                "([Ljava/lang/String;)Ljava/lang/String;");
    if(idCreate == NULL){
        ereport(ERROR, (errmsg("Failed to find the JDBCUtils.createConnection method!")));
    }
    // Construct the array to pass our parameters
    // Query timeout is an int, we need a string
    querytimeout_string = (char *)palloc(intSize);
    snprintf(querytimeout_string, intSize, "%d", opts.querytimeout);
    stringArray[0] = (*Jenv)->NewStringUTF(Jenv, opts.drivername);
    stringArray[1] = (*Jenv)->NewStringUTF(Jenv, opts.url);
    stringArray[2] = (*Jenv)->NewStringUTF(Jenv, opts.username);
    stringArray[3] = (*Jenv)->NewStringUTF(Jenv, opts.password);
    stringArray[4] = (*Jenv)->NewStringUTF(Jenv, querytimeout_string);
    stringArray[5] = (*Jenv)->NewStringUTF(Jenv, opts.jarfile);
    // Set up the return value
    javaString = (*Jenv)->FindClass(Jenv, "java/lang/String");
    argArray = (*Jenv)->NewObjectArray(Jenv, numParams, javaString, stringArray[0]);
    if(argArray == NULL){
        ereport(ERROR, (errmsg("Failed to create argument array")));
    }
    for(i = 1; i < numParams; i++){
        (*Jenv)->SetObjectArrayElement(Jenv, argArray, i, stringArray[i]);
    }
    conn->utilsObject = (*Jenv)->AllocObject(Jenv, JDBCUtilsClass);
    if(conn->utilsObject == NULL){
        ereport(ERROR, (errmsg("Failed to create java call")));
    }
    connResult = NULL;
    connResult = (*Jenv)->CallObjectMethod(Jenv, conn->utilsObject, idCreate, argArray);
    if(connResult != NULL){  // Happy result is null
        cString = ConvertStringToCString((jobject)connResult);
        ereport(ERROR, (errmsg("%s", cString)));
    }
    // Return Java memory
    for(i = 0; i < numParams; i++){
        (*Jenv)->DeleteLocalRef(Jenv, stringArray[i]);
    }
    (*Jenv)->DeleteLocalRef(Jenv, argArray);
    (*Jenv)->ReleaseStringUTFChars(Jenv, connResult, cString);
    (*Jenv)->DeleteLocalRef(Jenv, connResult);
    ereport(DEBUG3, (errmsg("Created a JDBC connection: %s",opts.url)));
    conn->status = CONNECTION_OK;
    return conn;
}

/*
 * Fetch the options for a jdbc2_fdw foreign server and user mapping.
 */
static void
jdbcGetServerOptions(JserverOptions *opts, const ForeignServer *f_server, const UserMapping *f_mapping)
{
    List        *options;
    ListCell    *lc;

    /* Collect options from server and user mapping */
    options = NIL;
    options = list_concat(options, f_server->options);
    options = list_concat(options, f_mapping->options);

    /* Loop through the options, and get the values */
    foreach(lc, options)
    {
        DefElem *def = (DefElem *) lfirst(lc);

//      ereport(DEBUG3, (errmsg("Option %s",defGetString(def))));

        if (strcmp(def->defname, "drivername") == 0){
            opts->drivername = defGetString(def);
        }
        if (strcmp(def->defname, "username") == 0){
            opts->username = defGetString(def);
        }
        if (strcmp(def->defname, "querytimeout") == 0){
            opts->querytimeout = atoi(defGetString(def));
        }
        if (strcmp(def->defname, "jarfile") == 0){
            opts->jarfile = defGetString(def);
        }
        if (strcmp(def->defname, "maxheapsize") == 0){
            opts->maxheapsize = atoi(defGetString(def));
        }
        if (strcmp(def->defname, "password") == 0){
            opts->password = defGetString(def);
        }
        if (strcmp(def->defname, "url") == 0){
            opts->url = defGetString(def);
        }
    }
}

Jresult *
JQexec(Jconn *conn, const char *query)
{
	jmethodID idCreateStatement;
	jstring statement;
	jstring returnValue;
	jclass JDBCUtilsClass;
	jfieldID idNumberOfColumns;
	char *cString = NULL;
	Jresult *res;

	ereport(DEBUG3, (errmsg("JQexec(%p): %s", conn, query)));
    // Our object of the JDBCUtils class is on the connection
    if(conn->utilsObject == NULL){
        ereport(ERROR, (errmsg("utilsObject is not on connection! Has the connection not been created?")));
    }
	//TODO: Need to reclaim this memory
	res = (Jresult *)palloc(sizeof(Jresult));
	res->resultStatus = PGRES_FATAL_ERROR; // Be pessimistic

    JDBCUtilsClass = (*Jenv)->FindClass(Jenv, "JDBCUtils");
	if(JDBCUtilsClass == NULL){
        ereport(ERROR, (errmsg("JDBCUtils class could not be created")));
    }
    idCreateStatement = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "createStatement",
                                                "(Ljava/lang/String;)Ljava/lang/String;");
    if(idCreateStatement == NULL){
        ereport(ERROR, (errmsg("Failed to find the JDBCUtils.createStatement method!")));
    }
    // The query argument
    statement = (*Jenv)->NewStringUTF(Jenv, query);
    if(statement == NULL){
        ereport(ERROR, (errmsg("Failed to create query argument")));
    }
    returnValue = NULL;
    returnValue = (*Jenv)->CallObjectMethod(Jenv, conn->utilsObject, idCreateStatement, statement);
    if(returnValue != NULL){  // Happy return Value is null
        cString = ConvertStringToCString((jobject)returnValue);
        ereport(ERROR, (errmsg("%s", cString)));
    }
    // Set up the execution state
    idNumberOfColumns = (*Jenv)->GetFieldID(Jenv, JDBCUtilsClass, "numberOfColumns" , "I");
    if (idNumberOfColumns == NULL){
            ereport(ERROR, (errmsg("Cannot read the number of columns")));
    }
    conn->festate->NumberOfColumns = (*Jenv)->GetIntField(Jenv, conn->utilsObject, idNumberOfColumns);
    // Return Java memory
    (*Jenv)->DeleteLocalRef(Jenv, statement);
    (*Jenv)->ReleaseStringUTFChars(Jenv, returnValue, cString);
    (*Jenv)->DeleteLocalRef(Jenv, returnValue);
    res->resultStatus = PGRES_COMMAND_OK;
    return res;
}

/*
 * JQiterate:
 * 		Read the next row from the remote server
 */
TupleTableSlot *
JQiterate(Jconn *conn, ForeignScanState *node){
	jobject utilsObject;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	jclass JDBCUtilsClass;
	jmethodID idResultSet;
	jobjectArray rowArray;
	char **values;
	int numberOfColumns;
	int i;
	HeapTuple tuple;
	jstring tempString;

	numberOfColumns = conn->festate->NumberOfColumns;
	utilsObject = conn->utilsObject;
	if(utilsObject == NULL){
		ereport(ERROR, (errmsg("Cannot get the utilsObject from the connection")));
	}
	// Cleanup
	ExecClearTuple(slot);
	SIGINTInterruptCheckProcess();
	if((*Jenv)->PushLocalFrame(Jenv, (numberOfColumns + 10)) < 0){
		ereport(ERROR, (errmsg("Error pushing local java frame")));
	}
    JDBCUtilsClass = (*Jenv)->FindClass(Jenv, "JDBCUtils");
	if(JDBCUtilsClass == NULL){
        ereport(ERROR, (errmsg("JDBCUtils class could not be created")));
    }
    idResultSet = (*Jenv)->GetMethodID(Jenv, JDBCUtilsClass, "returnResultSet", "()[Ljava/lang/String;");
    if(idResultSet == NULL){
        ereport(ERROR, (errmsg("Failed to find the JDBCUtils.returnResultSet method!")));
    }
	// Allocate pointers to the row data
    values=(char **)palloc(numberOfColumns * sizeof(char *));
    rowArray = (*Jenv)->CallObjectMethod(Jenv, utilsObject, idResultSet);
    if(rowArray != NULL){
    	for(i=0; i < numberOfColumns; i++){
    		values[i] = ConvertStringToCString((jobject)(*Jenv)->GetObjectArrayElement(Jenv, rowArray, i));
    	}
    	tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att), values);
    	ExecStoreTuple(tuple, slot, InvalidBuffer, false);
    	++(conn->festate->NumberOfRows);
    	// Take out the garbage
    	for(i=0; i < numberOfColumns; i++){
    		tempString = (jstring)(*Jenv)->GetObjectArrayElement(Jenv, rowArray,i);
    		(*Jenv)->ReleaseStringUTFChars(Jenv, tempString, values[i]);
    		(*Jenv)->DeleteLocalRef(Jenv, tempString);
    	}
    	(*Jenv)->DeleteLocalRef(Jenv, rowArray);
    }
    (*Jenv)->PopLocalFrame(Jenv, NULL);
    return(slot);
}

Jresult *
JQexecPrepared(Jconn *conn, const char *stmtName, int nParams,
    const char *const *paramValues, const int *paramLengths,
    const int *paramFormats, int resultFormat)
{
	ereport(DEBUG3, (errmsg("In JQexecPrepared")));
    return 0;
}

Jresult *
JQexecParams(Jconn *conn, const char *command,
    int nParams, const Oid *paramTypes, const char *const *paramValues,
    const int *paramLengths, const int *paramFormats, int resultFormat)
{
	Jresult *res;

	ereport(DEBUG3, (errmsg("In JQexecParams: %s, %d", command, nParams)));
	res = JQexec(conn, command);
	if(res->resultStatus != PGRES_COMMAND_OK){
		ereport(ERROR, (errmsg("JQexec returns %d", res->resultStatus)));
		return res;
	}
    return res;
}

ExecStatusType 
JQresultStatus(const Jresult *res)
{
	ereport(DEBUG3, (errmsg("In JQresultStatus")));
    return res->resultStatus;
}

void
JQclear(Jresult *res)
{
	ereport(DEBUG3, (errmsg("In JQclear")));
    return;
}

int
JQntuples(const Jresult *res)
{
	ereport(DEBUG3, (errmsg("In JQntuples")));
    return 0;
}

char *
JQcmdTuples(Jresult *res)
{
	ereport(DEBUG3, (errmsg("In JQcmdTuples")));
    return 0;
}

char *
JQgetvalue(const Jresult *res, int tup_num, int field_num)
{
	ereport(DEBUG3, (errmsg("In JQgetvalue")));
    return 0;
}

Jresult *
JQprepare(Jconn *conn, const char *stmtName, const char *query,
    int nParams, const Oid *paramTypes)
{
	ereport(DEBUG3, (errmsg("In JQprepare")));
    return 0;
}

int 
JQnfields(const Jresult *res)
{
	ereport(DEBUG3, (errmsg("In JQnfields")));
    return 0;
}

int 
JQgetisnull(const Jresult *res, int tup_num, int field_num)
{
	ereport(DEBUG3, (errmsg("In JQgetisnull")));
    return 0;
}

Jconn *
JQconnectdbParams(const ForeignServer *server, const UserMapping *user, 
    const char *const *keywords, const char *const *values)
{
    Jconn *conn;
    int i = 0;
    while(keywords[i]){
        const char *pkey = keywords[i];
        const char *pvalue = values[i];
        if(pvalue == NULL && pvalue[0] == '\0'){
        	break;
        }
        i++;
    }
    /* Initialize the Java JVM (if it has not been done already) */
    JVMInit(server, user);
    conn = createJDBCConnection(server, user);
    if(JQstatus(conn) == CONNECTION_BAD){
        (void) connectDBComplete(conn);
    }
    return conn;
}

/*
 * Do any cleanup needed and close a database connection
 * Return 1 on success, 0 on failure
 */
static int
connectDBComplete(Jconn *conn)
{
	ereport(DEBUG3, (errmsg("In connectDBComplete")));
    return 0;
}

ConnStatusType 
JQstatus(const Jconn *conn)       
{
    if(!conn){
        return CONNECTION_BAD;
    }
    return conn->status;
}

char *
JQerrorMessage(const Jconn *conn)
{
	ereport(DEBUG3, (errmsg("In JQerrorMessage")));
    return "Unknown Error!";
}

int 
JQconnectionUsedPassword(const Jconn *conn)
{
	ereport(DEBUG3, (errmsg("In JQconnectionUsedPassword")));
    return 0;
}

void
JQfinish(Jconn *conn)
{
	ereport(DEBUG3, (errmsg("In JQfinish for conn=%p", conn)));
	pfree(conn);
	conn = NULL;
    return;
}

int
JQserverVersion(const Jconn *conn)
{
	ereport(DEBUG3, (errmsg("In JQserverVersion")));
    return 0;
}

char *
JQresultErrorField(const Jresult *res, int fieldcode)
{
	ereport(DEBUG3, (errmsg("In JQresultErrorField")));
    return 0;
}

PGTransactionStatusType
JQtransactionStatus(const Jconn *conn)
{
	ereport(DEBUG3, (errmsg("In JQtransactionStatus")));
    return PQTRANS_UNKNOWN;
}

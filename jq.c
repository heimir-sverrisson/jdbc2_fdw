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

#include "jni.h"

#define Str(arg) #arg
#define StrValue(arg) Str(arg)
#define STR_PKGLIBDIR StrValue(PKG_LIB_DIR)

/*
 * Local housekeeping functions and Java objects
 */

static JNIEnv *env;
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

/* Local function prototypes */
static int connectDBComplete(Jconn *conn);
static void JVMInit(const ForeignServer *server, const UserMapping *user);
static void jdbcGetServerOptions(JserverOptions *opts, const ForeignServer *f_server, const UserMapping *f_mapping);
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

    if (InterruptFlag == true)
    {   
        jclass      JDBCUtilsClass;
        jmethodID   id_cancel;  
        jstring     cancel_result = NULL;
        char        *cancel_result_cstring = NULL;

        JDBCUtilsClass = (*env)->FindClass(env, "JDBCUtils");
        if (JDBCUtilsClass == NULL) 
        {       
            elog(ERROR, "JDBCUtilsClass is NULL");
        }       

        id_cancel = (*env)->GetMethodID(env, JDBCUtilsClass, "Cancel", "()Ljava/lang/String;");
        if (id_cancel == NULL) 
        {       
            elog(ERROR, "id_cancel is NULL");
        }       

        cancel_result = (*env)->CallObjectMethod(env,java_call,id_cancel);
        if (cancel_result != NULL)
        {       
            cancel_result_cstring = ConvertStringToCString((jobject)cancel_result);
            elog(ERROR, "%s", cancel_result_cstring);
        }       

        InterruptFlag = false;
        elog(ERROR, "Query has been cancelled");

        (*env)->ReleaseStringUTFChars(env, cancel_result, cancel_result_cstring);
        (*env)->DeleteLocalRef(env, cancel_result);
    }   
}

/*
 * ConvertStringToCString
 *              Uses a String object passed as a jobject to the function to 
 *              create an instance of C String.
 */
static char*
ConvertStringToCString(jobject java_cstring)
{
        jclass  JavaString;
        char    *StringPointer;

        SIGINTInterruptCheckProcess();

        JavaString = (*env)->FindClass(env, "java/lang/String");
        if (!((*env)->IsInstanceOf(env, java_cstring, JavaString))) {
                elog(ERROR, "Object not an instance of String class");
        }

        if (java_cstring != NULL) {
                StringPointer = (char*)(*env)->GetStringUTFChars(env, (jstring)java_cstring, 0);
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

    jint res = -5;/* Initializing the value of res so that we can check it later to see whether JVM has been correctly created or not */
    JavaVMInitArgs  vm_args;
    JavaVMOption    *options;
    char strpkglibdir[] = STR_PKGLIBDIR;
    char *classpath;
    char *maxheapsizeoption = NULL;
    JserverOptions opts;
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
        res = JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args);
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
    return 0;
}

Jresult *
JQexecPrepared(Jconn *conn, const char *stmtName, int nParams,
    const char *const *paramValues, const int *paramLengths,
    const int *paramFormats, int resultFormat)
{
    return 0;
}

Jresult *
JQexecParams(Jconn *conn, const char *command,
    int nParams, const Oid *paramTypes, const char *const *paramValues,
    const int *paramLengths, const int *paramFormats, int resultFormat)
{
    return 0;
}

ExecStatusType 
JQresultStatus(const Jresult *res)
{
    return PGRES_COMMAND_OK;
}

void
JQclear(Jresult *res)
{
    return;
}

int
JQntuples(const Jresult *res)
{
    return 0;
}

char *
JQcmdTuples(Jresult *res)
{
    return 0;
}

char *
JQgetvalue(const Jresult *res, int tup_num, int field_num)
{
    return 0;
}

Jresult *
JQprepare(Jconn *conn, const char *stmtName, const char *query,
    int nParams, const Oid *paramTypes)
{
    return 0;
}

int 
JQnfields(const Jresult *res)
{
    return 0;
}

int 
JQgetisnull(const Jresult *res, int tup_num, int field_num)
{
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
        if(pvalue != NULL && pvalue[0] != '\0'){
            ereport(DEBUG3, (errmsg("(key, value) = (%s, %s)", pkey, pvalue)));
        } else {
            break; // No more values
        }
        i++;
    }
    /* Initialize the Java JVM (if it has not been done already) */
    JVMInit(server, user);
    conn = 0;
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
    return 0;
}

int 
JQconnectionUsedPassword(const Jconn *conn)
{
    return 0;
}

void
JQfinish(Jconn *conn)
{
    return;
}

int
JQserverVersion(const Jconn *conn)
{
    return 0;
}

char *
JQresultErrorField(const Jresult *res, int fieldcode)
{
    return 0;
}

PGTransactionStatusType
JQtransactionStatus(const Jconn *conn)
{
    return PQTRANS_UNKNOWN;
}

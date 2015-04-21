/* ---------------------------------------------
 * jq.h
 *    Low level JDBC based functions replacing the libpq-fe functions
 *
 * Heimir Sverrisson, 2015-04-13
 *
 * ---------------------------------------------
 */
#ifndef JQ_H
#define JQ_H

#include "libpq-fe.h"
#include "jni.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"

typedef struct jdbcFdwExecutionState
{
        char            *query;
        int             NumberOfRows;
        int             NumberOfColumns;
} jdbcFdwExecutionState;

/* JDBC connection, same role as PGconn */
typedef struct Jconn {
    jobject utilsObject;
    ConnStatusType status;
    jdbcFdwExecutionState *festate;
} Jconn;

/* Same thing for Jresult replacing PGresult */
typedef struct Jresult{
	ExecStatusType resultStatus;
} Jresult;
/*
 * Replacement for libpq-fe.h functions
 */
extern Jresult *JQexec(Jconn *conn, const char *query);
extern Jresult *JQexecPrepared(Jconn *conn, const char *stmtName, int nParams,
    const char *const *paramValues, const int *paramLengths,
    const int *paramFormats, int resultFormat);
extern Jresult *JQexecParams(Jconn *conn, const char *command,
    int nParams, const Oid *paramTypes, const char *const *paramValues,
    const int *paramLengths, const int *paramFormats, int resultFormat);
extern ExecStatusType JQresultStatus(const Jresult *res);
extern void JQclear(Jresult *res);       
extern int JQntuples(const Jresult *res);
extern char *JQcmdTuples(Jresult *res);       
extern char *JQgetvalue(const Jresult *res, int tup_num, int field_num);
extern Jresult* JQprepare(Jconn *conn, const char *stmtName, const char *query,
    int nParams, const Oid *paramTypes);
extern int JQnfields(const Jresult *res);
extern int JQgetisnull(const Jresult *res, int tup_num, int field_num);
extern Jconn* JQconnectdbParams(const ForeignServer *server, const UserMapping *user, const char *const *keywords,
    const char *const *values);
extern ConnStatusType JQstatus(const Jconn *conn);       
extern char *JQerrorMessage(const Jconn *conn);
extern int JQconnectionUsedPassword(const Jconn *conn);
extern void JQfinish(Jconn *conn);
extern int JQserverVersion(const Jconn *conn);
extern char* JQresultErrorField(const Jresult *res, int fieldcode);
extern PGTransactionStatusType JQtransactionStatus(const Jconn *conn);
extern TupleTableSlot *JQiterate(Jconn *conn, ForeignScanState *node);

#endif /* JQ_H */

from tornado import gen
from TCLIServiceTornado.ttypes import TFetchResultsReq, TGetResultSetMetadataReq, TTypeId, TExecuteStatementReq, \
    TFetchOrientation, TCloseOperationReq, TGetSchemasReq

from error import Pyhs2Exception

def get_type(typeDesc):
    for ttype in typeDesc.types:
        if ttype.primitiveEntry is not None:
            return TTypeId._VALUES_TO_NAMES[ttype.primitiveEntry.type]
        elif ttype.mapEntry is not None:
            return ttype.mapEntry
        elif ttype.unionEntry is not None:
            return ttype.unionEntry
        elif ttype.arrayEntry is not None:
            return ttype.arrayEntry
        elif ttype.structEntry is not None:
            return ttype.structEntry
        elif ttype.userDefinedTypeEntry is not None:
            return ttype.userDefinedTypeEntry

def get_value(colValue):
    if colValue.boolVal is not None:
      return colValue.boolVal.value
    elif colValue.byteVal is not None:
      return colValue.byteVal.value
    elif colValue.i16Val is not None:
      return colValue.i16Val.value
    elif colValue.i32Val is not None:
      return colValue.i32Val.value
    elif colValue.i64Val is not None:
      return colValue.i64Val.value
    elif colValue.doubleVal is not None:
      return colValue.doubleVal.value
    elif colValue.stringVal is not None:
      return colValue.stringVal.value

class TornadoCursor(object):
    session = None
    client = None
    operationHandle = None

    def __init__(self, _client, sessionHandle):
        self.session = sessionHandle
        self.client = _client

    @gen.engine
    def execute(self, hql, callback):
        query = TExecuteStatementReq(self.session, statement=hql, confOverlay={})
        res = yield gen.Task(self.client.ExecuteStatement, query)
        self.operationHandle = res.operationHandle
        if res.status.errorCode is not None:
            raise Pyhs2Exception(res.status.errorCode, res.status.errorMessage)
        callback()

    @gen.engine
    def fetch(self, callback):
        rows = []
        fetchReq = TFetchResultsReq(operationHandle=self.operationHandle,
                                    orientation=TFetchOrientation.FETCH_NEXT,
                                    maxRows=10000)
        rows = yield gen.Task(self._fetch, rows, fetchReq)
        callback(rows)

    @gen.engine
    def getSchema(self, callback):
        cols = None
        if self.operationHandle:
            req = TGetResultSetMetadataReq(self.operationHandle)
            res = yield gen.Task(self.client.GetResultSetMetadata, req)
            if res.schema is not None:
                cols = []
                metadata = yield gen.Task(self.client.GetResultSetMetadata, req)
                for c in metadata.schema.columns:
                    col = dict()
                    col['type'] = get_type(c.typeDesc)
                    col['columnName'] = c.columnName
                    col['comment'] = c.comment
                    cols.append(col)
        callback(cols)

    @gen.engine
    def getDatabases(self, callback):
        req = TGetSchemasReq(self.session)
        res = yield gen.Task(self.client.GetSchemas, req)
        self.operationHandle = res.operationHandle
        if res.status.errorCode is not None:
            raise Pyhs2Exception(res.status.errorCode, res.status.errorMessage)
        fetch_res = yield gen.Task(self.fetch)
        callback(fetch_res)

    @gen.engine
    def _fetch(self, rows, fetchReq, callback):
        while True:
            resultsRes = yield gen.Task(self.client.FetchResults, fetchReq)
            for row in resultsRes.results.rows:
                rowData = []
                for i, col in enumerate(row.colVals):
                    rowData.append(get_value(col))
                rows.append(rowData)
            if len(resultsRes.results.rows) == 0:
                break
        callback(rows)

    @gen.engine
    def close(self, callback):
        if self.operationHandle is not None:
            req = TCloseOperationReq(operationHandle=self.operationHandle)
            yield gen.Task(self.client.CloseOperation, req)
        callback()
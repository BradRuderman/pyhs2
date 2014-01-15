from TCLIService.ttypes import TOpenSessionReq, TGetTablesReq, TFetchResultsReq,\
  TStatusCode, TGetResultSetMetadataReq, TGetColumnsReq, TType, TTypeId, \
  TExecuteStatementReq, TGetOperationStatusReq, TFetchOrientation, TCloseOperationReq, \
  TCloseSessionReq, TGetSchemasReq, TGetLogReq, TCancelOperationReq, TGetCatalogsReq

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

class Cursor(object):
    session = None
    client = None
    operationHandle = None

    def __init__(self, _client, sessionHandle):
        self.session = sessionHandle
        self.client = _client

    def execute(self, hql):
        query = TExecuteStatementReq(self.session, statement=hql, confOverlay={})
        res = self.client.ExecuteStatement(query)
        self.operationHandle = res.operationHandle
        if res.status.errorCode is not None:
            raise Pyhs2Exception(res.status.errorCode, res.status.errorMessage)
        
    def fetch(self):
        rows = []
        fetchReq = TFetchResultsReq(operationHandle=self.operationHandle,
                                    orientation=TFetchOrientation.FETCH_NEXT,
                                    maxRows=10000)
        self._fetch(rows, fetchReq)
        return rows

    def getSchema(self):
        if self.operationHandle:
            req = TGetResultSetMetadataReq(self.operationHandle)
            res = self.client.GetResultSetMetadata(req)
            if res.schema is not None:
                cols = []
                for c in self.client.GetResultSetMetadata(req).schema.columns:
                    col = {}
                    col['type'] = get_type(c.typeDesc)
                    col['columnName'] = c.columnName
                    col['comment'] = c.comment
                    cols.append(col)
                return cols
        return None

    def getDatabases(self):
        req = TGetSchemasReq(self.session)
        res = self.client.GetSchemas(req)
        self.operationHandle = res.operationHandle
        if res.status.errorCode is not None:
            raise Pyhs2Exception(res.status.errorCode, res.status.errorMessage)
        return self.fetch()

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    def _fetch(self, rows, fetchReq):
        while True:
            resultsRes = self.client.FetchResults(fetchReq)
            for row in resultsRes.results.rows:
                rowData= []
                for i, col in enumerate(row.colVals):
                    rowData.append(get_value(col))
                rows.append(rowData)
            if len(resultsRes.results.rows) == 0:
                break
        return rows

    def close(self):
        if self.operationHandle is not None:
            req = TCloseOperationReq(operationHandle=self.operationHandle)
            self.client.CloseOperation(req) 

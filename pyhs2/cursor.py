from TCLIService.ttypes import TOpenSessionReq, TGetTablesReq, TFetchResultsReq,\
  TStatusCode, TGetResultSetMetadataReq, TGetColumnsReq, TType, TTypeId, \
  TExecuteStatementReq, TGetOperationStatusReq, TFetchOrientation, TCloseOperationReq, \
  TCloseSessionReq, TGetSchemasReq, TGetLogReq, TCancelOperationReq

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
    operationHandle =  None
    def __init__(self, _client, session):
        res = _client.OpenSession(TOpenSessionReq())
        self.session = res.sessionHandle
        self.client = _client

    def execute(self, hql):
        query = TExecuteStatementReq(self.session, statement=hql, confOverlay={})
        response = self.client.ExecuteStatement(query)
        self.operationHandle = response.operationHandle

    def fetch(self):
        rows = []
        fetchReq = TFetchResultsReq(operationHandle=self.operationHandle, orientation=TFetchOrientation.FETCH_NEXT, maxRows=100);
        self._fetch(rows, fetchReq)
        return rows

    def _fetch(self, rows, fetchReq):
        resultsRes = self.client.FetchResults(fetchReq)
        for row in resultsRes.results.rows:
            rowData= []
            for i, col in enumerate(row.colVals):
                rowData.append(get_value(col))
            rows.append(rowData)
        if len(resultsRes.results.rows) != 0:
            self._fetch(rows, fetchReq)
        else:
            return rows

    def close(self):
        req = TCloseOperationReq(operationHandle=self.operationHandle)
        self.client.CloseOperation(req) 

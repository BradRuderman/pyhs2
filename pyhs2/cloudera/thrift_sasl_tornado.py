#!/usr/bin/env python
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
""" SASL transports for Thrift. Updated for Tornado Thrift. """
import logging

from thrift.TTornado import TTornadoStreamTransport
from thrift.transport.TTransport import *
import struct
from tornado import gen

logger = logging.getLogger(__name__)


class TSaslClientTransportTornado(TTornadoStreamTransport):
    START = 1
    OK = 2
    BAD = 3
    ERROR = 4
    COMPLETE = 5

    def __init__(self, sasl_client_factory, mechanism, host, port):
        """
        @param sasl_client_factory: a callable that returns a new sasl.Client object
        @param mechanism: the SASL mechanism (e.g. "GSSAPI")
        @param host: ip address or host name
        @param port: port
        """
        TTornadoStreamTransport.__init__(self, host, port)
        self.sasl_client_factory = sasl_client_factory
        self.sasl = None
        self.mechanism = mechanism
        self.opened = False
        self.encode = None
        self.__wbuf = StringIO()

    @gen.engine
    def open(self, callback=None):
        yield gen.Task(TTornadoStreamTransport.open, self)

        if self.sasl is not None:
            raise TTransportException(type=TTransportException.NOT_OPEN, message="Already open!")
        self.sasl = self.sasl_client_factory

        ret, chosen_mech, initial_response = self.sasl.start(self.mechanism)
        if not ret:
            raise TTransportException(type=TTransportException.NOT_OPEN,
                                      message=("Could not start SASL: %s" % self.sasl.getError()))

        # Send initial response
        yield gen.Task(self._send_message, self.START, chosen_mech)
        yield gen.Task(self._send_message, self.OK, initial_response)

        # SASL negotiation loop
        while True:
            status, payload = yield gen.Task(self._recv_sasl_message)
            if status not in (self.OK, self.COMPLETE):
                raise TTransportException(type=TTransportException.NOT_OPEN,
                                          message=("Bad status: %d (%s)" % (status, payload)))
            if status == self.COMPLETE:
                break
            ret, response = self.sasl.step(payload)
            if not ret:
                raise TTransportException(type=TTransportException.NOT_OPEN,
                                          message=("Bad SASL result: %s" % (self.sasl.getError())))
            self._send_message(self.OK, response)
        if callback:
            callback()

    @gen.engine
    def _send_message(self, status, body, callback=None):
        header = struct.pack(">BI", status, len(body))
        yield gen.Task(self.stream.write, header + body)
        if callback:
            callback()

    @gen.engine
    def _recv_sasl_message(self, callback):
        header = yield gen.Task(self.stream.read_bytes, 5)
        status, length = struct.unpack(">BI", header)
        if length > 0:
            payload = yield gen.Task(self.stream.read_bytes, length)
        else:
            payload = ""
        callback((status, payload))

    def write(self, data):
        self.__wbuf.write(data)

    @gen.engine
    def flush(self, callback=None):
        buf = self.__wbuf.getvalue()
        # The first time we flush data, we send it to sasl.encode()
        # If the length doesn't change, then we must be using a QOP
        # of auth and we should no longer call sasl.encode(), otherwise
        # we encode every time.
        if self.encode is None:
            success, encoded = self.sasl.encode(buf)
            if not success:
                raise TTransportException(type=TTransportException.UNKNOWN,
                                          message=self.sasl.getError())
            if len(encoded) == len(buf):
                self.encode = False
                yield gen.Task(self._flush_plain, buf)
            else:
                self.encode = True
                yield gen.Task(self.stream.write, encoded)
        elif self.encode:
            yield gen.Task(self._flush_encoded, buf)
        else:
            yield gen.Task(self._flush_plain, buf)
        self.__wbuf = StringIO()
        if callback:
            callback()

    @gen.engine
    def _flush_encoded(self, buf, callback):
        # sasl.ecnode() does the encoding and adds the length header, so nothing
        # to do but call it and write the result.
        success, encoded = self.sasl.encode(buf)
        if not success:
            raise TTransportException(type=TTransportException.UNKNOWN,
                                      message=self.sasl.getError())
        yield gen.Task(self.stream.write, encoded)
        callback()

    @gen.engine
    def _flush_plain(self, buf, callback):
        # When we have QOP of auth, sasl.encode() will pass the input to the output
        # but won't put a length header, so we have to do that.

        # Note stolen from TFramedTransport:
        # N.B.: Doing this string concatenation is WAY cheaper than making
        # two separate calls to the underlying socket object. Socket writes in
        # Python turn out to be REALLY expensive, but it seems to do a pretty
        # good job of managing string buffer operations without excessive copies
        yield gen.Task(self.stream.write, struct.pack(">I", len(buf)) + buf)
        callback()

    @gen.engine
    def _readFrameFromStream(self, callback):
        frame_header = yield gen.Task(self.stream.read_bytes, 4)
        frame_length, = struct.unpack(">I", frame_header)
        if self.encode:
            body = yield gen.Task(self.stream.read_bytes, frame_length)
            encoded = frame_header + body
            success, frame = self.sasl.decode(encoded)
            if not success:
                raise TTransportException(type=TTransportException.UNKNOWN,
                                          message=self.sasl.getError())
        else:
            frame = yield gen.Task(self.stream.read_bytes, frame_length)
        callback(frame)

    def close(self):
        TTornadoStreamTransport.close(self)
        self.sasl = None

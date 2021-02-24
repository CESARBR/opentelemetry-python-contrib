# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
RethinkDB instrumentation supporting `rethinkdb`_, it can be enabled by
using ``RethinkDBInstrumentor``.

.. _rethinkdb: https://docs.python.org/3/library/rethinkdb.html

Usage
-----

.. code:: python

    from rethinkdb import r
    from opentelemetry.instrumentation.rethinkdb import RethinkDBInstrumentor

    RethinkDBInstrumentor().instrument()

    #
    # Sample code from https://pypi.org/project/rethinkdb/
    #

    connection = r.connect(db='test')

    r.table_create('marvel').run(connection)

    marvel_heroes = r.table('marvel')
    marvel_heroes.insert({
        'id': 1,
        'name': 'Iron Man',
        'first_appearance': 'Tales of Suspense #39'
    }).run(connection)

    for hero in marvel_heroes.run(connection):
        print(hero['name'])

API
---
"""

import wrapt
import typing
from opentelemetry import trace as trace_api
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.trace import SpanKind, get_tracer
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.rethinkdb.version import __version__

from rethinkdb import r, net
from rethinkdb.errors import ReqlCursorEmpty

class ConnectionInstanceProxy(wrapt.ObjectProxy):
    # pylint: disable=unused-argument
    def __init__(self, connection_instance, tracer_provider, *args, **kwargs):
        wrapt.ObjectProxy.__init__(self, connection_instance)
        self._tracer_provider = tracer_provider

    def get_tracer(self):
        return get_tracer(__name__,
                          instrumenting_library_version=__version__,
                          tracer_provider=self._tracer_provider,
    )

    def get_query_str(self, query):
        result = str(query.serialize(self.__wrapped__._parent._get_json_encoder(query)))
        ustr = ['\\x00','\\x01','\\x02','\\x03','\\x04','\\x05','\\x06','b\'','\'' ]
        for u in ustr:
            result = result.replace(u, '')
        return result

    def traced_execution(
        self,
        instance,
        run_method,
        query,
        noreply
    ):
        name =  'run_query'
        query_str = self.get_query_str(query)
        if len(query_str) < 4:
            return run_method(query, noreply)

        with self.get_tracer().start_as_current_span(
            name, kind=SpanKind.CLIENT
        ) as span:
            self._populate_span(span, query_str)
            try:
                result = run_method(query, noreply)
                return result
            except Exception as ex:  # pylint: disable=broad-except
                if span.is_recording():
                    span.set_status(Status(StatusCode.ERROR, str(ex)))
                raise ex

    def _populate_span(
        self,
        span,
        query_str
    ):
        if not span.is_recording():
            return
        span.set_attribute(
            "db.system", 'rethinkdb'
        )
        span.set_attribute(
            "db.query", query_str
        )

    def run_query(self, query, noreply):
        return self.traced_execution(self.__wrapped__, self.__wrapped__.run_query, query, noreply)


class ConnectionProxy(wrapt.ObjectProxy):
    # pylint: disable=unused-argument
    def __init__(self, connection, tracer_provider, *args, **kwargs):
        wrapt.ObjectProxy.__init__(self, connection)
        self._tracer_provider = tracer_provider
        connection._instance = ConnectionInstanceProxy(connection._instance, tracer_provider, *args, **kwargs)

    def reconnect(self, noreply_wait, timeout):
        conn = self.__wrapped__.reconnect(noreply_wait, timeout)
        if not isinstance(conn, wrapt.ObjectProxy):
            conn = ConnectionProxy(conn, self.tracer_provider)
        return conn


def wrap_connection(tracer_provider):
    _tracer_provider = tracer_provider

    def wrap_connect_(
        wrapped: typing.Callable[..., typing.Any],
        instance: typing.Any,
        args: typing.Tuple[typing.Any, typing.Any],
        kwargs: typing.Dict[typing.Any, typing.Any],
    ):
        """Add object proxy to table object."""
        conn = wrapped(*args, **kwargs)
        return ConnectionProxy(conn, tracer_provider, *args, **kwargs)

    wrapt.wrap_function_wrapper(r, "connect", wrap_connect_)

class RethinkDBInstrumentor(BaseInstrumentor):
    def _instrument(self, **kwargs):
        """Integrate with RethinkDB Python library.
        https://docs.python.org/3/library/rethinkdb.html
        """
        tracer_provider = kwargs.get("tracer_provider")

        wrap_connection(tracer_provider)

    def _uninstrument(self, **kwargs):
        """"Disable RethinkDB instrumentation"""
        unwrap(r, "table")

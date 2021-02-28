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

import logging
import wrapt
import typing
from opentelemetry import trace as trace_api
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.trace import SpanKind, get_tracer
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.rethinkdb.version import __version__

from rethinkdb import r
from rethinkdb.errors import ReqlCursorEmpty


logger = logging.getLogger(__name__)

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
        result = ''
        try:
            result = str(query.serialize(self.__wrapped__._parent._get_json_encoder(query)).decode('utf-8','ignore'))
            result = "{}".format(result.replace(result.partition('[')[0],'',1).strip())
        except Exception as ex:  # pylint: disable=broad-except
            result = "Could not get query: {} ".format(ex)
            logger.debug(result)
        return result

    def traced_execution_simple(
        self,
        instance,
        run_method,
        query,
        noreply
    ):
        name =  'run_query'
        query_str = self.get_query_str(query)
        if self.isContinueQuery(query_str):
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

    def traced_execution(
        self,
        instance,
        run_method,
        query,
        noreply
    ):
        def run_in_context(span, continue_span):
            try:
                result = run_method(query, noreply)
                if continue_span:
                    if not span.is_recording():
                        span.end()
                return result
            except Exception as ex:  # pylint: disable=broad-except
                logger.debug("runquery.traced_execution.exception: {}".format(ex))
                if span.is_recording():
                        span.set_status(Status(StatusCode.ERROR, str(ex)))
                raise ex

        name =  'run_query'
        query_str = self.get_query_str(query)
        current_span = trace_api.get_current_span()
        span = None
        continue_span = False
        if self.isContinueQuery(query_str):
            span = current_span
            if span and getattr(span, 'name', '') == name:
                logger.debug("continue span for last query")
                continue_span = True
                if span.is_recording():
                    span.set_attribute(
                        "db.x.continue", True
                    )
                return run_in_context(span, continue_span)
            else:
                logger.warning("runquery.traced_execution: current_span name is not run_query: ({})".format(span.name))
                return run_method(query, noreply)
        else:
            logger.debug("starting span for query: {}".format(query_str))
            if current_span and getattr(current_span, 'name', '') == name:
                if current_span.is_recording():
                    current_span.end()
            with self.get_tracer().start_as_current_span(
                name, kind=SpanKind.CLIENT
            ) as span:
                self._populate_span(span, query_str)
                return run_in_context(span, continue_span)

    def isContinueQuery(self, query):
        return query == "[2]"

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

    def reconnect(self, *args, **kwargs):
        conn = self.__wrapped__.reconnect(*args, **kwargs)
        if not isinstance(conn, wrapt.ObjectProxy):
            conn = ConnectionProxy(conn, self.tracer_provider, *args, **kwargs)
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
        unwrap(r, "connect")

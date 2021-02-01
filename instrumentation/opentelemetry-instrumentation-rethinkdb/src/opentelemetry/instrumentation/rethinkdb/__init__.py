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

from rethinkdb import r

class MethodExecutionContext:
    def __init__(self, name, trace_provider, *args, **kwargs):
        self._name = name
        self._tracer_provider = trace_provider
        self._args=args
        self._kwargs=kwargs
        self.clear_context()

    def add_context(self, name, *args, **kwargs):
        self._context = "{context}/{name}".format(context=self._context, name=name)
        key = "#{index:02d}.{name}".format(name=name,index=len(self._history))
        self._history[key] = self._get_arguments_list(*args,**kwargs)
        return self

    def get_context_path(self):
        return self._context

    def get_history(self):
        return self._history

    def clear_context(self):
        self._context = ''
        self._history = {}
        self.add_context(self._name, *self._args, **self._kwargs)

    def get_tracer(self):
        return get_tracer(
            __name__,
            instrumenting_library_version=__version__,
            tracer_provider=self._tracer_provider,
        )

    def _get_arguments_list(self, *args, **kwargs):
        result = ''
        for arg in args:
            result += "{},".format(arg)
        for key, value in kwargs.items():
            result += "({key}={value}),".format(key=key,value=value)
        return "({})".format(result)

class RqlQueryTracer:
    def __init__(self, method_context):
        self._method_context = method_context

    def traced_execution(
        self,
        instance,
        run_method: typing.Callable[..., typing.Any],
        *args: typing.Tuple[typing.Any, typing.Any],
        **kwargs: typing.Dict[typing.Any, typing.Any]
    ):
        name =  self._method_context.get_context_path()

        with self._method_context.get_tracer().start_as_current_span(
            name, kind=SpanKind.CLIENT
        ) as span:
            self._populate_span(span)
            try:
                result = run_method(*args, **kwargs)
                return result
            except Exception as ex:  # pylint: disable=broad-except
                if span.is_recording():
                    span.set_status(Status(StatusCode.ERROR, str(ex)))
                raise ex

    def _populate_span(
        self,
        span: trace_api.Span
    ):
        if not span.is_recording():
            return
        span.set_attribute(
            "db.system", 'rethinkdb'
        )
        for key, value in self._method_context.get_history().items():
            span.set_attribute("db.method.{}".format(key), value)

        self._method_context.clear_context()

        #span.set_attribute("db.name", self._db_api_integration.database)


# pylint: disable=abstract-method
class RqlQueryProxy(wrapt.ObjectProxy):
    # pylint: disable=unused-argument
    def __init__(self, rql_query, method_context, *args, **kwargs):
        wrapt.ObjectProxy.__init__(self, rql_query)
        self._method_context = method_context

    def __execute_method__(self, wrapped_method, *args, **kwargs):
        result = wrapped_method(*args, **kwargs)
        return RqlQueryProxy(result, self._method_context.add_context(wrapped_method.__name__, *args, **kwargs), *args, **kwargs)

    def insert(self, *args, **kwargs):
        print(args)
        print(kwargs)
        return self.__execute_method__(self.__wrapped__.insert, *args, **kwargs)

    def update(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.update, *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.delete, *args, **kwargs)

    def pluck(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.pluck, *args, **kwargs)

    def default(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.default, *args, **kwargs)

    def merge(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.merge, *args, **kwargs)

    def coerce_to(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.coerce_to, *args, **kwargs)

    def get(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.get, *args, **kwargs)

    def filter(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.filter, *args, **kwargs)

    def run(self, *args, **kwargs):
        return RqlQueryTracer(self._method_context.add_context(self.__wrapped__.run.__name__, *args, **kwargs)).traced_execution(
                self.__wrapped__, self.__wrapped__.run, *args, **kwargs
            )

    def __enter__(self):
        self.__wrapped__.__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self.__wrapped__.__exit__(*args, **kwargs)


def wrap_table(tracer_provider):
    _tracer_provider = tracer_provider

    def wrap_table_(
        wrapped: typing.Callable[..., typing.Any],
        instance: typing.Any,
        args: typing.Tuple[typing.Any, typing.Any],
        kwargs: typing.Dict[typing.Any, typing.Any],
    ):
        """Add object proxy to table object."""
        table = wrapped(*args, **kwargs)
        method_context = MethodExecutionContext('table', _tracer_provider, *args, **kwargs)
        return RqlQueryProxy(table, method_context, *args, **kwargs)

    wrapt.wrap_function_wrapper(r, "table", wrap_table_)


class RethinkDBInstrumentor(BaseInstrumentor):
    def _instrument(self, **kwargs):
        """Integrate with RethinkDB Python library.
        https://docs.python.org/3/library/rethinkdb.html
        """
        tracer_provider = kwargs.get("tracer_provider")

        wrap_table(tracer_provider)

    def _uninstrument(self, **kwargs):
        """"Disable RethinkDB instrumentation"""
        unwrap(r, "table")

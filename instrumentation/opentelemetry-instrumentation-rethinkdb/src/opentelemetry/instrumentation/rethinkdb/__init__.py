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
        self._running_span = None

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

    def _start_new_span(self):
        if self._running_span is None:
            name = self._method_context.get_context_path()
            self._running_span = self._method_context.get_tracer().start_span(name, kind=SpanKind.CLIENT)
        else:
            raise Exception("You need to finish the trace before starting a new one")

    def _stop_running_span(self):
        if self._running_span is None:
            return
        self._populate_span(self._running_span)
        self._running_span.end()
        self._running_span = None

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

    # others methods
    def eq(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.eq,  *args, **kwargs)

    def ne(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.ne,  *args, **kwargs)

    def lt(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.lt,  *args, **kwargs)

    def le(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.le,  *args, **kwargs)

    def gt(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.gt,  *args, **kwargs)

    def ge(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.ge,  *args, **kwargs)

    def add(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.add,  *args, **kwargs)

    def sub(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.sub,  *args, **kwargs)

    def mul(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.mul,  *args, **kwargs)

    def div(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.div,  *args, **kwargs)

    def mod(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.mod,  *args, **kwargs)

    def bit_and(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.bit_and,  *args, **kwargs)

    def bit_or(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.bit_or,  *args, **kwargs)

    def bit_xor(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.bit_xor,  *args, **kwargs)

    def bit_not(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.bit_not,  *args, **kwargs)

    def bit_sal(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.bit_sal,  *args, **kwargs)

    def bit_sar(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.bit_sar,  *args, **kwargs)

    def floor(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.floor,  *args, **kwargs)

    def ceil(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.ceil,  *args, **kwargs)

    def round(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.round,  *args, **kwargs)

    def and_(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.and_,  *args, **kwargs)

    def or_(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.or_,  *args, **kwargs)

    def not_(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.not_,  *args, **kwargs)

    def contains(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.contains,  *args, **kwargs)

    def has_fields(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.has_fields,  *args, **kwargs)

    def with_fields(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.with_fields,  *args, **kwargs)

    def keys(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.keys,  *args, **kwargs)

    def values(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.values,  *args, **kwargs)

    def changes(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.changes, *args, **kwargs)

    def without(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.without,  *args, **kwargs)

    def do(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.do,  *args, **kwargs)

    def replace(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.replace, *args, **kwargs)

    # Rql type inspection
    def ungroup(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.ungroup, *args, **kwargs)

    def type_of(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.type_of, *args, **kwargs)

    def append(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.append, *args, **kwargs)

    def prepend(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.prepend, *args, **kwargs)

    def difference(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.difference, *args, **kwargs)

    def set_insert(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.set_insert, *args, **kwargs)

    def set_union(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.set_union, *args, **kwargs)

    def set_intersection(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.set_intersection, *args, **kwargs)

    def set_difference(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.set_difference, *args, **kwargs)

    def get_field(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.get_field, *args, **kwargs)

    def nth(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.nth, *args, **kwargs)

    def to_json(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.to_json, *args, **kwargs)

    def to_json_string(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.to_json_string, *args, **kwargs)

    def match(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.match, *args, **kwargs)

    def split(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.split, *args, **kwargs)

    def upcase(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.filter, *args, **kwargs)

    def downcase(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.upcase, *args, **kwargs)

    def is_empty(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.is_empty, *args, **kwargs)

    def offsets_of(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.offsets_of, *args, **kwargs)

    def slice(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.slice, *args, **kwargs)

    def skip(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.skip, *args, **kwargs)

    def limit(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.limit, *args, **kwargs)

    def reduce(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.reduce, *args, **kwargs)

    def sum(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.sum, *args, **kwargs)

    def avg(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.avg, *args, **kwargs)

    def min(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.min, *args, **kwargs)

    def max(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.max, *args, **kwargs)

    def map(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.map, *args, **kwargs)

    def fold(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.fold, *args, **kwargs)

    def concat_map(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.concat_map, *args, **kwargs)

    def order_by(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.order_by, *args, **kwargs)

    def between(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.between, *args, **kwargs)

    def distinct(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.distinct, *args, **kwargs)

    def count(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.count, *args, **kwargs)

    def union(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.union, *args, **kwargs)

    def inner_join(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.inner_join, *args, **kwargs)

    def outer_join(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.outer_join, *args, **kwargs)

    def eq_join(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.eq_join, *args, **kwargs)

    def zip(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.zip, *args, **kwargs)

    def group(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.group, *args, **kwargs)

    def branch(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.branch, *args, **kwargs)

    def for_each(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.for_each, *args, **kwargs)

    def info(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.info, *args, **kwargs)

    def insert_at(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.insert_at, *args, **kwargs)

    def splice_at(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.splice_at, *args, **kwargs)

    def delete_at(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.delete_at, *args, **kwargs)

    def change_at(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.change_at, *args, **kwargs)

    def sample(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.sample, *args, **kwargs)

    def to_iso8601(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.to_iso8601, *args, **kwargs)

    def to_epoch_time(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.to_epoch_time, *args, **kwargs)

    def during(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.during, *args, **kwargs)

    def date(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.date, *args, **kwargs)

    def time_of_day(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.time_of_day, *args, **kwargs)

    def timezone(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.timezone, *args, **kwargs)

    def year(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.year, *args, **kwargs)

    def month(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.month, *args, **kwargs)

    def day(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.day, *args, **kwargs)

    def day_of_week(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.day_of_week, *args, **kwargs)

    def day_of_year(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.day_of_year, *args, **kwargs)

    def hours(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.hours, *args, **kwargs)

    def minutes(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.minutes, *args, **kwargs)

    def seconds(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.seconds, *args, **kwargs)

    def in_timezone(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.in_timezone, *args, **kwargs)

    def to_geojson(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.to_geojson, *args, **kwargs)

    def distance(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.distance, *args, **kwargs)

    def intersects(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.intersects, *args, **kwargs)

    def includes(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.includes, *args, **kwargs)

    def fill(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.fill, *args, **kwargs)

    def polygon_sub(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.polygon_sub, *args, **kwargs)

    #def compose(self, args, optargs):
    def compose(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.compose,  *args, **kwargs)

    def get_all(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.get_all, *args, **kwargs)

    def set_write_hook(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.set_write_hook, *args, **kwargs)

    def get_write_hook(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.get_write_hook, *args, **kwargs)

    def index_create(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.index_create, *args, **kwargs)

    def index_drop(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.index_drop, *args, **kwargs)

    def index_rename(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.index_rename, *args, **kwargs)

    def index_list(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.index_list, *args, **kwargs)

    def index_status(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.index_status, *args, **kwargs)

    def index_wait(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.index_wait, *args, **kwargs)

    def status(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.status, *args, **kwargs)

    def config(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.config, *args, **kwargs)

    def wait(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.wait, *args, **kwargs)

    def reconfigure(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.reconfigure, *args, **kwargs)

    def rebalance(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.rebalance, *args, **kwargs)

    def sync(self,  *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.sync, *args, **kwargs)

    def grant(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.grant, *args, **kwargs)

    def get_intersecting(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.get_intersecting, *args, **kwargs)

    def get_nearest(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.get_nearest, *args, **kwargs)

    def uuid(self, *args, **kwargs):
        return self.__execute_method__(self.__wrapped__.uuid, *args, **kwargs)

    # run
    def run(self, *args, **kwargs):
        return RqlQueryTracer(self._method_context.add_context(self.__wrapped__.run.__name__, *args, **kwargs)).traced_execution(
                self.__wrapped__, self.__wrapped__.run, *args, **kwargs
            )

    def __enter__(self):
        self.__wrapped__.__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self.__wrapped__.__exit__(*args, **kwargs)

class DefaultCursorProxy(wrapt.ObjectProxy):
    # pylint: disable=unused-argument
    def __init__(self, cursor, method_context, *args, **kwargs):
        wrapt.ObjectProxy.__init__(self, cursor)
        self._method_context = method_context
        self.tracer = RqlQueryTracer(self._method_context.add_context("cursor", *args, **kwargs))

    def __iter__(self):
        self.tracer._start_new_span()
        return self

    def __next__(self):
        try:
            return self.__wrapped__.__next__()
        except ReqlCursorEmpty as empty:
            self.tracer._stop_running_span()
            raise empty
        except Exception as e:
            self.tracer._stop_running_span()
            raise e


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

    def wrap_cursor_(
        wrapped: typing.Callable[..., typing.Any],
        instance: typing.Any,
        args: typing.Tuple[typing.Any, typing.Any],
        kwargs: typing.Dict[typing.Any, typing.Any],
    ):
        """Add object proxy to cursor object."""
        cursor = wrapped(*args, **kwargs)
        method_context = MethodExecutionContext('cursor', _tracer_provider, *args, **kwargs)
        return DefaultCursorProxy(cursor, method_context, *args, **kwargs)

    wrapt.wrap_function_wrapper(net, "DefaultCursor", wrap_cursor_)
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

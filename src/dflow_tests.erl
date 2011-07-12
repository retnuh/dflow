-module(dflow_tests).
-behaviour(dataflow).
%% dataflow fns
-export([table_for_stage/1, is_stage_transient/1, functions_for_stage/1, next_stage/2]).
%% exported fns 
-export([prepend/2, flaky/1]).

-include_lib("eunit/include/eunit.hrl").
-include("dflow.hrl").

%%% dataflow API

table_for_stage(_) ->
    dflow_tests.

is_stage_transient(foo) ->
    true;
is_stage_transient(_) ->
    false.

functions_for_stage(flaky) ->
    [{?MODULE, flaky, []}];
functions_for_stage(foo) ->                     % Demonstrate that a stage can have multiple functions
    [{?MODULE, prepend, ["foo1"]}, {?MODULE, prepend, ["foo2"]}];
functions_for_stage(bar) ->
    [{?MODULE, prepend, ["bar"]}];
functions_for_stage(baz) ->
    [{?MODULE, prepend, ["baz"]}];
functions_for_stage(identity) ->
    identity;
functions_for_stage(none) ->
    % This is a bit of a hack for the test
    ?MODULE ! "item1",
    none.

%% Note that I don't need a next_stage for "none"
next_stage(identity, Result) ->
    [{none, Result}];
next_stage(flaky, Result) ->
    [{foo, Result}];
next_stage(foo, Result) ->
    [{bar, Result}];
next_stage(bar, Result) ->
    [{baz, Result}, {baz, "extra-" ++ Result}]; % results can produce multiple new functions
next_stage(baz, Result) ->
    ?MODULE ! Result,
    [].

%%% Functions that actually "do stuff"

prepend(Data, Prefix) ->
    Prefix ++ "-" ++ Data.

flaky(Data) ->
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    case random:uniform(2) of
        1 -> timer:sleep(100), error("Faux failure");
        2 -> Data
    end.

%%% Test related functions

match_result(N, "item" ++  [X]) when X >= $1, X=<$9 ->
    N-1;
match_result(N, "baz-" ++ Rest) ->
    match_result(N, Rest);
match_result(N, "extra-" ++ Rest) ->
    match_result(N, Rest);
match_result(N, "bar-" ++ Rest) ->
    match_result(N, Rest);
match_result(N, "foo" ++ [X , $- | Rest]) when X >= $1, X=<$2 ->
    match_result(N, Rest).

register_if_necessary() ->
    register_if_necessary(whereis(?MODULE), self()).
register_if_necessary(undefined, Self) ->
    register(?MODULE, Self);
register_if_necessary(X, X) ->
    ok;
register_if_necessary(_Other, Self) ->
    unregister(?MODULE),
    register(?MODULE, Self).

setUp() ->
    application:start(mnesia),
    application:start(dflow),
    dflow:register([dflow_tests]),
    ok.

tearDown(_) ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) -> unregister(?MODULE);
        undefined -> ok
    end,
    %% mnesia:stop(),
    %% application:stop(dflow),
    ok.

test_receive(0) ->
    timer:sleep(100), % Ick, not crazy about this!
    ok;
test_receive(N) ->
    receive
        Val -> X = match_result(N, Val), test_receive(X)
    after 2000 ->
            error("Timeout")
    end.

basic_test_() ->
    {setup, fun setUp/0, fun tearDown/1,
     [
      { "Basic",
        fun() ->
                register_if_necessary(),
                dflow:add_datum({foo, ?MODULE}, "item1"),
                dflow:add_data({foo, ?MODULE}, ["item2", "item3"]),
                test_receive(12)
        end },
      { "Flaky",
        fun() ->
                register_if_necessary(),
                dflow:add_datum({flaky, ?MODULE}, "item4"),
                test_receive(4)
        end },
      { "Persistence",
        fun() ->
                register_if_necessary(),
                mnesia:clear_table(dflow_tests),
                dflow:add_datum({bar, ?MODULE}, "item5"),
                test_receive(2),
                ?assertEqual([ "item5" ], dfq:completed_data({bar, ?MODULE})),
                Bazes = dfq:completed_data({baz, ?MODULE}),
                Bazes,
                ?assert(lists:member("bar-item5", Bazes)),
                ?assert(lists:member("extra-bar-item5", Bazes))
        end },
      { "Dups are Discarded",
        fun() ->
                register_if_necessary(),
                mnesia:clear_table(dflow_tests),
                dflow:add_datum({bar, ?MODULE}, "item6"),
                test_receive(2),
                [Bar1] = dfq:completed({bar, ?MODULE}),
                dflow:add_datum({bar, ?MODULE}, "item6"),
                ?assertEqual({message_queue_len, 0}, process_info(self(), message_queue_len)),
                [Bar2] = dfq:completed({bar, ?MODULE}),
                Bazes = dfq:completed({baz, ?MODULE}),
                ?assertEqual(Bar1, Bar2),
                ?assert(lists:all(fun(Baz) -> Baz#dflow.created < Bar1#dflow.completed end, Bazes))
        end },
      { "Transient data wiped out",
        fun() ->
                register_if_necessary(),
                mnesia:clear_table(dflow_tests),
                dflow:add_data({foo, ?MODULE}, ["item8", "item9"]),
                test_receive(8),
                Foos = dfq:completed({foo, ?MODULE}),
                ?assert(lists:all(fun(#dflow{data=D}) -> D =:= transient end, Foos))
        end },
      { "Incomplete re-injected",
        fun() ->
                register_if_necessary(),
                mnesia:clear_table(dflow_tests),
                mnesia:dirty_write(dflow_tests, #dflow{uuid="foo1",stage=foo,module=dflow_tests,
                                                       status=created,created=now(), data="item7"}),
                mnesia:dirty_write(dflow_tests, #dflow{uuid="foo2",stage=foo,module=dflow_tests,
                                                       status=created,created=now(), data="item8"}),
                dflow:register([dflow_tests]),
                test_receive(8)
        end },
      { "Identity & None",
        fun() ->
                register_if_necessary(),
                mnesia:clear_table(dflow_tests),
                dflow:add_datum({identity, ?MODULE}, "item1"),
                test_receive(1),
                ?assertEqual(1, length(dfq:completed({identity, ?MODULE}))),
                ?assertEqual(1, length(dfq:completed({none, ?MODULE})))
        end }
     ]}.

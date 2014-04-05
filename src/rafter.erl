-module(rafter).

-include("rafter.hrl").
-include("rafter_consensus_fsm.hrl").
-include("rafter_opts.hrl").

%% API
-export([start_node/2, stop_node/1, op/2, read_op/2, set_config/2,
         get_leader/1, get_entry/2, get_last_entry/1]).

%% Test API
-export([start_cluster/0, start_test_node/1]).

start_node(Peer, Opts) ->
    io:format("Starting peer ~p~n", [Peer]),
    rafter_sup:start_peer(Peer, Opts).

stop_node(Peer) ->
    io:format("Stopping peer ~p~n", [Peer]),
    rafter_sup:stop_peer(Peer).

%% @doc Run an operation on the backend state machine.
%% Note: Peer is just the local node in production.
op(Peer, Command) ->
    Id = uuid:generate(),
    rafter_consensus_fsm:op(Peer, {Id, Command}).

read_op(Peer, Command) ->
    Id = uuid:generate(),
    rafter_consensus_fsm:read_op(Peer, {Id, Command}).

set_config(Peer, NewServers) ->
    Id = uuid:generate(),
    rafter_consensus_fsm:set_config(Peer, {Id, NewServers}).

-spec get_leader(peer()) -> peer() | undefined.
get_leader(Peer) ->
    rafter_consensus_fsm:get_leader(Peer).

-spec get_entry(peer(), non_neg_integer()) -> term().
get_entry(Peer, Index) ->
    rafter_log:get_entry(Peer, Index).

-spec get_last_entry(peer()) -> term().
get_last_entry(Peer) ->
    rafter_log:get_last_entry(Peer).

%% =============================================
%% Test Functions
%% =============================================
-define(test_key, x).
-define(test_val, 55).

start_cluster() ->
    {ok, _Started} = application:ensure_all_started(rafter),
    Opts = #rafter_opts{state_machine=rafter_backend_ets, logdir="./data", 
                        clean_start=true},
    Peers = [peer1, peer2, peer3, peer4, peer5],
    [start_node(Me, Opts) || Me <- Peers],
    set_config(peer1, [peer1, peer2, peer3, peer4, peer5]),
    Leader = get_leader(peer1),
    op(Leader, {new, test_table}),
    op(get_leader(Leader), {put, test_table, 
                            ?test_key, ?test_val}),
    TestVal = read_op(get_leader(Leader), 
                      {get, test_table, ?test_key}),
    op(get_leader(Leader), {delete, test_table}),
    case TestVal =:= {ok, ?test_val} of
      false -> 
            io:format("Value does not match, got: ~p, expected~p~n", [TestVal, {ok, ?test_val}]),
            throw(asserion_fail);
      _ -> io:format("Passed~n")
    end,
    [stop_node(Me) || Me <- Peers], 
    application:stop(rafter), 
    application:stop(lager).

start_test_node(Name) ->
    {ok, _Started} = application:ensure_all_started(rafter),
    Me = {Name, node()},
    Opts = #rafter_opts{state_machine=rafter_backend_ets, logdir="./data"},
    start_node(Me, Opts).

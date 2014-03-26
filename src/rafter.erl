-module(rafter).

-include("rafter.hrl").
-include("rafter_consensus_fsm.hrl").
-include("rafter_opts.hrl").

%% API
-export([start_node/2, stop_node/1, op/2, read_op/2, set_config/2,
         get_leader/1, get_entry/2, get_last_entry/1]).

%% Test API
-export([start_cluster/0, start_test_node/1, start_concuerror_cluster/0]).

start_node(Peer, Opts) ->
    rafter_sup:start_peer(Peer, Opts).

stop_node(Peer) ->
    rafter_sup:stop_peer(Peer).

%% @doc Run an operation on the backend state machine.
%% Note: Peer is just the local node in production.
op(Peer, Command) ->
    Id = druuid:v4(),
    rafter_consensus_fsm:op(Peer, {Id, Command}).

read_op(Peer, Command) ->
    Id = druuid:v4(),
    rafter_consensus_fsm:read_op(Peer, {Id, Command}).

set_config(Peer, NewServers) ->
    Id = druuid:v4(),
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

start_peers([Peer | Rest], Opts) ->
    io:format("Starting peer ~p~n", [Peer]),
    rafter_sup:start_peer(Peer, Opts),
    io:format("Started peer ~p~n", [Peer]),
    start_peers(Rest, Opts);
start_peers([], _) ->
    ok.

stop_peers([Peer | Rest]) ->
    io:format("Stoppin peer ~p~n", [Peer]),
    rafter_sup:stop_peer(Peer),
    io:format("Stopping peer ~p~n", [Peer]),
    stop_peers(Rest);
stop_peers([]) ->
    ok.

start_cluster() ->
    %{ok, _Started} = application:ensure_all_started(rafter),
    Opts = #rafter_opts{state_machine=rafter_backend_echo, logdir="./log", clean_start=true},
    Peers = [peer1, peer2, peer3, peer4, peer5],
    io:format("Starting peers~n"),
    start_peers(Peers, Opts),
    io:format("Started all peers~n"),
    set_config(peer1, Peers),
    Leader = get_leader(peer2),
    op(Leader, {new, food}).

kill_app(App, {ok, Pid}) ->
    App:stop([]),
    exit(Pid, kill);
kill_app(App, {ok, Pid, State}) ->
    App:stop(State),
    exit(Pid, kill);
kill_app(_App, _) ->
    ok.

start_concuerror_cluster() ->
    io:format("start_concuerror_cluster Starting concuerror cluster~n"),
    %_GrRet = gr_app:start(normal, []),
    %_LgrRet = lager_app:start(normal, []),
    RftrRet = rafter_app:start(normal, []),
    %{ok, _Started} = application:ensure_all_started(rafter),
    Opts = #rafter_opts{state_machine=rafter_backend_echo, logdir="./log", clean_start=true},
    Peers = [peer1, peer2, peer3, peer4, peer5],
    io:format("start_concuerror_cluster Starting peers~n"),
    start_peers(Peers, Opts),
    io:format("start_concuerror_cluster Started all peers~n"),
    set_config(peer1, Peers),
    Leader = get_leader(peer2),
    op(Leader, {new, food}),
    io:format("start_concuerror_cluster Done exploring, stopped peers, killing peers~n"),
    stop_peers(Peers),
    io:format("start_concuerror_cluster Done exploring, stopped peers, killing things~n"),
    kill_app(rafter_app, RftrRet),
    %kill_app(lager_app, LgrRet),
    %kill_app(gr_app, GrRet),
    io:format("start_concuerror_cluster Concuerror cluster killed~n").
    

start_test_node(Name) ->
    {ok, _Started} = application:ensure_all_started(rafter),
    Me = {Name, node()},
    Opts = #rafter_opts{state_machine=rafter_backend_ets, logdir="./data"},
    start_node(Me, Opts).

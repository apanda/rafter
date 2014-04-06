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
    Id = rafter_uuid:v4(),
    rafter_consensus_fsm:op(Peer, {Id, Command}).

read_op(Peer, Command) ->
    Id = rafter_uuid:v4(),
    rafter_consensus_fsm:read_op(Peer, {Id, Command}).

set_config(Peer, NewServers) ->
    Id = rafter_uuid:v4(),
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
    start_node(Peer, Opts),
    io:format("Started peer ~p~n", [Peer]),
    start_peers(Rest, Opts);
start_peers([], _) ->
    ok.

stop_peers([Peer | Rest]) ->
    io:format("Stopping peer ~p~n", [Peer]),
    stop_node(Peer),
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

start_concuerror_cluster() ->
    io:format("start_concuerror_cluster Starting concuerror cluster~n"),
    {ok, Pid} = rafter_app:start(normal, []),
    Opts = #rafter_opts{state_machine=rafter_backend_echo, logdir="./log", clean_start=true},
    Peers = [peer1, peer2, peer3],
    start_peers(Peers, Opts),
    io:format("start_concuerror_cluster Started all peers~n"),
    set_config(peer1, Peers),
    io:format("start_concuerror_cluster Set configuration ~n"),
    Leader = get_leader(peer2),
    io:format("start_concuerror_cluster Get leader, Leader is ~p ~n", [Leader]),
    %op(Leader, {new, food}),
    %io:format("start_concuerror_cluster Done exploring, stopped peers, killing peers~n"),
    %demonitor(RftrRet),
    unlink(Pid),
    Mon = monitor(process, Pid),
    io:format("PID is ~p~n", [Pid]),
    exit(Pid, kill),
    receive
      {'DOWN', Mon, process, Pid, _} ->
        io:format("start_concuerror_cluster Concuerror cluster killed~n");
      Msg ->
        io:format("Some other kind of message ~p~n", [Msg])
    end,
    stop_peers(Peers),
    case Leader of
      undefined -> throw(bad_leader);
      _ -> ok
    end.

start_test_node(Name) ->
    {ok, _Started} = application:ensure_all_started(rafter),
    Me = {Name, node()},
    Opts = #rafter_opts{state_machine=rafter_backend_ets, logdir="./data"},
    start_node(Me, Opts).

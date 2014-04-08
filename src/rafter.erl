-module(rafter).

-include("rafter.hrl").
-include("rafter_consensus_fsm.hrl").
-include("rafter_opts.hrl").

%% API
-export([start_node/2, stop_node/1, op/2, read_op/2, set_config/2,
         get_leader/1, get_entry/2, get_last_entry/1, start_multi_test/1,
         start_named_cluster_multi/1, concuerror_cluster/0]).

%% Test API
-export([start_cluster/0, start_test_node/1]).

start_node(Peer, Opts) ->
    io:format("Starting peer ~p~n", [Peer]),
    try 
      rafter_sup:start_peer(Peer, Opts)
    catch
      exit:Ex -> 
        io:format("Caught exit :~p in start_node~n", [Ex]),
        exit(Ex);
      _:_ ->
        io:format("Caught somethign else~n")
    end.
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
-define(election_times, [1500, 500, 650, 2000]).

start_cluster() ->
    {ok, _Started} = application:ensure_all_started(rafter),
    Opts = #rafter_opts{state_machine=rafter_backend_ets, logdir="./data",
                        clean_start=true},
    Peers = [peer1, peer2, peer3, peer4],
    %[start_node(Me, Opts) || Me <- Peers],
    [start_node(Me, Opts#rafter_opts{election_timer = Time})
     || {Me, Time} <- lists:zip(Peers, ?election_times)],
    set_config(lists:last(Peers), Peers),
    Leader = get_leader(peer1),
    receive
      after 700 ->
        ok
    end,
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
    application:stop(rafter).

start_named_cluster(Name) ->
    %{ok, _Started} = application:ensure_all_started(rafter),
    {ok, Pid} = rafter_app:start(normal, []),
    %io:format("rafter_app:start returns~p~n", [Result]),
    Opts = #rafter_opts{state_machine=rafter_backend_ets,
                        clean_start=true, heartbeat_time = 250, log_service=rafter_nodisk_log},
    OPeers = [peer1, peer2, peer3, peer4],
    Peers = [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeers],
    [start_node(Me, Opts#rafter_opts{election_timer = Time})
     || {Me, Time} <- lists:zip(Peers, ?election_times)],
    set_config(lists:last(Peers), Peers),
    receive
      after 300 ->
        ok
    end,
    Leader = get_leader(lists:last(Peers)),
    io:format("~p says Leader is ~p~n", [lists:last(Peers), Leader]),
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
    unlink(Pid),
    Mon = monitor(process, Pid),
    exit(Pid, kill),
    receive
      {'DOWN', Mon, process, _, _}  ->
        %io:format("Message for down~n");
        ok;
      Msg ->
        io:format("Received other message ~p~n", [Msg])
    end.

start_named_cluster_multi(Name) ->
    random:seed(7, 20, 69), % Seed the random number generator
    start_named_cluster(Name),
    proc_lib:init_ack(ok).

start_test_node(Name) ->
    {ok, _Started} = application:ensure_all_started(rafter),
    Me = {Name, node()},
    Opts = #rafter_opts{state_machine=rafter_backend_ets, logdir="./data"},
    start_node(Me, Opts).

concuerror_cluster() ->
  random:seed(7, 20, 69), % Seed the random number generator
  start_named_cluster("concuerror").

start_multi_test(Count) when is_integer(Count) ->
  case Count of
    0 ->
      ok;
    _ ->
      %[Name] = io_lib:format("~B", [Count]),
      ok = proc_lib:start(?MODULE, start_named_cluster_multi, ["multi"]),
      start_multi_test(Count - 1)
  end;
start_multi_test([Count]) ->
  case string:to_integer(Count) of
    {error, Reason} ->
       io:format("Bad argument, need int, given ~p (~p) ~n", [Count, Reason]);
    {C, _} ->
       start_multi_test(C)
    end.

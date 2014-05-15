-module(rafter).

-include("rafter.hrl").
-include("rafter_consensus_fsm.hrl").
-include("rafter_opts.hrl").

%% API
-export([start_node/2, stop_node/1, op/2, read_op/2, set_config/2,
         get_leader/1, get_entry/2, get_last_entry/1, start_multi_test/1,
         start_named_cluster_multi/1, concuerror_cluster/0, concuerror_error_cluster/0,
         concuerror_error_cluster_small/0, concuerror_min_error_cluster/0]).

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

%-define(stop_manually, true).
-define(stop_noop, true).
-ifdef(stop_noop).
clean_process(_Pid) ->
    ok.
-endif.
-ifdef(stop_manually).
clean_process(Pid) ->
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
-endif.
-ifdef(stop_application).
clean_process() ->
  application:stop(rafter),
  application:unload(rafter).
-endif.

start_named_cluster(Name) ->
    %{ok, _Started} = application:ensure_all_started(rafter),
    {ok, Pid} = rafter_app:start(normal, []),
    %io:format("rafter_app:start returns~p~n", [Result]),
    Opts = #rafter_opts{state_machine=rafter_backend_dict,
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
    case Leader =:= undefined of
      true -> 
        io:format("No leader found,dying here~n"),
        %[stop_node(Me) || Me <- Peers],
        clean_process(Pid),
        throw(asserion_fail);
      false -> ok
    end,
    op(get_leader(Leader), {?test_key, ?test_val}),
    TestVal = read_op(get_leader(Leader), ?test_key),
    case TestVal =:= {ok, ?test_val} of
      false ->
            %[stop_node(Me) || Me <- Peers],
            clean_process(Pid),
            io:format("Value does not match, got: ~p, expected~p~n", [TestVal, {ok, ?test_val}]),
            throw(asserion_fail);
      _ -> io:format("Passed, expected ~p got ~p ~n", [{ok, ?test_val}, TestVal])
    end,
    %[stop_node(Me) || Me <- Peers],
    clean_process(Pid).


-define(error_etime, [500, 450]).
%-define(error_etime, [500]).
start_named_cluster_error_large(Name) ->
    {ok, Pid} = rafter_app:start(normal, []),
    Opts = #rafter_opts{state_machine=rafter_backend_dict,
                        clean_start=true, heartbeat_time = 50, log_service=rafter_nodisk_log},
    OPeers = [pa, pb, pc, pd, pe, pf, pg, ph, pi, pj1, pj2, pj3, pj4, pj5, pj6, pj7, pj8],
    OPeerA = [pa, pc, pd, pe],
    OPeerB = [pb, pa, pf, pg, ph],
    OPeersToStartFirst = [pa, pb],
    %OPeersToStartFirst = [pb],
    OPeersToStartNext = [pc, pg, pd, pe, pf, ph, pi, pj1, pj2, pj3, pj4, pj5, pj6, pj7, pj8],
    Peers = [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeers],
    PeerA = [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeerA],
    PeerB = [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeerB],
    PeersToStartFirst = [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeersToStartFirst],
    PeersToStartNext= [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeersToStartNext],
    [start_node(Me, Opts#rafter_opts{election_timer = Time})
     || {Me, Time} <- lists:zip(PeersToStartFirst, ?error_etime)],
    [Pa, Pb] = PeersToStartFirst,
    %[Pb] = PeersToStartFirst,
    set_config(Pb, PeerB),
    set_config(Pa, PeerA),
    receive
      after 300 ->
        ok
    end,
    LeaderPa = get_leader(Pa),
    %io:format("~p says Leader is ~p~n", [Pa, LeaderPa]),
    LeaderPb = get_leader(Pb),
    %io:format("~p says Leader is ~p~n", [Pb, LeaderPb]),
    [start_node(Me, Opts#rafter_opts{election_timer = 10000})
     || Me<-PeersToStartNext],
    receive
      after 700 ->
        ok
    end,
    LeaderPa1 = get_leader(Pa),
    %io:format("~p says Leader is ~p~n", [Pa, LeaderPa1]),
    LeaderPb1 = get_leader(Pb),
    %io:format("~p says Leader is ~p~n", [Pb, LeaderPb1]),
    Pc = list_to_atom(atom_to_list(pc) ++ "_" ++ Name),
    Pf = list_to_atom(atom_to_list(pf) ++ "_" ++ Name),
    LeaderPc = get_leader(Pc),
    %io:format("~p says Leader is ~p~n", [Pc, LeaderPc]),
    LeaderPf = get_leader(Pf),
    %io:format("~p says Leader is ~p~n", [Pf, LeaderPf]),
    case LeaderPb1 =:= undefined orelse LeaderPb1 =:= LeaderPa1 of
      true ->
        %io:format("Passed~n"),
        ok;
      false ->
        %io:format("Failed found ~p ~p~n", [LeaderPa1, LeaderPb1]),
        throw(assetion_fail)
    end,
    case LeaderPa1 =:= LeaderPc andalso LeaderPb1 =:= LeaderPf of
      true ->
        %io:format("Passed found ~p ~p ~p ~p~n", [LeaderPa1, LeaderPc, LeaderPb1, LeaderPf]),
        ok;
      false ->
        %io:format("Failed found ~p ~p ~p ~p~n", [LeaderPa1, LeaderPc, LeaderPb1, LeaderPf]),
        throw(assetion_fail)
    end,
    case LeaderPa1 =:= undefined of
      %true -> exit(assertion_fail);
      true -> throw(assertion_fail);
      false ->
            ok
    end,
    clean_process(Pid).

start_named_cluster_error_small(Name) ->
    {ok, Pid} = rafter_app:start(normal, []),
    Opts = #rafter_opts{state_machine=rafter_backend_dict,
                        clean_start=true, heartbeat_time = 50, log_service=rafter_nodisk_log},
    OPeers = [pa, pb, pc, pd, pe, pf, pg, ph, pi, pj1],
    OPeerA = [pa, pc, pd, pe, pi],
    OPeerB = [pb, pa, pf, pg, ph],
    OPeersToStartFirst = [pa, pb],
    %OPeersToStartFirst = [pb],
    OPeersToStartNext = [pc, pg, pd, pe, pf, ph, pi, pj1],
    Peers = [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeers],
    PeerA = [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeerA],
    PeerB = [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeerB],
    PeersToStartFirst = [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeersToStartFirst],
    PeersToStartNext= [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeersToStartNext],
    [start_node(Me, Opts#rafter_opts{election_timer = Time})
     || {Me, Time} <- lists:zip(PeersToStartFirst, ?error_etime)],
    [Pa, Pb] = PeersToStartFirst,
    %[Pb] = PeersToStartFirst,
    set_config(Pb, PeerB),
    set_config(Pa, PeerA),
    receive
      after 300 ->
        ok
    end,
    LeaderPa = get_leader(Pa),
    %io:format("~p says Leader is ~p~n", [Pa, LeaderPa]),
    LeaderPb = get_leader(Pb),
    %io:format("~p says Leader is ~p~n", [Pb, LeaderPb]),
    [start_node(Me, Opts#rafter_opts{election_timer = 10000})
     || Me<-PeersToStartNext],
    receive
      after 700 ->
        ok
    end,
    LeaderPa1 = get_leader(Pa),
    %io:format("~p says Leader is ~p~n", [Pa, LeaderPa1]),
    LeaderPb1 = get_leader(Pb),
    %io:format("~p says Leader is ~p~n", [Pb, LeaderPb1]),
    Pc = list_to_atom(atom_to_list(pc) ++ "_" ++ Name),
    Pf = list_to_atom(atom_to_list(pf) ++ "_" ++ Name),
    LeaderPc = get_leader(Pc),
    %io:format("~p says Leader is ~p~n", [Pc, LeaderPc]),
    LeaderPf = get_leader(Pf),
    %io:format("~p says Leader is ~p~n", [Pf, LeaderPf]),
    case LeaderPb1 =:= undefined orelse LeaderPb1 =:= LeaderPa1 of
      true ->
        %io:format("Passed~n"),
        ok;
      false ->
        %io:format("Failed found ~p ~p~n", [LeaderPa1, LeaderPb1]),
        throw(assetion_fail)
    end,
    case LeaderPa1 =:= LeaderPc andalso LeaderPb1 =:= LeaderPf of
      true ->
        %io:format("Passed found ~p ~p ~p ~p~n", [LeaderPa1, LeaderPc, LeaderPb1, LeaderPf]),
        ok;
      false ->
        %io:format("Failed found ~p ~p ~p ~p~n", [LeaderPa1, LeaderPc, LeaderPb1, LeaderPf]),
        throw(assetion_fail)
    end,
    case LeaderPa1 =:= undefined of
      %true -> exit(assertion_fail);
      true -> throw(assertion_fail);
      false ->
            ok
    end,
    clean_process(Pid).

start_minimized_cluster_error(Name) ->
    {ok, Pid} = rafter_app:start(normal, []),
    Opts = #rafter_opts{state_machine=rafter_backend_dict,
                        clean_start=true, heartbeat_time = 50, log_service=rafter_nodisk_log},
    OPeers = [pa, pb, pc, pd, pe, pf, pg, ph],
    OPeerA = [pa, pc, pd, pe],
    OPeerB = [pb, pa, pf, pg, ph],
    OPeersToStartFirst = [pa, pb],
    %OPeersToStartFirst = [pb],
    OPeersToStartNext = [pc, pg, pd, pe, pf, ph],
    Peers = [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeers],
    PeerA = [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeerA],
    PeerB = [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeerB],
    PeersToStartFirst = [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeersToStartFirst],
    PeersToStartNext= [list_to_atom(atom_to_list(Peer) ++ "_" ++ Name) || Peer <- OPeersToStartNext],
    [start_node(Me, Opts#rafter_opts{election_timer = Time})
     || {Me, Time} <- lists:zip(PeersToStartFirst, ?error_etime)],
    [Pa, Pb] = PeersToStartFirst,
    %[Pb] = PeersToStartFirst,
    set_config(Pb, PeerB),
    %set_config(Pa, PeerA),
    receive
      after 300 ->
        ok
    end,
    LeaderPa = get_leader(Pa),
    %io:format("~p says Leader is ~p~n", [Pa, LeaderPa]),
    LeaderPb = get_leader(Pb),
    %io:format("~p says Leader is ~p~n", [Pb, LeaderPb]),
    [start_node(Me, Opts#rafter_opts{election_timer = 10000})
     || Me<-PeersToStartNext],
    receive
      after 700 ->
        ok
    end,
    LeaderPa1 = get_leader(Pa),
    %io:format("~p says Leader is ~p~n", [Pa, LeaderPa1]),
    LeaderPb1 = get_leader(Pb),
    %io:format("~p says Leader is ~p~n", [Pb, LeaderPb1]),
    case LeaderPb1 =:= undefined orelse LeaderPb1 =:= LeaderPa1 of
      true ->
        %io:format("Passed~n"),
        ok;
      false ->
        %io:format("Failed found ~p ~p~n", [LeaderPa1, LeaderPb1]),
        throw(assetion_fail)
    end,
    %Pc = list_to_atom(atom_to_list(pc) ++ "_" ++ Name),
    Pf = list_to_atom(atom_to_list(pf) ++ "_" ++ Name),
    %LeaderPc = get_leader(Pc),
    %io:format("~p says Leader is ~p~n", [Pc, LeaderPc]),
    LeaderPf = get_leader(Pf),
    %io:format("~p says Leader is ~p~n", [Pf, LeaderPf]),
    case LeaderPb1 =:= LeaderPf of
      true ->
        %io:format("Passed~n"),
        ok;
      false ->
        %io:format("Failed found ~p ~p~n", [LeaderPa1, LeaderPf]),
        throw(assetion_fail)
    end,
    clean_process(Pid).

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

concuerror_error_cluster() ->
  random:seed(7, 20, 69), % Seed the random number generator
  start_named_cluster_error_large("concuerror").

concuerror_error_cluster_small() ->
  random:seed(7, 20, 69), % Seed the random number generator
  start_named_cluster_error_small("concuerror").

concuerror_min_error_cluster() ->
  random:seed(7, 20, 69), % Seed the random number generator
  start_minimized_cluster_error("concuerror").

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

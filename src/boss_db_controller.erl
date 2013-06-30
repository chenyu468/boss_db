-module(boss_db_controller).

-behaviour(gen_server).

-export([start_link/0, start_link/1, try_connection/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(MAXDELAY, 10000).

-record(state, {
	  connection_state,
	  connection_delay,
	  connection_retry_timer,
	  options,
        adapter, 
        read_connection, 
        write_connection, 
        shards = [],
        model_dict = dict:new(),
        cache_enable,
        cache_ttl,
        cache_prefix,
        depth = 0}).

start_link() ->
    start_link([]).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

connections_for_adapter(Adapter, Options) ->
    case Adapter:init(Options) of
        {ok, {readwrite, Read, Write}} ->
            {ok, {Read, Write}};
        {ok, Other} ->
            {ok, {Other, Other}};
	Error ->
	    {connection_error, Error}
    end.

setup_reconnect(State) ->
    Delay = case State#state.connection_delay of
		D when D < ?MAXDELAY ->
		    D;
		_ ->
		    ?MAXDELAY
	    end,
    Pid = self(),
    timer:apply_after(Delay, boss_db_controller, try_connection, [Pid, State#state.options]).

try_connection(Pid, Options) ->
    gen_server:cast(Pid, {try_connect, Options}).

terminate_connections(Adapter, RC, RC) ->
    Adapter:terminate(RC);
terminate_connections(Adapter, RC, WC) ->
    Adapter:terminate(RC),
    Adapter:terminate(WC).

init(Options) ->
    AdapterName = proplists:get_value(adapter, Options, mock),
    Adapter = list_to_atom(lists:concat(["boss_db_adapter_", AdapterName])),
    CacheEnable = proplists:get_value(cache_enable, Options, false),
    CacheTTL = proplists:get_value(cache_exp_time, Options, 60),
    process_flag(trap_exit, true),
    try_connection(self(), Options),
    {ok, #state{connection_state = connecting,
		connection_delay = 1,
		options = Options,
		adapter = Adapter,
		cache_enable = CacheEnable,
		cache_ttl = CacheTTL, cache_prefix = db }}.

handle_call(_Anything, _Anyone, State) when State#state.connection_state /= connected ->
    {reply, db_connection_down, State};

handle_call({find, Key}, From, #state{ cache_enable = true, cache_prefix = Prefix } = State) ->
    case boss_cache:get(Prefix, Key) of
        undefined ->
            {reply, Res, _} = handle_call({find, Key}, From, State#state{ cache_enable = false }),
            IsSuccess = (Res =:= undefined orelse (is_tuple(Res) andalso element(1, Res) =/= error)),
            case IsSuccess of
                true ->
                    boss_cache:set(Prefix, Key, Res, State#state.cache_ttl),
                    WatchString = lists:concat([Key, ", ", Key, ".*"]), 
                    boss_news:set_watch(Key, WatchString, fun boss_db_cache:handle_record_news/3, 
                        {Prefix, Key}, State#state.cache_ttl);
                _ -> error % log it here?
            end,
            {reply, Res, State};
        CachedValue ->
            boss_news:extend_watch(Key),
            {reply, CachedValue, State}
    end;
handle_call({find, Key}, _From, #state{ cache_enable = false } = State) ->
    {Adapter, Conn, _} = db_for_key(Key, State),
    {reply, Adapter:find(Conn, Key), State};

handle_call({find, Type, Conditions, Max, Skip, Sort, SortOrder, Include} = Cmd, From, 
    #state{ cache_enable = true, cache_prefix = Prefix } = State) ->
    Key = {Type, Conditions, Max, Skip, Sort, SortOrder},
    case boss_cache:get(Prefix, Key) of
        undefined ->
            {reply, Res, _} = handle_call(Cmd, From, State#state{ cache_enable = false }),
            case is_list(Res) of
                true ->
                    DummyRecord = boss_record_lib:dummy_record(Type),
                    BelongsToTypes = DummyRecord:belongs_to_types(),
                    IncludedRecords = lists:foldl(fun
                            ({RelationshipName, InnerInclude}, Acc) ->
                                RecordList = case proplists:get_value(RelationshipName, BelongsToTypes) of
                                    undefined -> [];
                                    RelationshipType ->
                                        IdList = lists:map(fun(Record) -> 
                                                    Record:get(lists:concat([RelationshipType, "_id"]))
                                            end, Res),
                                        handle_call({find, RelationshipName, 
                                                [{'id', 'in', IdList}], all, 0, id, ascending,
                                                InnerInclude}, From, State)
                                end,
                                RecordList ++ Acc
                        end, [], lists:map(fun({R, I}) -> {R, I}; (R) -> {R, []} end, Include)),
                    lists:map(fun(Rec) ->
                                boss_cache:set(Prefix, Rec:id(), Rec, State#state.cache_ttl)
                        end, IncludedRecords),
                    boss_cache:set(Prefix, Key, Res, State#state.cache_ttl),
                    WatchString = lists:concat([inflector:pluralize(atom_to_list(Type)), ", ", Type, "-*.*"]), 
                    boss_news:set_watch(Key, WatchString, fun boss_db_cache:handle_collection_news/3, 
                        {Prefix, Key}, State#state.cache_ttl);
                _ -> error % log it here?
            end,
            {reply, Res, State};
        CachedValue ->
            boss_news:extend_watch(Key),
            {reply, CachedValue, State}
    end;
handle_call({find, Type, Conditions, Max, Skip, Sort, SortOrder, _}, _From, #state{ cache_enable = false } = State) ->
    {Adapter, Conn, _} = db_for_type(Type, State),
    {reply, Adapter:find(Conn, Type, Conditions, Max, Skip, Sort, SortOrder), State};

handle_call({get_migrations_table}, _From, #state{ cache_enable = false } = State) ->
    {Adapter, Conn} = {State#state.adapter, State#state.read_connection},
    {reply, Adapter:get_migrations_table(Conn), State};

handle_call({migration_done, Tag, Direction}, _From, #state{ cache_enable = false } = State) ->
    {Adapter, Conn} = {State#state.adapter, State#state.write_connection},
    {reply, Adapter:migration_done(Conn, Tag, Direction), State};

handle_call({count, Type}, _From, State) ->
    {Adapter, Conn, _} = db_for_type(Type, State),
    {reply, Adapter:count(Conn, Type), State};

handle_call({count, Type, Conditions}, _From, State) ->
    {Adapter, Conn, _} = db_for_type(Type, State),
    {reply, Adapter:count(Conn, Type, Conditions), State};

handle_call({counter, Counter}, _From, State) ->
    {Adapter, Conn, _} = db_for_counter(Counter, State),
    {reply, Adapter:counter(Conn, Counter), State};

handle_call({incr, Key}, _From, State) ->
    {Adapter, _, Conn} = db_for_counter(Key, State),
    {reply, Adapter:incr(Conn, Key), State};

handle_call({incr, Key, Count}, _From, State) ->
    {Adapter, _, Conn} = db_for_counter(Key, State),
    {reply, Adapter:incr(Conn, Key, Count), State};

handle_call({delete, Id}, _From, State) ->
    {Adapter, _, Conn} = db_for_key(Id, State),
    {reply, Adapter:delete(Conn, Id), State};

handle_call({save_record, Record}, _From, State) ->
    {Adapter, _, Conn} = db_for_record(Record, State),
    {reply, Adapter:save_record(Conn, Record), State};

handle_call(push, _From, State) ->
    Adapter = State#state.adapter,
    Conn = State#state.write_connection,
    Depth = State#state.depth,
    {reply, Adapter:push(Conn, Depth), State#state{depth = Depth + 1}};

handle_call(pop, _From, State) ->
    Adapter = State#state.adapter,
    Conn = State#state.write_connection,
    Depth = State#state.depth,
    {reply, Adapter:pop(Conn, Depth), State#state{depth = Depth - 1}};

handle_call(depth, _From, State) ->
    {reply, State#state.depth, State};

handle_call(dump, _From, State) ->
    Adapter = State#state.adapter,
    Conn = State#state.read_connection,
    {reply, Adapter:dump(Conn), State};

handle_call({create_table, TableName, TableDefinition}, _From, State) ->
    Adapter = State#state.adapter,
    Conn = State#state.write_connection,
    {reply, Adapter:create_table(Conn, TableName, TableDefinition), State};

handle_call({table_exists, TableName}, _From, State) ->
    Adapter = State#state.adapter,
    Conn = State#state.read_connection,
    {reply, Adapter:table_exists(Conn, TableName), State};

handle_call({execute, Commands}, _From, State) ->
    Adapter = State#state.adapter,
    Conn = State#state.write_connection,
    error_logger:info_report([boss_db_controller_handle_call_execute_1,Commands]),
    {reply, Adapter:execute(Conn, Commands), State};

handle_call({execute, Commands, Params}, _From, State) ->
    Adapter = State#state.adapter,
    Conn = State#state.write_connection,
    {reply, Adapter:execute(Conn, Commands, Params), State};

handle_call({transaction, TransactionFun}, _From, State) ->
    Adapter = State#state.adapter,
    Conn = State#state.write_connection,
    {reply, Adapter:transaction(Conn, TransactionFun), State};

handle_call(state, _From, State) ->
    {reply, State, State}.

handle_cast({try_connect, Options}, State) when State#state.connection_state /= connected ->
    Adapter = State#state.adapter,
    CacheEnable = State#state.cache_enable,
    CacheTTL = State#state.cache_ttl,
    try connections_for_adapter(Adapter, Options) of
	{ok, {ReadConn, WriteConn}} ->
	    {Shards, ModelDict} =
		lists:foldr(fun(ShardOptions, {ShardAcc, ModelDictAcc}) ->
				    case proplists:get_value(db_shard_models, ShardOptions, []) of
					[] ->
					    {ShardAcc, ModelDictAcc};
					Models ->
					    ShardAdapter = case proplists:get_value(db_adapter, ShardOptions) of
							       undefined -> Adapter;
							       ShortName -> list_to_atom(lists:concat(["boss_db_adapter_", ShortName]))
							   end,
					    MergedOptions = case proplists:get_value(db_replication_set, ShardOptions) of
								undefined -> ShardOptions ++ proplists:delete(db_replication_set, Options);
								_ -> ShardOptions ++ Options
							    end,
					    {ok, {ShardRead, ShardWrite}} = connections_for_adapter(ShardAdapter, MergedOptions),
					    Index = erlang:length(ShardAcc),
					    NewDict = lists:foldr(fun(ModelAtom, Dict) ->
									  dict:store(ModelAtom, Index, Dict)
								  end, ModelDictAcc, Models),
					    {[{ShardAdapter, ShardRead, ShardWrite}|ShardAcc], NewDict}
				    end
			    end, {[], dict:new()}, proplists:get_value(shards, Options, [])),
	    {noreply, #state{connection_state = connected, connection_delay = 1,
			     adapter = Adapter, read_connection = ReadConn, write_connection = WriteConn,
			     shards = lists:reverse(Shards), model_dict = ModelDict, options = Options,
			     cache_enable = CacheEnable, cache_ttl = CacheTTL, cache_prefix = db }};
	_Failure ->
	    {ok, Tref} = setup_reconnect(State),
	    {noreply, #state{connection_state = disconnected, connection_delay = State#state.connection_delay * 2,
			     connection_retry_timer = Tref,
			     adapter = Adapter, read_connection = undefined, write_connection = undefined,
			     options = Options, cache_enable = CacheEnable, cache_ttl = CacheTTL, cache_prefix = db }}
    catch
	_Error ->
	    {ok, Tref} = setup_reconnect(State),
	    {noreply, #state{connection_state = disconnected, connection_delay = State#state.connection_delay * 2,
			     connection_retry_timer = Tref,
			     adapter = Adapter, read_connection = undefined, write_connection = undefined,
			     options = Options, cache_enable = CacheEnable, cache_ttl = CacheTTL, cache_prefix = db }}
    end;

handle_cast(_Request, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    Adapter = State#state.adapter,
    case State#state.connection_retry_timer of
	undefined ->
	    noop;
	Timer ->
	    timer:cancel(Timer)
    end,
    terminate_connections(Adapter, State#state.read_connection, State#state.write_connection),
    lists:map(fun({A, RC, WC}) -> terminate_connections(A, RC, WC) end, State#state.shards).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info(stop, State) ->
    {stop, shutdown, State};

handle_info({'EXIT', _From, _Reason}, State) when State#state.connection_state == connected ->
    {ok, Tref} = setup_reconnect(State),
    {noreply, State#state { connection_state = disconnected, connection_delay = State#state.connection_delay * 2,
			    connection_retry_timer = Tref } };

handle_info({'EXIT', _From, _Reason}, State) ->
    {noreply, State#state { connection_state = disconnected } };

handle_info(_Info, State) ->
    {noreply, State}.

db_for_counter(_Counter, State) ->
    {State#state.adapter, State#state.read_connection, State#state.write_connection}.

db_for_record(Record, State) ->
    db_for_type(element(1, Record), State).

db_for_key(Key, State) ->
    db_for_type(infer_type_from_id(Key), State).

db_for_type(Type, State) ->
    case dict:find(Type, State#state.model_dict) of
        {ok, Index} ->
            lists:nth(Index + 1, State#state.shards);
        _ ->
            {State#state.adapter, State#state.read_connection, State#state.write_connection}
    end.

infer_type_from_id(Id) when is_binary(Id) ->
    infer_type_from_id(binary_to_list(Id));
infer_type_from_id(Id) when is_list(Id) ->
    list_to_atom(hd(string:tokens(Id, "-"))).

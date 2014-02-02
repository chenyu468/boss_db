%% @doc Chicago Boss database abstraction

-module(boss_db_2).
-compile([{parse_transform, lager_transform}]).

-export([start/2, stop/0]).

-export([
        migrate/2,
        migrate/3,
        find/2,
        find/3,
        find/4,
        find_first/2,
        find_first/3,
        find_first/4,
        find_last/2,
        find_last/3,
        find_last/4,
        count/2,
        count/3,
        counter/2,
        incr/2,
        incr/3,
        delete/2,
        save_record/2,
        push/1,
        pop/01,
        create_table/3,
        table_exists/2,
        depth/1,
        dump/1,
        execute/2,
        execute/3,
        transaction/2,
        validate_record/1,
        validate_record/2,
        validate_record_types/1,
        type/2,
        data_type/2]).

-define(DEFAULT_TIMEOUT, (30 * 1000)).
%% -define(POOLNAME, boss_db_pool).

%%----------------------
%% 由于内部需要维护多个pool所以，key标示pool的名称
%%----------------------
start(Key,Options) ->
    AdapterName = proplists:get_value(adapter, Options, mock),
    Adapter = list_to_atom(lists:concat(["boss_db_adapter_", AdapterName])),
    Adapter:start(Options),
    lists:foldr(fun(ShardOptions, Acc) ->
                case proplists:get_value(db_shard_models, ShardOptions, []) of
                    [] -> Acc;
                    _ ->
                        ShardAdapter = case proplists:get_value(db_adapter, ShardOptions) of
                            undefined -> Adapter;
                            ShortName -> list_to_atom(lists:concat(["boss_db_adapter_", ShortName]))
                        end,
                        ShardAdapter:start(ShardOptions ++ Options),
                        Acc
                end
        end, [], proplists:get_value(shards, Options, [])),
    boss_db_sup_2:start_link(Key,Options).

stop() ->
    ok.

db_call(Key,Msg) ->
    case erlang:get(boss_db_transaction_info) of
        undefined ->
            boss_pool:call(Key, Msg, ?DEFAULT_TIMEOUT);
        State ->
            {reply, Reply, _} = boss_db_controller:handle_call(Msg, self(), State),
            Reply
    end.

%% @doc Apply migrations from list [{Tag, Fun}]
%% currently runs all migrations 'up'
migrate(Key,Migrations) when is_list(Migrations) ->
    %% 1. Do we have a migrations table?  If not, create it.
    case table_exists(Key,schema_migrations) of
        false ->
            ok = create_table(Key,schema_migrations, [{id, auto_increment, []},
                                                  {version, string, [not_null]},
                                                  {migrated_at, datetime, []}]);
        _ ->
            noop
    end,
    %% 2. Get all the current migrations from it.
    DoneMigrations = db_call(Key,{get_migrations_table}),
    DoneMigrationTags = [list_to_atom(binary_to_list(Tag)) ||
                            {_Id, Tag, _MigratedAt} <- DoneMigrations],
    %% 3. Run the ones that are not in this list.
    transaction(Key,fun() ->
			[migrate(Key,{Tag, Fun}, up) ||
			    {Tag, Fun} <- Migrations,
			    not lists:member(Tag, DoneMigrationTags)]
		end).

%% @doc Run database migration {Tag, Fun} in Direction
migrate(Key,{Tag, Fun}, Direction) ->
    io:format("Running migration: ~p ~p~n", [Tag, Direction]),
    Fun(Direction),
    db_call(Key,{migration_done, Tag, Direction}).

%% @spec find(Id::string()) -> Value | {error, Reason}
%% @doc Find a BossRecord with the specified `Id' (e.g. "employee-42") or a value described
%% by a dot-separated path (e.g. "employee-42.manager.name").
find(_Pool_key,"") -> undefined;
find(Pool_key,Key) when is_list(Key) ->
    %% [IdToken|Rest] = string:tokens(Key, "."),
    %% case db_call({find, IdToken}) of
    %%     undefined -> undefined;
    %%     {error, Reason} -> {error, Reason};
    %%     BossRecord -> BossRecord:get(string:join(Rest, "."))
    %% end;
    IdToken = Key,
     case db_call(Pool_key,{find, IdToken}) of
         undefined -> undefined;
         {error, Reason} -> {error, Reason};
        BossRecord -> BossRecord
     end;

find(Pool_key,Key) when is_list(Key) ->
    [IdToken|Rest] = string:tokens(Key, "."),
    case db_call(Pool_key,{find, IdToken}) of
        undefined -> undefined;
        {error, Reason} -> {error, Reason};
        BossRecord -> BossRecord:get(string:join(Rest, "."))
    end;
find(_Pool_key,_) ->
    {error, invalid_id}.

%% @spec find(Type::atom(), Conditions) -> [ BossRecord ]
%% @doc Query for BossRecords. Returns all BossRecords of type
%% `Type' matching all of the given `Conditions'
find(Pool_key,Type, Conditions) ->
    find(Pool_key,Type, Conditions, []).

%% @spec find(Type::atom(), Conditions, Options::proplist()) -> [ BossRecord ]
%% @doc Query for BossRecords. Returns BossRecords of type
%% `Type' matching all of the given `Conditions'. Options may include
%% `limit' (maximum number of records to return), `offset' (number of records
%% to skip), `order_by' (attribute to sort on), `descending' (whether to
%% sort the values from highest to lowest), and `include' (list of belongs_to
%% associations to pre-cache)
find(Pool_key,Type, Conditions, Options) ->
    Max = proplists:get_value(limit, Options, all),
    Skip = proplists:get_value(offset, Options, 0),
    Sort = proplists:get_value(order_by, Options, id),
    SortOrder = case proplists:get_value(descending, Options) of
        true -> descending;
        _ -> ascending
    end,
    Include = proplists:get_value(include, Options, []),
    db_call(Pool_key,{find, Type, normalize_conditions(Conditions), Max, Skip, Sort, SortOrder, Include}).

%% @spec find_first( Type::atom() ) -> Record | undefined
%% @doc Query for the first BossRecord of type `Type'.
find_first(Pool_key,Type) ->
    return_one(find(Pool_key,Type, [], [{limit, 1}])).

%% @spec find_first( Type::atom(), Conditions ) -> Record | undefined
%% @doc Query for the first BossRecord of type `Type' matching all of the given `Conditions'
find_first(Pool_key,Type, Conditions) ->
    return_one(find(Pool_key,Type, Conditions, [{limit, 1}])).

%% @spec find_first( Type::atom(), Conditions, Sort::atom() ) -> Record | undefined
%% @doc Query for the first BossRecord of type `Type' matching all of the given `Conditions',
%% sorted on the attribute `Sort'.
find_first(Pool_key,Type, Conditions, Sort) ->
    return_one(find(Pool_key,Type, Conditions, [{limit, 1}, {order_by, Sort}])).

%% @spec find_last( Type::atom() ) -> Record | undefined
%% @doc Query for the last BossRecord of type `Type'.
find_last(Pool_key,Type) ->
    return_one(find(Pool_key,Type, [], [{limit, 1}, descending])).

%% @spec find_last( Type::atom(), Conditions ) -> Record | undefined
%% @doc Query for the last BossRecord of type `Type' matching all of the given `Conditions'
find_last(Pool_key,Type, Conditions) ->
    return_one(find(Pool_key,Type, Conditions, [{limit, 1}, descending])).

%% @spec find_last( Type::atom(), Conditions, Sort ) -> Record | undefined
%% @doc Query for the last BossRecord of type `Type' matching all of the given `Conditions'
find_last(Pool_key,Type, Conditions, Sort) ->
    return_one(find(Pool_key,Type, Conditions, [{limit, 1}, {order_by, Sort}, descending])).

%% @spec count( Type::atom() ) -> integer()
%% @doc Count the number of BossRecords of type `Type' in the database.
count(Pool_key,Type) ->
    count(Pool_key,Type, []).

%% @spec count( Type::atom(), Conditions ) -> integer()
%% @doc Count the number of BossRecords of type `Type' in the database matching
%% all of the given `Conditions'.
count(Pool_key,Type, Conditions) ->
    db_call(Pool_key,{count, Type, normalize_conditions(Conditions)}).

%% @spec counter( Id::string() ) -> integer()
%% @doc Treat the record associated with `Id' as a counter and return its value.
%% Returns 0 if the record does not exist, so to reset a counter just use
%% "delete".
counter(Pool_key,Key) ->
    db_call(Pool_key,{counter, Key}).

%% @spec incr( Id::string() ) -> integer()
%% @doc Treat the record associated with `Id' as a counter and atomically increment its value by 1.
incr(Pool_key,Key) ->
    incr(Pool_key,Key, 1).

%% @spec incr( Id::string(), Increment::integer() ) -> integer()
%% @doc Treat the record associated with `Id' as a counter and atomically increment its value by `Increment'.
incr(Pool_key,Key, Count) ->
    db_call(Pool_key,{incr, Key, Count}).

%% @spec delete( Id::string() ) -> ok | {error, Reason}
%% @doc Delete the BossRecord with the given `Id'.
delete(Pool_key,Key) ->
    case boss_db_2:find(Pool_key,Key) of
        undefined ->
            {error, not_found};
        AboutToDelete ->
            case boss_record_lib:run_before_delete_hooks(AboutToDelete) of
                ok ->
                    Result = db_call(Pool_key,{delete, Key}),
                    case Result of
                        ok ->
                            boss_news:deleted(Key, AboutToDelete:attributes()),
                            ok;
                        _ ->
                            Result
                    end;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

push(Pool_key) ->
    db_call(Pool_key,push).

pop(Pool_key) ->
    db_call(Pool_key,pop).

depth(Pool_key) ->
    db_call(Pool_key,depth).

dump(Pool_key) ->
    db_call(Pool_key,dump).

%% @spec create_table ( TableName::string(), TableDefinition ) -> ok | {error, Reason}
%% @doc Create a table based on TableDefinition
create_table(Pool_key,TableName, TableDefinition) ->
    db_call(Pool_key,{create_table, TableName, TableDefinition}).

table_exists(Pool_key,TableName) ->
    db_call(Pool_key,{table_exists, TableName}).

%% @spec execute( Commands::iolist() ) -> RetVal
%% @doc Execute raw database commands on SQL databases
execute(Pool_key,Commands) ->
    db_call(Pool_key,{execute, Commands}).

%% @spec execute( Commands::iolist(), Params::list() ) -> RetVal
%% @doc Execute database commands with interpolated parameters on SQL databases
execute(Pool_key,Commands, Params) ->
    db_call(Pool_key,{execute, Commands, Params}).

%% @spec transaction( TransactionFun::function() ) -> {atomic, Result} | {aborted, Reason}
%% @doc Execute a fun inside a transaction.
transaction(Pool_key,TransactionFun) ->
    Worker = poolboy:checkout(Pool_key),
    State = gen_server:call(Worker, state, ?DEFAULT_TIMEOUT),
    lager:info("boss_db_2_transaction_1"),
    put(boss_db_transaction_info, State),
    lager:info("boss_db_2_transaction_2"),
    {reply, Reply, _} = boss_db_controller:handle_call({transaction, TransactionFun}, self(), State),
    lager:info("boss_db_2_transaction_3"),
    put(boss_db_transaction_info, undefined),
    lager:info("boss_db_2_transaction_4"),
    poolboy:checkin(Pool_key, Worker),
    lager:info("boss_db_2_transaction_5"),
    Reply.
    %% db_call(Pool_key,{transaction,TransactionFun}).

%% @spec save_record( BossRecord ) -> {ok, SavedBossRecord} | {error, [ErrorMessages]}
%% @doc Save (that is, create or update) the given BossRecord in the database.
%% Performs validation first; see `validate_record/1'.
save_record(Pool_key,Record) ->
    case validate_record(Record) of
        ok ->
            RecordId = Record:id(),
            {IsNew, OldRecord} = if
                                     RecordId =:= 'id' ->
                                         {true, Record};
                                     true ->
                                         case find(Pool_key,RecordId) of
                                             {error, _Reason} -> {true, Record};
                                             undefined -> {true, Record};
                                             FoundOldRecord -> {false, FoundOldRecord}
                                         end
                                 end,
                                                % Action dependent valitation
            case validate_record(Record, IsNew) of
                ok ->
                    HookResult = case boss_record_lib:run_before_hooks(Record, IsNew) of
                                     ok -> {ok, Record};
                                     {ok, Record1} -> {ok, Record1};
                                     {error, Reason} -> {error, Reason}
                                 end,
                    case HookResult of
                        {ok, PossiblyModifiedRecord} ->
                            case db_call(Pool_key,{save_record, PossiblyModifiedRecord}) of
                                {ok, SavedRecord} ->
                                    %% A = 'jacob_001.book':module_info(),
                                    %% lager:info("boss_news_controller_handle_call_update_1:~p",[A]),
                                    boss_record_lib:run_after_hooks(OldRecord, SavedRecord, IsNew),
                                    {ok, SavedRecord};
                                Err -> Err
                            end;
                        Err -> Err
                    end;
                Err -> Err
            end;
        Err -> Err
    end.

%% @spec validate_record( BossRecord ) -> ok | {error, [ErrorMessages]}
%% @doc Validate the given BossRecord without saving it in the database.
%% `ErrorMessages' are generated from the list of tests returned by the BossRecord's
%% `validation_tests/0' function (if defined). The returned list should consist of
%% `{TestFunction, ErrorMessage}' tuples, where `TestFunction' is a fun of arity 0
%% that returns `true' if the record is valid or `false' if it is invalid.
%% `ErrorMessage' should be a (constant) string which will be included in `ErrorMessages'
%% if the `TestFunction' returns `false' on this particular BossRecord.
validate_record(Record) ->
    Type = element(1, Record),
    Errors1 = case validate_record_types(Record) of
        ok -> [];
        {error, Errors} -> Errors
    end,
    Errors2 = case Errors1 of
        [] ->
            case erlang:function_exported(Type, validation_tests, 1) of
                true -> [String || {TestFun, String} <- Record:validation_tests(), not TestFun()];
                false -> []
            end;
        _ -> Errors1
    end,
    case length(Errors2) of
        0 -> ok;
        _ -> {error, Errors2}
    end.

%% @spec validate_record( BossRecord, IsNew ) -> ok | {error, [ErrorMessages]}
%% @doc Validate the given BossRecord without saving it in the database.
%% `ErrorMessages' are generated from the list of tests returned by the BossRecord's
%% `validation_tests/1' function (if defined), where parameter is atom() `on_create | on_update'.
%% The returned list should consist of `{TestFunction, ErrorMessage}' tuples,
%% where `TestFunction' is a fun of arity 0
%% that returns `true' if the record is valid or `false' if it is invalid.
%% `ErrorMessage' should be a (constant) string which will be included in `ErrorMessages'
%% if the `TestFunction' returns `false' on this particular BossRecord.
validate_record(Record, IsNew) ->
    Type = element(1, Record),
    Action = case IsNew of
                   true -> on_create;
                   false -> on_update
               end,
    Errors = case erlang:function_exported(Type, validation_tests, 2) of
                 % makes Action optional
                 true -> [String || {TestFun, String} <- try Record:validation_tests(Action)
                                                         catch error:function_clause -> []
                                                         end,
                                    not TestFun()];
                 false -> []
             end,
    case length(Errors) of
        0 -> ok;
        _ -> {error, Errors}
    end.

%% @spec validate_record_types( BossRecord ) -> ok | {error, [ErrorMessages]}
%% @doc Validate the parameter types of the given BossRecord without saving it
%% to the database.
validate_record_types(Record) ->
    Errors = lists:foldl(fun
            ({Attr, Type}, Acc) ->
                case Attr of
                  id -> Acc;
                  _  ->
                    Data = Record:Attr(),
                        lager:info("boss_db_validate_record_types_1:~p,~p",[Data,Type]),
                    GreatSuccess = case {Data, Type} of
                        {undefined, _} ->
                            true;
                        {Data, string} when is_list(Data) ->
                            true;
                        {Data, binary} when is_binary(Data) ->
                            true;
                        {{{D1, D2, D3}, {T1, T2, T3}}, datetime} when is_integer(D1), is_integer(D2), is_integer(D3),
                                                                      is_integer(T1), is_integer(T2), is_integer(T3) ->
                            true;
                        {{D1, D2, D3}, date} when is_integer(D1), is_integer(D2), is_integer(D3) ->
                            true;
                        {Data, integer} when is_integer(Data) ->
                            true;
                        {Data, float} when is_float(Data) ->
                            true;
                        {Data, boolean} when is_boolean(Data) ->
                            true;
                        {{N1, N2, N3}, timestamp} when is_integer(N1), is_integer(N2), is_integer(N3) ->
                            true;
                        {Data, atom} when is_atom(Data) ->
                            true;
                        {_Data, Type} ->
                            false
                    end,
                    if
                        GreatSuccess ->
                            Acc;
                        true ->
                            [lists:concat(["Invalid data type for ", Attr])|Acc]
                    end
                  end
        end, [], Record:attribute_types()),
    case Errors of
        [] -> ok;
        _ -> {error, Errors}
    end.

%% @spec type( Id::string() ) -> Type::atom()
%% @doc Returns the type of the BossRecord with `Id', or `undefined' if the record does not exist.
type(Pool_key,Key) ->
    case find(Pool_key,Key) of
        undefined -> undefined;
        Record -> element(1, Record)
    end.

data_type(_, _Val) when is_float(_Val) ->
    "float";
data_type(_, _Val) when is_binary(_Val) ->
    "binary";
data_type(_, _Val) when is_integer(_Val) ->
    "integer";
data_type(_, _Val) when is_tuple(_Val) ->
    "datetime";
data_type(_, _Val) when is_boolean(_Val) ->
    "boolean";
data_type(_, null) ->
    "null";
data_type(_, undefined) ->
    "null";
data_type('id', _) ->
    "id";
data_type(Key, Val) when is_list(Val) ->
    case lists:suffix("_id", atom_to_list(Key)) of
        true -> "foreign_id";
        false -> "string"
    end.

normalize_conditions(Conditions) ->
    normalize_conditions(Conditions, []).

normalize_conditions([], Acc) ->
    lists:reverse(Acc);
normalize_conditions([Key, Operator, Value|Rest], Acc) when is_atom(Key), is_atom(Operator) ->
    normalize_conditions(Rest, [{Key, Operator, Value}|Acc]);
normalize_conditions([{Key, Value}|Rest], Acc) when is_atom(Key) ->
    normalize_conditions(Rest, [{Key, 'equals', Value}|Acc]);
normalize_conditions([{Key, 'eq', Value}|Rest], Acc) when is_atom(Key) ->
    normalize_conditions(Rest, [{Key, 'equals', Value}|Acc]);
normalize_conditions([{Key, 'ne', Value}|Rest], Acc) when is_atom(Key) ->
    normalize_conditions(Rest, [{Key, 'not_equals', Value}|Acc]);
normalize_conditions([{Key, Operator, Value}|Rest], Acc) when is_atom(Key), is_atom(Operator) ->
    normalize_conditions(Rest, [{Key, Operator, Value}|Acc]);
normalize_conditions([{Key, Operator, Value, Options}|Rest], Acc) when is_atom(Key), is_atom(Operator), is_list(Options) ->
    normalize_conditions(Rest, [{Key, Operator, Value, Options}|Acc]).

return_one(Result) ->
    case Result of
        [] -> undefined;
        [Record] -> Record
    end.

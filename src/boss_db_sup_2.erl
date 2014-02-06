-module(boss_db_sup_2).
-author('emmiller@gmail.com').

-behaviour(supervisor).

-export([start_link/1, start_link/2]).

-export([init/1]).

start_link(Pool_key) ->
    start_link(Pool_key,[]).

start_link(Key,StartArgs) ->
    Name = love_misc:to_atom("db_sup_2" ++ love_misc:to_list(Key)),
    supervisor:start_link({local,Name}, ?MODULE, [Key,StartArgs]).

init([Key,StartArgs]) ->
    %% Args = [{name, {local, boss_db_pool}},
    Args = [{name, {local, Key}},
        {worker_module, boss_db_controller},
        {size, 10}, {max_overflow, 100}|StartArgs],
    PoolSpec = {db_controller, {poolboy, start_link, [Args,StartArgs]}, permanent, 2000, worker, [poolboy]},
    {ok, {{one_for_one, 10, 10}, [PoolSpec]}}.

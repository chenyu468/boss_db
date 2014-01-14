-module(boss_db_sup_2).
-author('emmiller@gmail.com').

-behaviour(supervisor).

-export([start_link/1, start_link/2]).

-export([init/1]).

start_link(Pool_key) ->
    start_link(Pool_key,[]).

start_link(Key,StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Key,StartArgs]).

init([Key,StartArgs]) ->
    %% Args = [{name, {local, boss_db_pool}},
    Args = [{name, {local, Key}},
        {worker_module, boss_db_controller},
        {size, 5}, {max_overflow, 10}|StartArgs],
    PoolSpec = {db_controller, {poolboy, start_link, [Args]}, permanent, 2000, worker, [poolboy]},
    {ok, {{one_for_one, 10, 10}, [PoolSpec]}}.

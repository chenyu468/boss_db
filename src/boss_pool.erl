-module(boss_pool).
-compile([{parse_transform, lager_transform}]).
-export([call/2, call/3]).

call(Pool, Msg) ->
    lager:info("boss_pool_call_1:~p,~p",[Pool,Msg]),
    Worker = poolboy:checkout(Pool),
    Reply = gen_server:call(Worker, Msg),
    poolboy:checkin(Pool, Worker),
    lager:info("boss_pool_call_2"),
    Reply.

call(Pool, Msg, Timeout) ->
    lager:info("boss_pool_call_4:~p,~p,~p",[Pool,Msg,Timeout]),
    Worker = poolboy:checkout(Pool),
    Reply = gen_server:call(Worker, Msg, Timeout),
    poolboy:checkin(Pool, Worker),
    lager:info("boss_pool_call_5"),
    Reply.

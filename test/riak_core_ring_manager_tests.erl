-module(riak_core_ring_manager_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).
-define(TEST_RINGDIR, "ring_manager_eunit").
-define(TMP_CLUSTER_NAME,  ("test")).

ring_reload_test_() ->
    {foreach,
        fun() ->
            Core_Settings = [{ring_creation_size, 4},
                            {ring_state_dir, ?TEST_RINGDIR},
                            {cluster_name, ?TMP_CLUSTER_NAME}],
            helper_set_core_settings(Core_Settings),
             % Ensure our test rinf directory exists and is cleared from previous tests
            ok = filelib:ensure_dir(?TEST_RINGDIR ++ "/"),
            helper_delete_files_in(?TEST_RINGDIR)
        end,
        fun(_) ->
             % Delete past ring files from previous tests
            helper_delete_files_in(?TEST_RINGDIR),
            Core_Settings = [ring_creation_size, ring_state_dir, cluster_name],
            helper_unset_core_settings(Core_Settings)
        end,
        [
            fun test_reload_ring_file_loads_previous_if_ringfile_corrupt/0,
            fun test_reload_ring_file_fails_if_all_ringfile_corrupt/0,
            fun test_reload_ring_file_generates_new_one_if_no_ringfiles/0,
            fun test_reload_ring_file_throws_error_enoent_if_ringfile_dir_missing/0
        ]
    }.

test_reload_ring_file_loads_previous_if_ringfile_corrupt() ->
    GenR = fun(Name) -> riak_core_ring:fresh(64, Name) end,

    % Create a ring file with an old date time
    DateTimePast = {{2010,7,13}, {14,59,16}},
    ok = riak_core_ring_manager:do_write_ringfile_with_datetime(GenR(?TMP_CLUSTER_NAME), ?TEST_RINGDIR, DateTimePast),
    riak_core_ring_manager:reload_ring(live),

    % Create new ring with a current timestamp
    ok = riak_core_ring_manager:do_write_ringfile(GenR(?TMP_CLUSTER_NAME)),
    helper_corrupt_latest_ring_file(),

    riak_core_ring_manager:reload_ring(live),
    ok.

test_reload_ring_file_fails_if_all_ringfile_corrupt() ->
    GenR = fun(Name) -> riak_core_ring:fresh(64, Name) end,

    % Create a ring file with an old date time
    DateTimePast = {{2010,7,13}, {14,59,16}},
    ok = riak_core_ring_manager:do_write_ringfile_with_datetime(GenR(?TMP_CLUSTER_NAME), ?TEST_RINGDIR, DateTimePast),

    % Now corrupt that ring file
    helper_corrupt_latest_ring_file(),

    % attempt to load ring file, call should fail as there are no valid ring files
    ?assertError(badarg, riak_core_ring_manager:reload_ring(live)),
    ok.

test_reload_ring_file_generates_new_one_if_no_ringfiles() ->
    % All ring files deleted by test setup
    % Reloading ring causes fresh ring to be created
    riak_core_ring_manager:reload_ring(live),
    ok.

test_reload_ring_file_throws_error_enoent_if_ringfile_dir_missing() ->
    file:del_dir(?TEST_RINGDIR),
    % Ensure exception is thrown with a corect error
    ?assertThrow({error,enoent}, riak_core_ring_manager:reload_ring(live)),
    ok.

%%
%% Helper functions useful for testing
%%
helper_set_core_settings(Core_Settings) ->
    [begin
         put({?MODULE,AppKey}, app_helper:get_env(riak_core, AppKey)),
         ok = application:set_env(riak_core, AppKey, Val)
     end || {AppKey, Val} <- Core_Settings],
     Core_Settings.

helper_unset_core_settings(Core_Settings) ->
    [ok = application:set_env(riak_core, AppKey, get({?MODULE, AppKey})) || {AppKey} <- Core_Settings].

helper_corrupt_latest_ring_file() ->
    {ok, LatestRingFile} = riak_core_ring_manager:find_latest_ringfile(),
    helper_corrupt_ring_file(LatestRingFile).

helper_corrupt_ring_file(File) ->
    Size = filelib:file_size(File),
    {ok, IODev} = file:open(File, [write]),
    file:position(IODev, Size-1),
    file:truncate(IODev),
    file:close(IODev).

helper_delete_files_in(Dir) ->
    case filelib:is_dir(Dir) of
        true ->
            {ok, ChildrenNames} = file:list_dir(Dir),
            Children = lists:map(fun(F) -> filename:join(Dir, F) end, ChildrenNames),
            {FilesOnly, _} = lists:partition(fun filelib:is_file/1, Children),
            lists:foreach(fun(F) -> ok = file:delete(F) end, FilesOnly);
        false ->
            ok
    end.

-endif.

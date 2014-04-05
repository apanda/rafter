-record(rafter_opts, {state_machine = rafter_backend_echo :: atom(),
                      logdir :: string(), 
                      clean_start = false :: boolean(), 
                      election_timer :: 'undefined' | integer() }).

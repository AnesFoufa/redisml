(* Configuration constants *)

(* Replication *)
let master_repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
let master_repl_offset = 0

(* Timeouts (in seconds or milliseconds as noted) *)
let replica_connection_delay_s = 0.05
let replica_response_delay_s = 0.01
let min_wait_timeout_ms = 1000

(* Buffer sizes *)
let protocol_read_buffer = 4096
let replica_response_buffer = 1024
let initial_storage_capacity = 1024

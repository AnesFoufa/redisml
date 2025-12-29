(* Database unit tests *)

open Alcotest
open Codecrafters_redis

(* Helper to check if string contains substring *)
let contains_substring s sub =
  try
    let _ = Str.search_forward (Str.regexp_string sub) s 0 in
    true
  with Not_found -> false

(* Role Detection Tests *)
let test_master_role () =
  let config = Config.default in
  let db = Database.create config in
  check bool "should be master" false (Database.is_replica db)

let test_replica_role () =
  let config = { Config.default with replicaof = Some ("localhost", 6379) } in
  let db = Database.create config in
  check bool "should be replica" true (Database.is_replica db)

(* Command Execution Tests *)
let test_ping () =
  let db = Database.create Config.default in
  let result = Database.execute_command Command.Ping db ~current_time:1000.0 in
  check string "PING returns PONG"
    (Resp.serialize (Resp.SimpleString "PONG"))
    (Resp.serialize result)

let test_echo () =
  let db = Database.create Config.default in
  let msg = Resp.BulkString "hello" in
  let result = Database.execute_command (Command.Echo msg) db ~current_time:1000.0 in
  check string "ECHO returns message"
    (Resp.serialize msg)
    (Resp.serialize result)

let test_set_get () =
  let db = Database.create Config.default in
  let set_cmd = Command.Set {
    key = "mykey";
    value = Resp.BulkString "myvalue";
    expiry_ms = None
  } in
  let _ = Database.execute_command set_cmd db ~current_time:1000.0 in
  let get_cmd = Command.Get "mykey" in
  let result = Database.execute_command get_cmd db ~current_time:1000.0 in
  check string "GET returns SET value"
    (Resp.serialize (Resp.BulkString "myvalue"))
    (Resp.serialize result)

let test_get_nonexistent () =
  let db = Database.create Config.default in
  let get_cmd = Command.Get "nonexistent" in
  let result = Database.execute_command get_cmd db ~current_time:1000.0 in
  check string "GET nonexistent returns Null"
    (Resp.serialize Resp.Null)
    (Resp.serialize result)

(* INFO Command Tests *)
let test_info_master () =
  let db = Database.create Config.default in
  let result = Database.execute_command Command.InfoReplication db ~current_time:1000.0 in
  match result with
  | Resp.BulkString info ->
      check bool "contains role:master"
        true (contains_substring info "role:master")
  | _ -> fail "Expected BulkString"

let test_info_replica () =
  let config = { Config.default with replicaof = Some ("localhost", 6379) } in
  let db = Database.create config in
  let result = Database.execute_command Command.InfoReplication db ~current_time:1000.0 in
  match result with
  | Resp.BulkString info ->
      check bool "contains role:slave"
        true (contains_substring info "role:slave")
  | _ -> fail "Expected BulkString"

(* REPLCONF Tests *)
let test_replconf_ok () =
  let db = Database.create Config.default in
  let cmd = Command.Replconf (Command.ReplconfListeningPort 6379) in
  let result = Database.execute_command cmd db ~current_time:1000.0 in
  check string "REPLCONF returns OK"
    (Resp.serialize (Resp.SimpleString "OK"))
    (Resp.serialize result)

let test_replconf_getack_on_replica () =
  let config = { Config.default with replicaof = Some ("localhost", 6379) } in
  let db = Database.create config in
  let cmd = Command.Replconf Command.ReplconfGetAck in
  let result = Database.execute_command cmd db ~current_time:1000.0 in
  match result with
  | Resp.Array [
      Resp.BulkString "REPLCONF";
      Resp.BulkString "ACK";
      Resp.BulkString offset
    ] ->
      check string "initial offset is 0" "0" offset
  | _ -> fail "Expected REPLCONF ACK array"

(* Offset Tracking Tests *)
let test_increment_offset () =
  let config = { Config.default with replicaof = Some ("localhost", 6379) } in
  let db = Database.create config in
  Database.increment_offset db 100;
  let cmd = Command.Replconf Command.ReplconfGetAck in
  let result = Database.execute_command cmd db ~current_time:1000.0 in
  match result with
  | Resp.Array [_; _; Resp.BulkString offset] ->
      check string "offset incremented" "100" offset
  | _ -> fail "Expected ACK with offset"

let test_increment_offset_multiple_times () =
  let config = { Config.default with replicaof = Some ("localhost", 6379) } in
  let db = Database.create config in
  Database.increment_offset db 50;
  Database.increment_offset db 75;
  let cmd = Command.Replconf Command.ReplconfGetAck in
  let result = Database.execute_command cmd db ~current_time:1000.0 in
  match result with
  | Resp.Array [_; _; Resp.BulkString offset] ->
      check string "offset accumulated" "125" offset
  | _ -> fail "Expected ACK with offset"

(* Command Propagation Tests *)
let test_should_propagate_set_on_master () =
  let db = Database.create Config.default in
  let cmd = Command.Set {
    key = "key";
    value = Resp.BulkString "val";
    expiry_ms = None
  } in
  check bool "SET should propagate on master"
    true (Database.should_propagate_command db cmd)

let test_should_not_propagate_set_on_replica () =
  let config = { Config.default with replicaof = Some ("localhost", 6379) } in
  let db = Database.create config in
  let cmd = Command.Set {
    key = "key";
    value = Resp.BulkString "val";
    expiry_ms = None
  } in
  check bool "SET should not propagate on replica"
    false (Database.should_propagate_command db cmd)

let test_should_not_propagate_get () =
  let db = Database.create Config.default in
  let cmd = Command.Get "key" in
  check bool "GET should not propagate"
    false (Database.should_propagate_command db cmd)

let test_should_not_propagate_ping () =
  let db = Database.create Config.default in
  check bool "PING should not propagate"
    false (Database.should_propagate_command db Command.Ping)

(* PSYNC Tests *)
let test_psync_response () =
  let db = Database.create Config.default in
  let cmd = Command.Psync { replication_id = "?"; offset = -1 } in
  let result = Database.execute_command cmd db ~current_time:1000.0 in
  match result with
  | Resp.SimpleString s ->
      check bool "PSYNC returns FULLRESYNC"
        true (contains_substring s "FULLRESYNC")
  | _ -> fail "Expected SimpleString"

(* CONFIG Tests *)
let test_config_get_dir_with_value () =
  let config = { Config.default with dir = Some "/tmp/redis" } in
  let db = Database.create config in
  let cmd = Command.ConfigGet Command.Dir in
  let result = Database.execute_command cmd db ~current_time:1000.0 in
  check string "CONFIG GET dir returns array with value"
    (Resp.serialize (Resp.Array [Resp.BulkString "dir"; Resp.BulkString "/tmp/redis"]))
    (Resp.serialize result)

let test_config_get_dir_no_value () =
  let config = Config.default in
  let db = Database.create config in
  let cmd = Command.ConfigGet Command.Dir in
  let result = Database.execute_command cmd db ~current_time:1000.0 in
  check string "CONFIG GET dir with no value returns array with empty string"
    (Resp.serialize (Resp.Array [Resp.BulkString "dir"; Resp.BulkString ""]))
    (Resp.serialize result)

let test_config_get_dbfilename_with_value () =
  let config = { Config.default with dbfilename = Some "dump.rdb" } in
  let db = Database.create config in
  let cmd = Command.ConfigGet Command.Dbfilename in
  let result = Database.execute_command cmd db ~current_time:1000.0 in
  check string "CONFIG GET dbfilename returns array with value"
    (Resp.serialize (Resp.Array [Resp.BulkString "dbfilename"; Resp.BulkString "dump.rdb"]))
    (Resp.serialize result)

let test_config_get_dbfilename_no_value () =
  let config = Config.default in
  let db = Database.create config in
  let cmd = Command.ConfigGet Command.Dbfilename in
  let result = Database.execute_command cmd db ~current_time:1000.0 in
  check string "CONFIG GET dbfilename with no value returns array with empty string"
    (Resp.serialize (Resp.Array [Resp.BulkString "dbfilename"; Resp.BulkString ""]))
    (Resp.serialize result)

(* Test Suite *)
let () =
  run "Database" [
    "role", [
      test_case "master role" `Quick test_master_role;
      test_case "replica role" `Quick test_replica_role;
    ];
    "commands", [
      test_case "PING" `Quick test_ping;
      test_case "ECHO" `Quick test_echo;
      test_case "SET/GET" `Quick test_set_get;
      test_case "GET nonexistent" `Quick test_get_nonexistent;
    ];
    "info", [
      test_case "INFO on master" `Quick test_info_master;
      test_case "INFO on replica" `Quick test_info_replica;
    ];
    "replconf", [
      test_case "REPLCONF OK" `Quick test_replconf_ok;
      test_case "GETACK on replica" `Quick test_replconf_getack_on_replica;
    ];
    "offset", [
      test_case "increment offset" `Quick test_increment_offset;
      test_case "increment offset multiple" `Quick test_increment_offset_multiple_times;
    ];
    "propagation", [
      test_case "propagate SET on master" `Quick test_should_propagate_set_on_master;
      test_case "no propagate SET on replica" `Quick test_should_not_propagate_set_on_replica;
      test_case "no propagate GET" `Quick test_should_not_propagate_get;
      test_case "no propagate PING" `Quick test_should_not_propagate_ping;
    ];
    "psync", [
      test_case "PSYNC response" `Quick test_psync_response;
    ];
    "config", [
      test_case "CONFIG GET dir with value" `Quick test_config_get_dir_with_value;
      test_case "CONFIG GET dir no value" `Quick test_config_get_dir_no_value;
      test_case "CONFIG GET dbfilename with value" `Quick test_config_get_dbfilename_with_value;
      test_case "CONFIG GET dbfilename no value" `Quick test_config_get_dbfilename_no_value;
    ];
  ]

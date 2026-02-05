(* Database unit tests *)

open Alcotest
open Codecrafters_redis

(* Helper to check if string contains substring *)
let contains_substring s sub =
  try
    let _ = Str.search_forward (Str.regexp_string sub) s 0 in
    true
  with Not_found -> false

(* RESP command builders *)
let ping_cmd = Resp.Array [Resp.BulkString "PING"]
let echo_cmd msg = Resp.Array [Resp.BulkString "ECHO"; msg]
let get_cmd key = Resp.Array [Resp.BulkString "GET"; Resp.BulkString key]
let set_cmd key value = Resp.Array [Resp.BulkString "SET"; Resp.BulkString key; Resp.BulkString value]
let info_replication_cmd = Resp.Array [Resp.BulkString "INFO"; Resp.BulkString "replication"]
let replconf_port_cmd port = Resp.Array [Resp.BulkString "REPLCONF"; Resp.BulkString "listening-port"; Resp.BulkString (string_of_int port)]
let replconf_getack_cmd = Resp.Array [Resp.BulkString "REPLCONF"; Resp.BulkString "GETACK"; Resp.BulkString "*"]
let psync_cmd id offset = Resp.Array [Resp.BulkString "PSYNC"; Resp.BulkString id; Resp.BulkString (string_of_int offset)]
let config_get_cmd param = Resp.Array [Resp.BulkString "CONFIG"; Resp.BulkString "GET"; Resp.BulkString param]
let keys_cmd pattern = Resp.Array [Resp.BulkString "KEYS"; Resp.BulkString pattern]

(* Role Detection Tests *)
let test_master_role () =
  let config = Config.default in
  let db = Test_helpers.create_db config in
  check bool "should be master" true (Test_helpers.is_master db)

let test_replica_role () =
  let config = { Config.default with replicaof = Some ("localhost", 6379) } in
  let db = Test_helpers.create_db config in
  check bool "should be replica" true (Test_helpers.is_replica db)

(* Command Execution Tests *)
let test_ping () =
  let db = Test_helpers.create_db Config.default in
  let result = Test_helpers.exec_command ping_cmd db ~current_time:1000.0 in
  check string "PING returns PONG"
    (Resp.serialize (Resp.SimpleString "PONG"))
    (Resp.serialize result)

let test_echo () =
  let db = Test_helpers.create_db Config.default in
  let msg = Resp.BulkString "hello" in
  let result = Test_helpers.exec_command (echo_cmd msg) db ~current_time:1000.0 in
  check string "ECHO returns message"
    (Resp.serialize msg)
    (Resp.serialize result)

let test_set_get () =
  let db = Test_helpers.create_db Config.default in
  let _ = Test_helpers.exec_command (set_cmd "mykey" "myvalue") db ~current_time:1000.0 in
  let result = Test_helpers.exec_command (get_cmd "mykey") db ~current_time:1000.0 in
  check string "GET returns SET value"
    (Resp.serialize (Resp.BulkString "myvalue"))
    (Resp.serialize result)

let test_get_nonexistent () =
  let db = Test_helpers.create_db Config.default in
  let result = Test_helpers.exec_command (get_cmd "nonexistent") db ~current_time:1000.0 in
  check string "GET nonexistent returns Null"
    (Resp.serialize Resp.Null)
    (Resp.serialize result)

(* INFO Command Tests *)
let test_info_master () =
  let db = Test_helpers.create_db Config.default in
  let result = Test_helpers.exec_command info_replication_cmd db ~current_time:1000.0 in
  match result with
  | Resp.BulkString info ->
      check bool "contains role:master"
        true (contains_substring info "role:master")
  | _ -> fail "Expected BulkString"

let test_info_replica () =
  let config = { Config.default with replicaof = Some ("localhost", 6379) } in
  let db = Test_helpers.create_db config in
  let result = Test_helpers.exec_command info_replication_cmd db ~current_time:1000.0 in
  match result with
  | Resp.BulkString info ->
      check bool "contains role:slave"
        true (contains_substring info "role:slave")
  | _ -> fail "Expected BulkString"

(* REPLCONF Tests *)
let test_replconf_ok () =
  let db = Test_helpers.create_db Config.default in
  let result = Test_helpers.exec_command (replconf_port_cmd 6379) db ~current_time:1000.0 in
  check string "REPLCONF returns OK"
    (Resp.serialize (Resp.SimpleString "OK"))
    (Resp.serialize result)

let test_replconf_getack_on_replica () =
  let config = { Config.default with replicaof = Some ("localhost", 6379) } in
  let db = Test_helpers.create_db config in
  let result = Test_helpers.exec_command replconf_getack_cmd db ~current_time:1000.0 in
  match result with
  | Resp.Array [
      Resp.BulkString "REPLCONF";
      Resp.BulkString "ACK";
      Resp.BulkString offset
    ] ->
      check string "initial offset is 0" "0" offset
  | _ -> fail "Expected REPLCONF ACK array"

(* PSYNC Tests *)
let test_psync_response () =
  let db = Test_helpers.create_db Config.default in
  let result = Test_helpers.exec_command (psync_cmd "?" (-1)) db ~current_time:1000.0 in
  match result with
  | Resp.SimpleString s ->
      check bool "PSYNC returns FULLRESYNC"
        true (contains_substring s "FULLRESYNC")
  | _ -> fail "Expected SimpleString"

(* CONFIG Tests *)
let test_config_get_dir_with_value () =
  let config = { Config.default with dir = Some "/tmp/redis" } in
  let db = Test_helpers.create_db config in
  let result = Test_helpers.exec_command (config_get_cmd "dir") db ~current_time:1000.0 in
  check string "CONFIG GET dir returns array with value"
    (Resp.serialize (Resp.Array [Resp.BulkString "dir"; Resp.BulkString "/tmp/redis"]))
    (Resp.serialize result)

let test_config_get_dir_no_value () =
  let config = Config.default in
  let db = Test_helpers.create_db config in
  let result = Test_helpers.exec_command (config_get_cmd "dir") db ~current_time:1000.0 in
  check string "CONFIG GET dir with no value returns array with empty string"
    (Resp.serialize (Resp.Array [Resp.BulkString "dir"; Resp.BulkString ""]))
    (Resp.serialize result)

let test_config_get_dbfilename_with_value () =
  let config = { Config.default with dbfilename = Some "dump.rdb" } in
  let db = Test_helpers.create_db config in
  let result = Test_helpers.exec_command (config_get_cmd "dbfilename") db ~current_time:1000.0 in
  check string "CONFIG GET dbfilename returns array with value"
    (Resp.serialize (Resp.Array [Resp.BulkString "dbfilename"; Resp.BulkString "dump.rdb"]))
    (Resp.serialize result)

let test_config_get_dbfilename_no_value () =
  let config = Config.default in
  let db = Test_helpers.create_db config in
  let result = Test_helpers.exec_command (config_get_cmd "dbfilename") db ~current_time:1000.0 in
  check string "CONFIG GET dbfilename with no value returns array with empty string"
    (Resp.serialize (Resp.Array [Resp.BulkString "dbfilename"; Resp.BulkString ""]))
    (Resp.serialize result)

(* KEYS Tests *)
let test_keys_empty () =
  let db = Test_helpers.create_db Config.default in
  let result = Test_helpers.exec_command (keys_cmd "*") db ~current_time:1000.0 in
  check string "KEYS on empty database"
    (Resp.serialize (Resp.Array []))
    (Resp.serialize result)

let test_keys_with_data () =
  let db = Test_helpers.create_db Config.default in
  let _ = Test_helpers.exec_command (set_cmd "foo" "bar") db ~current_time:1000.0 in
  let _ = Test_helpers.exec_command (set_cmd "baz" "qux") db ~current_time:1000.0 in
  let result = Test_helpers.exec_command (keys_cmd "*") db ~current_time:1000.0 in
  match result with
  | Resp.Array keys ->
      check int "KEYS returns 2 keys" 2 (List.length keys)
  | _ -> fail "Expected array"

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
    "psync", [
      test_case "PSYNC response" `Quick test_psync_response;
    ];
    "config", [
      test_case "CONFIG GET dir with value" `Quick test_config_get_dir_with_value;
      test_case "CONFIG GET dir no value" `Quick test_config_get_dir_no_value;
      test_case "CONFIG GET dbfilename with value" `Quick test_config_get_dbfilename_with_value;
      test_case "CONFIG GET dbfilename no value" `Quick test_config_get_dbfilename_no_value;
    ];
    "keys", [
      test_case "KEYS on empty database" `Quick test_keys_empty;
      test_case "KEYS with data" `Quick test_keys_with_data;
    ];
  ]

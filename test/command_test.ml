(* Command parsing tests *)

open Alcotest
open Codecrafters_redis

(* Custom testable for Command.t option *)
let command_testable = testable (fun ppf cmd ->
  let str = match cmd with
    | Command.Ping -> "Ping"
    | Command.Echo msg -> "Echo(" ^ Resp.serialize msg ^ ")"
    | Command.Get key -> "Get(" ^ key ^ ")"
    | Command.Set { key; value; expiry_ms } ->
        Printf.sprintf "Set(key=%s, value=%s, expiry=%s)"
          key (Resp.serialize value)
          (match expiry_ms with Some ms -> string_of_int ms | None -> "none")
    | Command.InfoReplication -> "InfoReplication"
    | Command.Replconf (Command.ReplconfListeningPort port) ->
        "Replconf(ListeningPort " ^ string_of_int port ^ ")"
    | Command.Replconf (Command.ReplconfCapa capa) ->
        "Replconf(Capa " ^ capa ^ ")"
    | Command.Replconf Command.ReplconfGetAck -> "Replconf(GetAck)"
    | Command.Psync { replication_id; offset } ->
        Printf.sprintf "Psync(id=%s, offset=%d)" replication_id offset
    | Command.Wait { num_replicas; timeout_ms } ->
        Printf.sprintf "Wait(replicas=%d, timeout=%d)" num_replicas timeout_ms
    | Command.ConfigGet Command.Dir ->
        "ConfigGet(dir)"
    | Command.ConfigGet Command.Dbfilename ->
        "ConfigGet(dbfilename)"
    | Command.Keys pattern ->
        "Keys(" ^ pattern ^ ")"
  in
  Format.fprintf ppf "%s" str
) (=)

let option_command_testable = option command_testable

(* PING Tests *)
let test_parse_ping () =
  let resp = Resp.Array [Resp.BulkString "PING"] in
  check option_command_testable "PING command"
    (Some Command.Ping)
    (Command.parse resp)

let test_parse_ping_lowercase () =
  let resp = Resp.Array [Resp.BulkString "ping"] in
  check option_command_testable "PING command (lowercase)"
    (Some Command.Ping)
    (Command.parse resp)

let test_parse_ping_mixed_case () =
  let resp = Resp.Array [Resp.BulkString "PiNg"] in
  check option_command_testable "PING command (mixed case)"
    (Some Command.Ping)
    (Command.parse resp)

let test_parse_ping_with_args () =
  let resp = Resp.Array [Resp.BulkString "PING"; Resp.BulkString "arg"] in
  check option_command_testable "PING with arguments (invalid)"
    None
    (Command.parse resp)

(* ECHO Tests *)
let test_parse_echo () =
  let resp = Resp.Array [Resp.BulkString "ECHO"; Resp.BulkString "hello"] in
  check option_command_testable "ECHO command"
    (Some (Command.Echo (Resp.BulkString "hello")))
    (Command.parse resp)

let test_parse_echo_integer () =
  let resp = Resp.Array [Resp.BulkString "ECHO"; Resp.Integer 42] in
  check option_command_testable "ECHO with integer"
    (Some (Command.Echo (Resp.Integer 42)))
    (Command.parse resp)

let test_parse_echo_array () =
  let msg = Resp.Array [Resp.BulkString "foo"; Resp.BulkString "bar"] in
  let resp = Resp.Array [Resp.BulkString "ECHO"; msg] in
  check option_command_testable "ECHO with array"
    (Some (Command.Echo msg))
    (Command.parse resp)

let test_parse_echo_no_arg () =
  let resp = Resp.Array [Resp.BulkString "ECHO"] in
  check option_command_testable "ECHO without argument (invalid)"
    None
    (Command.parse resp)

(* GET Tests *)
let test_parse_get () =
  let resp = Resp.Array [Resp.BulkString "GET"; Resp.BulkString "mykey"] in
  check option_command_testable "GET command"
    (Some (Command.Get "mykey"))
    (Command.parse resp)

let test_parse_get_empty_key () =
  let resp = Resp.Array [Resp.BulkString "GET"; Resp.BulkString ""] in
  check option_command_testable "GET with empty key"
    (Some (Command.Get ""))
    (Command.parse resp)

let test_parse_get_no_key () =
  let resp = Resp.Array [Resp.BulkString "GET"] in
  check option_command_testable "GET without key (invalid)"
    None
    (Command.parse resp)

let test_parse_get_wrong_type () =
  let resp = Resp.Array [Resp.BulkString "GET"; Resp.Integer 42] in
  check option_command_testable "GET with non-string key (invalid)"
    None
    (Command.parse resp)

(* SET Tests *)
let test_parse_set_basic () =
  let resp = Resp.Array [
    Resp.BulkString "SET";
    Resp.BulkString "key";
    Resp.BulkString "value"
  ] in
  check option_command_testable "SET command (basic)"
    (Some (Command.Set {
      key = "key";
      value = Resp.BulkString "value";
      expiry_ms = None
    }))
    (Command.parse resp)

let test_parse_set_with_px () =
  let resp = Resp.Array [
    Resp.BulkString "SET";
    Resp.BulkString "key";
    Resp.BulkString "value";
    Resp.BulkString "px";
    Resp.BulkString "1000"
  ] in
  check option_command_testable "SET with PX (milliseconds)"
    (Some (Command.Set {
      key = "key";
      value = Resp.BulkString "value";
      expiry_ms = Some 1000
    }))
    (Command.parse resp)

let test_parse_set_with_ex () =
  let resp = Resp.Array [
    Resp.BulkString "SET";
    Resp.BulkString "key";
    Resp.BulkString "value";
    Resp.BulkString "ex";
    Resp.BulkString "10"
  ] in
  check option_command_testable "SET with EX (seconds converted to ms)"
    (Some (Command.Set {
      key = "key";
      value = Resp.BulkString "value";
      expiry_ms = Some 10000
    }))
    (Command.parse resp)

let test_parse_set_with_px_uppercase () =
  let resp = Resp.Array [
    Resp.BulkString "SET";
    Resp.BulkString "key";
    Resp.BulkString "value";
    Resp.BulkString "PX";
    Resp.BulkString "2000"
  ] in
  check option_command_testable "SET with PX uppercase"
    (Some (Command.Set {
      key = "key";
      value = Resp.BulkString "value";
      expiry_ms = Some 2000
    }))
    (Command.parse resp)

let test_parse_set_negative_expiry () =
  let resp = Resp.Array [
    Resp.BulkString "SET";
    Resp.BulkString "key";
    Resp.BulkString "value";
    Resp.BulkString "px";
    Resp.BulkString "-100"
  ] in
  check option_command_testable "SET with negative expiry (invalid)"
    None
    (Command.parse resp)

let test_parse_set_non_bulk_value () =
  let resp = Resp.Array [
    Resp.BulkString "SET";
    Resp.BulkString "key";
    Resp.Integer 42
  ] in
  check option_command_testable "SET with integer value"
    (Some (Command.Set {
      key = "key";
      value = Resp.Integer 42;
      expiry_ms = None
    }))
    (Command.parse resp)

let test_parse_set_missing_value () =
  let resp = Resp.Array [
    Resp.BulkString "SET";
    Resp.BulkString "key"
  ] in
  check option_command_testable "SET without value (invalid)"
    None
    (Command.parse resp)

(* INFO Tests *)
let test_parse_info_replication () =
  let resp = Resp.Array [
    Resp.BulkString "INFO";
    Resp.BulkString "replication"
  ] in
  check option_command_testable "INFO replication"
    (Some Command.InfoReplication)
    (Command.parse resp)

let test_parse_info_replication_uppercase () =
  let resp = Resp.Array [
    Resp.BulkString "INFO";
    Resp.BulkString "REPLICATION"
  ] in
  check option_command_testable "INFO REPLICATION uppercase"
    (Some Command.InfoReplication)
    (Command.parse resp)

let test_parse_info_other_section () =
  let resp = Resp.Array [
    Resp.BulkString "INFO";
    Resp.BulkString "stats"
  ] in
  check option_command_testable "INFO stats (no 'r', invalid)"
    None
    (Command.parse resp)

let test_parse_info_no_section () =
  let resp = Resp.Array [Resp.BulkString "INFO"] in
  check option_command_testable "INFO without section (invalid)"
    None
    (Command.parse resp)

(* REPLCONF Tests *)
let test_parse_replconf_listening_port () =
  let resp = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "listening-port";
    Resp.BulkString "6380"
  ] in
  check option_command_testable "REPLCONF listening-port"
    (Some (Command.Replconf (Command.ReplconfListeningPort 6380)))
    (Command.parse resp)

let test_parse_replconf_port_mixed_case () =
  let resp = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "LISTENING-PORT";
    Resp.BulkString "6380"
  ] in
  check option_command_testable "REPLCONF LISTENING-PORT (mixed case)"
    (Some (Command.Replconf (Command.ReplconfListeningPort 6380)))
    (Command.parse resp)

let test_parse_replconf_port_too_low () =
  let resp = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "listening-port";
    Resp.BulkString "0"
  ] in
  check option_command_testable "REPLCONF port 0 (invalid)"
    None
    (Command.parse resp)

let test_parse_replconf_port_too_high () =
  let resp = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "listening-port";
    Resp.BulkString "65536"
  ] in
  check option_command_testable "REPLCONF port 65536 (invalid)"
    None
    (Command.parse resp)

let test_parse_replconf_capa () =
  let resp = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "capa";
    Resp.BulkString "psync2"
  ] in
  check option_command_testable "REPLCONF capa"
    (Some (Command.Replconf (Command.ReplconfCapa "psync2")))
    (Command.parse resp)

let test_parse_replconf_getack () =
  let resp = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "getack";
    Resp.BulkString "*"
  ] in
  check option_command_testable "REPLCONF GETACK"
    (Some (Command.Replconf Command.ReplconfGetAck))
    (Command.parse resp)

let test_parse_replconf_unknown () =
  let resp = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "unknown";
    Resp.BulkString "value"
  ] in
  check option_command_testable "REPLCONF unknown subcommand (invalid)"
    None
    (Command.parse resp)

let test_parse_replconf_missing_args () =
  let resp = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "listening-port"
  ] in
  check option_command_testable "REPLCONF missing argument (invalid)"
    None
    (Command.parse resp)

(* PSYNC Tests *)
let test_parse_psync () =
  let resp = Resp.Array [
    Resp.BulkString "PSYNC";
    Resp.BulkString "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    Resp.BulkString "0"
  ] in
  check option_command_testable "PSYNC command"
    (Some (Command.Psync {
      replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
      offset = 0
    }))
    (Command.parse resp)

let test_parse_psync_question_mark () =
  let resp = Resp.Array [
    Resp.BulkString "PSYNC";
    Resp.BulkString "?";
    Resp.BulkString "-1"
  ] in
  check option_command_testable "PSYNC with ? and -1"
    (Some (Command.Psync {
      replication_id = "?";
      offset = -1
    }))
    (Command.parse resp)

let test_parse_psync_invalid_offset () =
  let resp = Resp.Array [
    Resp.BulkString "PSYNC";
    Resp.BulkString "someid";
    Resp.BulkString "notanumber"
  ] in
  check option_command_testable "PSYNC with invalid offset (defaults to -1)"
    (Some (Command.Psync {
      replication_id = "someid";
      offset = -1
    }))
    (Command.parse resp)

let test_parse_psync_missing_args () =
  let resp = Resp.Array [
    Resp.BulkString "PSYNC";
    Resp.BulkString "someid"
  ] in
  check option_command_testable "PSYNC missing offset (invalid)"
    None
    (Command.parse resp)

(* WAIT Tests *)
let test_parse_wait () =
  let resp = Resp.Array [
    Resp.BulkString "WAIT";
    Resp.BulkString "2";
    Resp.BulkString "1000"
  ] in
  check option_command_testable "WAIT command"
    (Some (Command.Wait {
      num_replicas = 2;
      timeout_ms = 1000
    }))
    (Command.parse resp)

let test_parse_wait_zero_replicas () =
  let resp = Resp.Array [
    Resp.BulkString "WAIT";
    Resp.BulkString "0";
    Resp.BulkString "1000"
  ] in
  check option_command_testable "WAIT with 0 replicas"
    (Some (Command.Wait {
      num_replicas = 0;
      timeout_ms = 1000
    }))
    (Command.parse resp)

let test_parse_wait_zero_timeout () =
  let resp = Resp.Array [
    Resp.BulkString "WAIT";
    Resp.BulkString "1";
    Resp.BulkString "0"
  ] in
  check option_command_testable "WAIT with 0 timeout"
    (Some (Command.Wait {
      num_replicas = 1;
      timeout_ms = 0
    }))
    (Command.parse resp)

let test_parse_wait_negative_replicas () =
  let resp = Resp.Array [
    Resp.BulkString "WAIT";
    Resp.BulkString "-1";
    Resp.BulkString "1000"
  ] in
  check option_command_testable "WAIT with negative replicas (invalid)"
    None
    (Command.parse resp)

let test_parse_wait_negative_timeout () =
  let resp = Resp.Array [
    Resp.BulkString "WAIT";
    Resp.BulkString "1";
    Resp.BulkString "-1000"
  ] in
  check option_command_testable "WAIT with negative timeout (invalid)"
    None
    (Command.parse resp)

let test_parse_wait_invalid_numbers () =
  let resp = Resp.Array [
    Resp.BulkString "WAIT";
    Resp.BulkString "abc";
    Resp.BulkString "1000"
  ] in
  check option_command_testable "WAIT with invalid num_replicas (invalid)"
    None
    (Command.parse resp)

(* Edge Cases *)
let test_parse_unknown_command () =
  let resp = Resp.Array [Resp.BulkString "UNKNOWN"] in
  check option_command_testable "Unknown command"
    None
    (Command.parse resp)

let test_parse_empty_array () =
  let resp = Resp.Array [] in
  check option_command_testable "Empty array"
    None
    (Command.parse resp)

let test_parse_not_array () =
  let resp = Resp.BulkString "PING" in
  check option_command_testable "Not an array"
    None
    (Command.parse resp)

let test_parse_simple_string_cmd () =
  let resp = Resp.Array [Resp.SimpleString "PING"] in
  check option_command_testable "SimpleString instead of BulkString"
    None
    (Command.parse resp)

(* CONFIG Tests *)
let test_parse_config_get_dir () =
  let resp = Resp.Array [
    Resp.BulkString "CONFIG";
    Resp.BulkString "GET";
    Resp.BulkString "dir"
  ] in
  check option_command_testable "CONFIG GET dir"
    (Some (Command.ConfigGet Command.Dir))
    (Command.parse resp)

let test_parse_config_get_dbfilename () =
  let resp = Resp.Array [
    Resp.BulkString "CONFIG";
    Resp.BulkString "GET";
    Resp.BulkString "dbfilename"
  ] in
  check option_command_testable "CONFIG GET dbfilename"
    (Some (Command.ConfigGet Command.Dbfilename))
    (Command.parse resp)

let test_parse_config_get_mixed_case () =
  let resp = Resp.Array [
    Resp.BulkString "config";
    Resp.BulkString "get";
    Resp.BulkString "DIR"
  ] in
  check option_command_testable "CONFIG GET (mixed case)"
    (Some (Command.ConfigGet Command.Dir))
    (Command.parse resp)

let test_parse_config_missing_args () =
  let resp = Resp.Array [
    Resp.BulkString "CONFIG";
    Resp.BulkString "GET"
  ] in
  check option_command_testable "CONFIG GET without parameter (invalid)"
    None
    (Command.parse resp)

let test_parse_config_unknown_subcommand () =
  let resp = Resp.Array [
    Resp.BulkString "CONFIG";
    Resp.BulkString "SET";
    Resp.BulkString "dir";
    Resp.BulkString "/tmp"
  ] in
  check option_command_testable "CONFIG SET (unknown subcommand)"
    None
    (Command.parse resp)

(* Test Suite *)
let () =
  run "Command" [
    "ping", [
      test_case "PING" `Quick test_parse_ping;
      test_case "ping (lowercase)" `Quick test_parse_ping_lowercase;
      test_case "PiNg (mixed case)" `Quick test_parse_ping_mixed_case;
      test_case "PING with args (invalid)" `Quick test_parse_ping_with_args;
    ];
    "echo", [
      test_case "ECHO" `Quick test_parse_echo;
      test_case "ECHO integer" `Quick test_parse_echo_integer;
      test_case "ECHO array" `Quick test_parse_echo_array;
      test_case "ECHO no arg (invalid)" `Quick test_parse_echo_no_arg;
    ];
    "get", [
      test_case "GET" `Quick test_parse_get;
      test_case "GET empty key" `Quick test_parse_get_empty_key;
      test_case "GET no key (invalid)" `Quick test_parse_get_no_key;
      test_case "GET wrong type (invalid)" `Quick test_parse_get_wrong_type;
    ];
    "set", [
      test_case "SET basic" `Quick test_parse_set_basic;
      test_case "SET with PX" `Quick test_parse_set_with_px;
      test_case "SET with EX" `Quick test_parse_set_with_ex;
      test_case "SET with PX uppercase" `Quick test_parse_set_with_px_uppercase;
      test_case "SET negative expiry (invalid)" `Quick test_parse_set_negative_expiry;
      test_case "SET non-bulk value" `Quick test_parse_set_non_bulk_value;
      test_case "SET missing value (invalid)" `Quick test_parse_set_missing_value;
    ];
    "info", [
      test_case "INFO replication" `Quick test_parse_info_replication;
      test_case "INFO REPLICATION uppercase" `Quick test_parse_info_replication_uppercase;
      test_case "INFO other section (invalid)" `Quick test_parse_info_other_section;
      test_case "INFO no section (invalid)" `Quick test_parse_info_no_section;
    ];
    "replconf", [
      test_case "listening-port" `Quick test_parse_replconf_listening_port;
      test_case "LISTENING-PORT (mixed case)" `Quick test_parse_replconf_port_mixed_case;
      test_case "port 0 (invalid)" `Quick test_parse_replconf_port_too_low;
      test_case "port 65536 (invalid)" `Quick test_parse_replconf_port_too_high;
      test_case "capa" `Quick test_parse_replconf_capa;
      test_case "GETACK" `Quick test_parse_replconf_getack;
      test_case "unknown subcommand (invalid)" `Quick test_parse_replconf_unknown;
      test_case "missing args (invalid)" `Quick test_parse_replconf_missing_args;
    ];
    "psync", [
      test_case "PSYNC" `Quick test_parse_psync;
      test_case "PSYNC with ?" `Quick test_parse_psync_question_mark;
      test_case "PSYNC invalid offset" `Quick test_parse_psync_invalid_offset;
      test_case "PSYNC missing args (invalid)" `Quick test_parse_psync_missing_args;
    ];
    "wait", [
      test_case "WAIT" `Quick test_parse_wait;
      test_case "WAIT 0 replicas" `Quick test_parse_wait_zero_replicas;
      test_case "WAIT 0 timeout" `Quick test_parse_wait_zero_timeout;
      test_case "WAIT negative replicas (invalid)" `Quick test_parse_wait_negative_replicas;
      test_case "WAIT negative timeout (invalid)" `Quick test_parse_wait_negative_timeout;
      test_case "WAIT invalid numbers (invalid)" `Quick test_parse_wait_invalid_numbers;
    ];
    "config", [
      test_case "CONFIG GET dir" `Quick test_parse_config_get_dir;
      test_case "CONFIG GET dbfilename" `Quick test_parse_config_get_dbfilename;
      test_case "CONFIG GET (mixed case)" `Quick test_parse_config_get_mixed_case;
      test_case "CONFIG GET missing args (invalid)" `Quick test_parse_config_missing_args;
      test_case "CONFIG unknown subcommand (invalid)" `Quick test_parse_config_unknown_subcommand;
    ];
    "edge_cases", [
      test_case "Unknown command" `Quick test_parse_unknown_command;
      test_case "Empty array" `Quick test_parse_empty_array;
      test_case "Not array" `Quick test_parse_not_array;
      test_case "SimpleString cmd (invalid)" `Quick test_parse_simple_string_cmd;
    ];
  ]

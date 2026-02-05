(* Command parsing tests *)

open Alcotest
open Codecrafters_redis

(* Helper to extract parse error *)
let check_parse_error msg expected resp =
  match Command.parse resp with
  | Error e -> check bool msg true (e = expected)
  | Ok _ -> fail (msg ^ ": expected error but got Ok")

(* PING Tests *)
let test_parse_ping () =
  let resp = Resp.Array [Resp.BulkString "PING"] in
  match Command.parse resp with
  | Ok (Command.Read Command.Ping) -> ()
  | _ -> fail "expected Read Ping"

let test_parse_ping_lowercase () =
  let resp = Resp.Array [Resp.BulkString "ping"] in
  match Command.parse resp with
  | Ok (Command.Read Command.Ping) -> ()
  | _ -> fail "expected Read Ping"

let test_parse_ping_mixed_case () =
  let resp = Resp.Array [Resp.BulkString "PiNg"] in
  match Command.parse resp with
  | Ok (Command.Read Command.Ping) -> ()
  | _ -> fail "expected Read Ping"

let test_parse_ping_with_args () =
  check_parse_error "PING with arguments" `UnknownCommand
    (Resp.Array [Resp.BulkString "PING"; Resp.BulkString "arg"])

(* ECHO Tests *)
let test_parse_echo () =
  let resp = Resp.Array [Resp.BulkString "ECHO"; Resp.BulkString "hello"] in
  match Command.parse resp with
  | Ok (Command.Read (Command.Echo (Resp.BulkString "hello"))) -> ()
  | _ -> fail "expected Read (Echo \"hello\")"

let test_parse_echo_integer () =
  let resp = Resp.Array [Resp.BulkString "ECHO"; Resp.Integer 42] in
  match Command.parse resp with
  | Ok (Command.Read (Command.Echo (Resp.Integer 42))) -> ()
  | _ -> fail "expected Read (Echo 42)"

let test_parse_echo_array () =
  let msg = Resp.Array [Resp.BulkString "foo"; Resp.BulkString "bar"] in
  let resp = Resp.Array [Resp.BulkString "ECHO"; msg] in
  match Command.parse resp with
  | Ok (Command.Read (Command.Echo (Resp.Array [Resp.BulkString "foo"; Resp.BulkString "bar"]))) -> ()
  | _ -> fail "expected Read (Echo [foo; bar])"

let test_parse_echo_no_arg () =
  check_parse_error "ECHO without argument" `UnknownCommand
    (Resp.Array [Resp.BulkString "ECHO"])

(* GET Tests *)
let test_parse_get () =
  let resp = Resp.Array [Resp.BulkString "GET"; Resp.BulkString "mykey"] in
  match Command.parse resp with
  | Ok (Command.Read (Command.Get "mykey")) -> ()
  | _ -> fail "expected Read (Get \"mykey\")"

let test_parse_get_empty_key () =
  let resp = Resp.Array [Resp.BulkString "GET"; Resp.BulkString ""] in
  match Command.parse resp with
  | Ok (Command.Read (Command.Get "")) -> ()
  | _ -> fail "expected Read (Get \"\")"

let test_parse_get_no_key () =
  check_parse_error "GET without key" `UnknownCommand
    (Resp.Array [Resp.BulkString "GET"])

let test_parse_get_wrong_type () =
  check_parse_error "GET with non-string key" `UnknownCommand
    (Resp.Array [Resp.BulkString "GET"; Resp.Integer 42])

(* SET Tests *)
let test_parse_set_basic () =
  let resp = Resp.Array [
    Resp.BulkString "SET";
    Resp.BulkString "key";
    Resp.BulkString "value"
  ] in
  match Command.parse resp with
  | Ok (Command.Write (Command.Set { key = "key"; value = Resp.BulkString "value"; expiry_ms = None })) -> ()
  | _ -> fail "expected Write (Set {key; value; None})"

let test_parse_set_with_px () =
  let resp = Resp.Array [
    Resp.BulkString "SET";
    Resp.BulkString "key";
    Resp.BulkString "value";
    Resp.BulkString "px";
    Resp.BulkString "1000"
  ] in
  match Command.parse resp with
  | Ok (Command.Write (Command.Set { key = "key"; value = Resp.BulkString "value"; expiry_ms = Some 1000 })) -> ()
  | _ -> fail "expected Write (Set {key; value; Some 1000})"

let test_parse_set_with_ex () =
  let resp = Resp.Array [
    Resp.BulkString "SET";
    Resp.BulkString "key";
    Resp.BulkString "value";
    Resp.BulkString "ex";
    Resp.BulkString "10"
  ] in
  match Command.parse resp with
  | Ok (Command.Write (Command.Set { key = "key"; value = Resp.BulkString "value"; expiry_ms = Some 10000 })) -> ()
  | _ -> fail "expected Write (Set {key; value; Some 10000})"

let test_parse_set_with_px_uppercase () =
  let resp = Resp.Array [
    Resp.BulkString "SET";
    Resp.BulkString "key";
    Resp.BulkString "value";
    Resp.BulkString "PX";
    Resp.BulkString "2000"
  ] in
  match Command.parse resp with
  | Ok (Command.Write (Command.Set { key = "key"; value = Resp.BulkString "value"; expiry_ms = Some 2000 })) -> ()
  | _ -> fail "expected Write (Set {key; value; Some 2000})"

let test_parse_set_negative_expiry () =
  check_parse_error "SET with negative expiry" (`InvalidExpiry (-100))
    (Resp.Array [
      Resp.BulkString "SET";
      Resp.BulkString "key";
      Resp.BulkString "value";
      Resp.BulkString "px";
      Resp.BulkString "-100"
    ])

let test_parse_set_non_bulk_value () =
  let resp = Resp.Array [
    Resp.BulkString "SET";
    Resp.BulkString "key";
    Resp.Integer 42
  ] in
  match Command.parse resp with
  | Ok (Command.Write (Command.Set { key = "key"; value = Resp.Integer 42; expiry_ms = None })) -> ()
  | _ -> fail "expected Write (Set {key; Integer 42; None})"

let test_parse_set_missing_value () =
  check_parse_error "SET without value" `UnknownCommand
    (Resp.Array [
      Resp.BulkString "SET";
      Resp.BulkString "key"
    ])

(* INFO Tests *)
let test_parse_info_replication () =
  let resp = Resp.Array [
    Resp.BulkString "INFO";
    Resp.BulkString "replication"
  ] in
  match Command.parse resp with
  | Ok (Command.Read Command.InfoReplication) -> ()
  | _ -> fail "expected Read InfoReplication"

let test_parse_info_replication_uppercase () =
  let resp = Resp.Array [
    Resp.BulkString "INFO";
    Resp.BulkString "REPLICATION"
  ] in
  match Command.parse resp with
  | Ok (Command.Read Command.InfoReplication) -> ()
  | _ -> fail "expected Read InfoReplication"

let test_parse_info_other_section () =
  check_parse_error "INFO stats" (`MalformedCommand "INFO requires 'replication' section")
    (Resp.Array [
      Resp.BulkString "INFO";
      Resp.BulkString "stats"
    ])

let test_parse_info_no_section () =
  check_parse_error "INFO without section" `UnknownCommand
    (Resp.Array [Resp.BulkString "INFO"])

(* REPLCONF Tests *)
let test_parse_replconf_listening_port () =
  let resp = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "listening-port";
    Resp.BulkString "6380"
  ] in
  match Command.parse resp with
  | Ok (Command.Read (Command.Replconf (Command.ReplconfListeningPort 6380))) -> ()
  | _ -> fail "expected Read (Replconf (ListeningPort 6380))"

let test_parse_replconf_port_mixed_case () =
  let resp = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "LISTENING-PORT";
    Resp.BulkString "6380"
  ] in
  match Command.parse resp with
  | Ok (Command.Read (Command.Replconf (Command.ReplconfListeningPort 6380))) -> ()
  | _ -> fail "expected Read (Replconf (ListeningPort 6380))"

let test_parse_replconf_port_too_low () =
  check_parse_error "REPLCONF port 0" (`InvalidPort 0)
    (Resp.Array [
      Resp.BulkString "REPLCONF";
      Resp.BulkString "listening-port";
      Resp.BulkString "0"
    ])

let test_parse_replconf_port_too_high () =
  check_parse_error "REPLCONF port 65536" (`InvalidPort 65536)
    (Resp.Array [
      Resp.BulkString "REPLCONF";
      Resp.BulkString "listening-port";
      Resp.BulkString "65536"
    ])

let test_parse_replconf_capa () =
  let resp = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "capa";
    Resp.BulkString "psync2"
  ] in
  match Command.parse resp with
  | Ok (Command.Read (Command.Replconf (Command.ReplconfCapa "psync2"))) -> ()
  | _ -> fail "expected Read (Replconf (Capa \"psync2\"))"

let test_parse_replconf_getack () =
  let resp = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "getack";
    Resp.BulkString "*"
  ] in
  match Command.parse resp with
  | Ok (Command.Read (Command.Replconf Command.ReplconfGetAck)) -> ()
  | _ -> fail "expected Read (Replconf GetAck)"

let test_parse_replconf_unknown () =
  check_parse_error "REPLCONF unknown"
    (`MalformedCommand "Unknown REPLCONF subcommand: unknown")
    (Resp.Array [
      Resp.BulkString "REPLCONF";
      Resp.BulkString "unknown";
      Resp.BulkString "value"
    ])

let test_parse_replconf_missing_args () =
  check_parse_error "REPLCONF missing args"
    (`MalformedCommand "REPLCONF requires exactly 2 arguments")
    (Resp.Array [
      Resp.BulkString "REPLCONF";
      Resp.BulkString "listening-port"
    ])

(* PSYNC Tests *)
let test_parse_psync () =
  let resp = Resp.Array [
    Resp.BulkString "PSYNC";
    Resp.BulkString "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    Resp.BulkString "0"
  ] in
  match Command.parse resp with
  | Ok (Command.Write (Command.Psync { replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"; offset = 0 })) -> ()
  | _ -> fail "expected Write (Psync {id; offset=0})"

let test_parse_psync_question_mark () =
  let resp = Resp.Array [
    Resp.BulkString "PSYNC";
    Resp.BulkString "?";
    Resp.BulkString "-1"
  ] in
  match Command.parse resp with
  | Ok (Command.Write (Command.Psync { replication_id = "?"; offset = -1 })) -> ()
  | _ -> fail "expected Write (Psync {id=?; offset=-1})"

let test_parse_psync_invalid_offset () =
  let resp = Resp.Array [
    Resp.BulkString "PSYNC";
    Resp.BulkString "someid";
    Resp.BulkString "notanumber"
  ] in
  match Command.parse resp with
  | Ok (Command.Write (Command.Psync { replication_id = "someid"; offset = -1 })) -> ()
  | _ -> fail "expected Write (Psync {id=someid; offset=-1})"

let test_parse_psync_missing_args () =
  check_parse_error "PSYNC missing args" `UnknownCommand
    (Resp.Array [
      Resp.BulkString "PSYNC";
      Resp.BulkString "someid"
    ])

(* WAIT Tests *)
let test_parse_wait () =
  let resp = Resp.Array [
    Resp.BulkString "WAIT";
    Resp.BulkString "2";
    Resp.BulkString "1000"
  ] in
  match Command.parse resp with
  | Ok (Command.Write (Command.Wait { num_replicas = 2; timeout_ms = 1000 })) -> ()
  | _ -> fail "expected Write (Wait {2; 1000})"

let test_parse_wait_zero_replicas () =
  let resp = Resp.Array [
    Resp.BulkString "WAIT";
    Resp.BulkString "0";
    Resp.BulkString "1000"
  ] in
  match Command.parse resp with
  | Ok (Command.Write (Command.Wait { num_replicas = 0; timeout_ms = 1000 })) -> ()
  | _ -> fail "expected Write (Wait {0; 1000})"

let test_parse_wait_zero_timeout () =
  let resp = Resp.Array [
    Resp.BulkString "WAIT";
    Resp.BulkString "1";
    Resp.BulkString "0"
  ] in
  match Command.parse resp with
  | Ok (Command.Write (Command.Wait { num_replicas = 1; timeout_ms = 0 })) -> ()
  | _ -> fail "expected Write (Wait {1; 0})"

let test_parse_wait_negative_replicas () =
  check_parse_error "WAIT negative replicas" (`InvalidNumReplicas (-1))
    (Resp.Array [
      Resp.BulkString "WAIT";
      Resp.BulkString "-1";
      Resp.BulkString "1000"
    ])

let test_parse_wait_negative_timeout () =
  check_parse_error "WAIT negative timeout" (`InvalidTimeout (-1000))
    (Resp.Array [
      Resp.BulkString "WAIT";
      Resp.BulkString "1";
      Resp.BulkString "-1000"
    ])

let test_parse_wait_invalid_numbers () =
  check_parse_error "WAIT invalid numbers"
    (`MalformedCommand "WAIT requires two integer arguments")
    (Resp.Array [
      Resp.BulkString "WAIT";
      Resp.BulkString "abc";
      Resp.BulkString "1000"
    ])

(* Edge Cases *)
let test_parse_unknown_command () =
  check_parse_error "Unknown command" `UnknownCommand
    (Resp.Array [Resp.BulkString "UNKNOWN"])

let test_parse_empty_array () =
  check_parse_error "Empty array"
    (`MalformedCommand "Expected RESP Array with command")
    (Resp.Array [])

let test_parse_not_array () =
  check_parse_error "Not array"
    (`MalformedCommand "Expected RESP Array with command")
    (Resp.BulkString "PING")

let test_parse_simple_string_cmd () =
  check_parse_error "SimpleString cmd"
    (`MalformedCommand "Expected RESP Array with command")
    (Resp.Array [Resp.SimpleString "PING"])

(* CONFIG Tests *)
let test_parse_config_get_dir () =
  let resp = Resp.Array [
    Resp.BulkString "CONFIG";
    Resp.BulkString "GET";
    Resp.BulkString "dir"
  ] in
  match Command.parse resp with
  | Ok (Command.Read (Command.ConfigGet Command.Dir)) -> ()
  | _ -> fail "expected Read (ConfigGet Dir)"

let test_parse_config_get_dbfilename () =
  let resp = Resp.Array [
    Resp.BulkString "CONFIG";
    Resp.BulkString "GET";
    Resp.BulkString "dbfilename"
  ] in
  match Command.parse resp with
  | Ok (Command.Read (Command.ConfigGet Command.Dbfilename)) -> ()
  | _ -> fail "expected Read (ConfigGet Dbfilename)"

let test_parse_config_get_mixed_case () =
  let resp = Resp.Array [
    Resp.BulkString "config";
    Resp.BulkString "get";
    Resp.BulkString "DIR"
  ] in
  match Command.parse resp with
  | Ok (Command.Read (Command.ConfigGet Command.Dir)) -> ()
  | _ -> fail "expected Read (ConfigGet Dir)"

let test_parse_config_missing_args () =
  check_parse_error "CONFIG GET missing args" `UnknownCommand
    (Resp.Array [
      Resp.BulkString "CONFIG";
      Resp.BulkString "GET"
    ])

let test_parse_config_unknown_subcommand () =
  check_parse_error "CONFIG SET" `UnknownCommand
    (Resp.Array [
      Resp.BulkString "CONFIG";
      Resp.BulkString "SET";
      Resp.BulkString "dir";
      Resp.BulkString "/tmp"
    ])

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

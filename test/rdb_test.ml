(* RDB parser tests *)

open Alcotest
open Codecrafters_redis

let test_parse_nonexistent_file () =
  let pairs = Rdb.parse_file "/nonexistent/file.rdb" in
  check int "nonexistent file returns empty list" 0 (List.length pairs)

let test_parse_empty_rdb () =
  let temp_file = Filename.temp_file "test" ".rdb" in
  let oc = open_out_bin temp_file in
  output_string oc "REDIS0011";
  output_byte oc 0xFF;
  close_out oc;

  let pairs = Rdb.parse_file temp_file in
  Sys.remove temp_file;
  check int "empty RDB returns empty list" 0 (List.length pairs)

let test_parse_single_key () =
  let temp_file = Filename.temp_file "test" ".rdb" in
  let oc = open_out_bin temp_file in
  output_string oc "REDIS0011";
  output_byte oc 0xFE;
  output_byte oc 0;
  output_byte oc 0;
  output_byte oc 3;
  output_string oc "key";
  output_byte oc 5;
  output_string oc "value";
  output_byte oc 0xFF;
  close_out oc;

  let pairs = Rdb.parse_file temp_file in
  Sys.remove temp_file;
  check int "RDB with one key" 1 (List.length pairs);
  match pairs with
  | [(k, Resp.BulkString v)] ->
      check string "key name" "key" k;
      check string "value" "value" v
  | _ -> fail "Expected one key-value pair"

let test_parse_with_metadata () =
  let temp_file = Filename.temp_file "test" ".rdb" in
  let oc = open_out_bin temp_file in
  output_string oc "REDIS0011";
  (* AUX field *)
  output_byte oc 0xFA;
  output_byte oc 4;
  output_string oc "name";
  output_byte oc 5;
  output_string oc "value";
  (* SELECTDB *)
  output_byte oc 0xFE;
  output_byte oc 0;
  (* RESIZEDB *)
  output_byte oc 0xFB;
  output_byte oc 1;
  output_byte oc 0;
  (* Key-value *)
  output_byte oc 0;
  output_byte oc 5;
  output_string oc "hello";
  output_byte oc 5;
  output_string oc "world";
  (* EOF *)
  output_byte oc 0xFF;
  close_out oc;

  let pairs = Rdb.parse_file temp_file in
  Sys.remove temp_file;
  check int "RDB with metadata" 1 (List.length pairs);
  match pairs with
  | [(k, Resp.BulkString v)] ->
      check string "key name" "hello" k;
      check string "value" "world" v
  | _ -> fail "Expected one key-value pair"

let () =
  run "RDB" [
    "basic", [
      test_case "nonexistent file" `Quick test_parse_nonexistent_file;
      test_case "empty RDB" `Quick test_parse_empty_rdb;
      test_case "single key" `Quick test_parse_single_key;
      test_case "with metadata" `Quick test_parse_with_metadata;
    ];
  ]

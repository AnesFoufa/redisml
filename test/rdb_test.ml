(* RDB parser tests *)

open Alcotest
open Codecrafters_redis

let test_parse_nonexistent_file () =
  let result = Rdb.parse_file "/nonexistent/file.rdb" in
  match result with
  | Error (`FileNotFound _) -> ()
  | Error err -> failwith (Printf.sprintf "Expected FileNotFound, got: %s" (Rdb.error_to_string err))
  | Ok _ -> fail "Expected error for nonexistent file"

let test_parse_empty_rdb () =
  let temp_file = Filename.temp_file "test" ".rdb" in
  let oc = open_out_bin temp_file in
  output_string oc "REDIS0011";
  output_byte oc 0xFF;
  close_out oc;

  let result = Rdb.parse_file temp_file in
  Sys.remove temp_file;
  match result with
  | Ok pairs -> check int "empty RDB returns empty list" 0 (List.length pairs)
  | Error err -> failwith (Printf.sprintf "Parse failed: %s" (Rdb.error_to_string err))

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

  let result = Rdb.parse_file temp_file in
  Sys.remove temp_file;
  match result with
  | Ok pairs ->
      check int "RDB with one key" 1 (List.length pairs);
      (match pairs with
       | [(k, Resp.BulkString v, expiry)] ->
           check string "key name" "key" k;
           check string "value" "value" v;
           check (option reject) "no expiry" None expiry
       | _ -> fail "Expected one key-value pair")
  | Error err -> failwith (Printf.sprintf "Parse failed: %s" (Rdb.error_to_string err))

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

  let result = Rdb.parse_file temp_file in
  Sys.remove temp_file;
  match result with
  | Ok pairs ->
      check int "RDB with metadata" 1 (List.length pairs);
      (match pairs with
       | [(k, Resp.BulkString v, expiry)] ->
           check string "key name" "hello" k;
           check string "value" "world" v;
           check (option reject) "no expiry" None expiry
       | _ -> fail "Expected one key-value pair")
  | Error err -> failwith (Printf.sprintf "Parse failed: %s" (Rdb.error_to_string err))

let write_int32_le oc n =
  output_byte oc (n land 0xFF);
  output_byte oc ((n lsr 8) land 0xFF);
  output_byte oc ((n lsr 16) land 0xFF);
  output_byte oc ((n lsr 24) land 0xFF)

let write_int64_le oc n =
  output_byte oc (Int64.to_int (Int64.logand n 0xFFL));
  output_byte oc (Int64.to_int (Int64.logand (Int64.shift_right_logical n 8) 0xFFL));
  output_byte oc (Int64.to_int (Int64.logand (Int64.shift_right_logical n 16) 0xFFL));
  output_byte oc (Int64.to_int (Int64.logand (Int64.shift_right_logical n 24) 0xFFL));
  output_byte oc (Int64.to_int (Int64.logand (Int64.shift_right_logical n 32) 0xFFL));
  output_byte oc (Int64.to_int (Int64.logand (Int64.shift_right_logical n 40) 0xFFL));
  output_byte oc (Int64.to_int (Int64.logand (Int64.shift_right_logical n 48) 0xFFL));
  output_byte oc (Int64.to_int (Int64.logand (Int64.shift_right_logical n 56) 0xFFL))

let test_parse_with_expiretime () =
  let temp_file = Filename.temp_file "test" ".rdb" in
  let oc = open_out_bin temp_file in
  output_string oc "REDIS0011";
  output_byte oc 0xFE;
  output_byte oc 0;
  (* EXPIRETIME in seconds *)
  output_byte oc 0xFD;
  write_int32_le oc 1700000000;
  output_byte oc 0;
  output_byte oc 3;
  output_string oc "key";
  output_byte oc 5;
  output_string oc "value";
  output_byte oc 0xFF;
  close_out oc;

  let result = Rdb.parse_file temp_file in
  Sys.remove temp_file;
  match result with
  | Ok pairs ->
      check int "RDB with expiretime" 1 (List.length pairs);
      (match pairs with
       | [(k, Resp.BulkString v, Some expiry)] ->
           check string "key name" "key" k;
           check string "value" "value" v;
           check (float 0.1) "expiry timestamp" 1700000000.0 expiry
       | _ -> fail "Expected one key-value pair with expiry")
  | Error err -> failwith (Printf.sprintf "Parse failed: %s" (Rdb.error_to_string err))

let test_parse_with_expiretimems () =
  let temp_file = Filename.temp_file "test" ".rdb" in
  let oc = open_out_bin temp_file in
  output_string oc "REDIS0011";
  output_byte oc 0xFE;
  output_byte oc 0;
  (* EXPIRETIMEMS in milliseconds *)
  output_byte oc 0xFC;
  write_int64_le oc 1700000000500L;
  output_byte oc 0;
  output_byte oc 3;
  output_string oc "key";
  output_byte oc 5;
  output_string oc "value";
  output_byte oc 0xFF;
  close_out oc;

  let result = Rdb.parse_file temp_file in
  Sys.remove temp_file;
  match result with
  | Ok pairs ->
      check int "RDB with expiretimems" 1 (List.length pairs);
      (match pairs with
       | [(k, Resp.BulkString v, Some expiry)] ->
           check string "key name" "key" k;
           check string "value" "value" v;
           check (float 0.1) "expiry timestamp" 1700000000.5 expiry
       | _ -> fail "Expected one key-value pair with expiry")
  | Error err -> failwith (Printf.sprintf "Parse failed: %s" (Rdb.error_to_string err))

let () =
  run "RDB" [
    "basic", [
      test_case "nonexistent file" `Quick test_parse_nonexistent_file;
      test_case "empty RDB" `Quick test_parse_empty_rdb;
      test_case "single key" `Quick test_parse_single_key;
      test_case "with metadata" `Quick test_parse_with_metadata;
    ];
    "expiry", [
      test_case "EXPIRETIME (seconds)" `Quick test_parse_with_expiretime;
      test_case "EXPIRETIMEMS (milliseconds)" `Quick test_parse_with_expiretimems;
    ];
  ]

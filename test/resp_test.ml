(* RESP parsing and serialization tests *)

open Alcotest
open Codecrafters_redis

(* Custom testable for Resp.t *)
let resp_testable = testable (fun ppf r ->
  Format.fprintf ppf "%s" (Resp.serialize r)
) (=)

let option_resp_pair_testable = option (pair resp_testable string)
let resp_list_testable = list resp_testable

(* Serialization Tests *)
let test_serialize_simple_string () =
  check string "SimpleString serialization"
    "+OK\r\n"
    (Resp.serialize (Resp.SimpleString "OK"))

let test_serialize_simple_error () =
  check string "SimpleError serialization"
    "-Error message\r\n"
    (Resp.serialize (Resp.SimpleError "Error message"))

let test_serialize_integer () =
  check string "Integer serialization"
    ":42\r\n"
    (Resp.serialize (Resp.Integer 42))

let test_serialize_negative_integer () =
  check string "Negative integer serialization"
    ":-100\r\n"
    (Resp.serialize (Resp.Integer (-100)))

let test_serialize_zero () =
  check string "Zero serialization"
    ":0\r\n"
    (Resp.serialize (Resp.Integer 0))

let test_serialize_bulk_string () =
  check string "BulkString serialization"
    "$6\r\nfoobar\r\n"
    (Resp.serialize (Resp.BulkString "foobar"))

let test_serialize_empty_bulk_string () =
  check string "Empty BulkString serialization"
    "$0\r\n\r\n"
    (Resp.serialize (Resp.BulkString ""))

let test_serialize_null () =
  check string "Null serialization"
    "$-1\r\n"
    (Resp.serialize Resp.Null)

let test_serialize_empty_array () =
  check string "Empty array serialization"
    "*0\r\n"
    (Resp.serialize (Resp.Array []))

let test_serialize_array () =
  check string "Array serialization"
    "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    (Resp.serialize (Resp.Array [
      Resp.BulkString "foo";
      Resp.BulkString "bar"
    ]))

let test_serialize_nested_array () =
  check string "Nested array serialization"
    "*2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:42\r\n"
    (Resp.serialize (Resp.Array [
      Resp.Array [Resp.BulkString "foo"; Resp.BulkString "bar"];
      Resp.Integer 42
    ]))

let test_serialize_mixed_array () =
  check string "Mixed type array serialization"
    "*5\r\n+OK\r\n-Error\r\n:100\r\n$4\r\ntest\r\n$-1\r\n"
    (Resp.serialize (Resp.Array [
      Resp.SimpleString "OK";
      Resp.SimpleError "Error";
      Resp.Integer 100;
      Resp.BulkString "test";
      Resp.Null
    ]))

(* Parsing Tests *)
let test_parse_simple_string () =
  check option_resp_pair_testable "Parse SimpleString"
    (Some (Resp.SimpleString "OK", ""))
    (Resp.parse "+OK\r\n")

let test_parse_simple_error () =
  check option_resp_pair_testable "Parse SimpleError"
    (Some (Resp.SimpleError "ERR unknown", ""))
    (Resp.parse "-ERR unknown\r\n")

let test_parse_integer () =
  check option_resp_pair_testable "Parse Integer"
    (Some (Resp.Integer 1000, ""))
    (Resp.parse ":1000\r\n")

let test_parse_negative_integer () =
  check option_resp_pair_testable "Parse negative integer"
    (Some (Resp.Integer (-42), ""))
    (Resp.parse ":-42\r\n")

let test_parse_bulk_string () =
  check option_resp_pair_testable "Parse BulkString"
    (Some (Resp.BulkString "hello", ""))
    (Resp.parse "$5\r\nhello\r\n")

let test_parse_empty_bulk_string () =
  check option_resp_pair_testable "Parse empty BulkString"
    (Some (Resp.BulkString "", ""))
    (Resp.parse "$0\r\n\r\n")

let test_parse_null () =
  check option_resp_pair_testable "Parse Null"
    (Some (Resp.Null, ""))
    (Resp.parse "$-1\r\n")

let test_parse_empty_array () =
  check option_resp_pair_testable "Parse empty array"
    (Some (Resp.Array [], ""))
    (Resp.parse "*0\r\n")

let test_parse_array () =
  check option_resp_pair_testable "Parse array"
    (Some (Resp.Array [Resp.BulkString "foo"; Resp.BulkString "bar"], ""))
    (Resp.parse "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")

let test_parse_nested_array () =
  check option_resp_pair_testable "Parse nested array"
    (Some (Resp.Array [
      Resp.Array [Resp.Integer 1; Resp.Integer 2];
      Resp.Array [Resp.Integer 3; Resp.Integer 4]
    ], ""))
    (Resp.parse "*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n")

(* Parsing with Remainder *)
let test_parse_with_remainder () =
  check option_resp_pair_testable "Parse with remainder"
    (Some (Resp.SimpleString "OK", "extra data"))
    (Resp.parse "+OK\r\nextra data")

let test_parse_multiple_commands () =
  match Resp.parse "+PONG\r\n:42\r\n" with
  | Some (first, rest) ->
      check resp_testable "First value" (Resp.SimpleString "PONG") first;
      check string "Remainder" ":42\r\n" rest;
      (match Resp.parse rest with
       | Some (second, rest2) ->
           check resp_testable "Second value" (Resp.Integer 42) second;
           check string "Final remainder" "" rest2
       | None -> fail "Failed to parse second value")
  | None -> fail "Failed to parse first value"

(* Incomplete/Malformed Data Tests *)
let test_parse_incomplete_simple_string () =
  check option_resp_pair_testable "Incomplete SimpleString"
    None
    (Resp.parse "+OK")

let test_parse_incomplete_bulk_string_length () =
  check option_resp_pair_testable "Incomplete BulkString length"
    None
    (Resp.parse "$5")

let test_parse_incomplete_bulk_string_data () =
  check option_resp_pair_testable "Incomplete BulkString data"
    None
    (Resp.parse "$5\r\nhel")

let test_parse_incomplete_array () =
  check option_resp_pair_testable "Incomplete array"
    None
    (Resp.parse "*2\r\n$3\r\nfoo\r\n")

let test_parse_empty_string () =
  check option_resp_pair_testable "Empty string"
    None
    (Resp.parse "")

let test_parse_invalid_type () =
  check option_resp_pair_testable "Invalid type marker"
    None
    (Resp.parse "?invalid\r\n")

let test_parse_invalid_integer () =
  check option_resp_pair_testable "Invalid integer"
    None
    (Resp.parse ":notanumber\r\n")

let test_parse_invalid_bulk_length () =
  check option_resp_pair_testable "Invalid bulk string length"
    None
    (Resp.parse "$notanumber\r\nhello\r\n")

(* parse_many Tests *)
let test_parse_many_empty () =
  let (values, rest) = Resp.parse_many "" in
  check resp_list_testable "No values parsed" [] values;
  check string "No remainder" "" rest

let test_parse_many_single () =
  let (values, rest) = Resp.parse_many "+OK\r\n" in
  check resp_list_testable "Single value"
    [Resp.SimpleString "OK"]
    values;
  check string "No remainder" "" rest

let test_parse_many_multiple () =
  let (values, rest) = Resp.parse_many "+PONG\r\n:42\r\n$5\r\nhello\r\n" in
  check resp_list_testable "Multiple values"
    [Resp.SimpleString "PONG"; Resp.Integer 42; Resp.BulkString "hello"]
    values;
  check string "No remainder" "" rest

let test_parse_many_with_remainder () =
  let (values, rest) = Resp.parse_many "+OK\r\n:100\r\nincomplete" in
  check resp_list_testable "Parsed values"
    [Resp.SimpleString "OK"; Resp.Integer 100]
    values;
  check string "Remainder" "incomplete" rest

let test_parse_many_incomplete () =
  let (values, rest) = Resp.parse_many "$5\r\nhel" in
  check resp_list_testable "No values from incomplete" [] values;
  check string "All data in remainder" "$5\r\nhel" rest

(* Roundtrip Tests *)
let test_roundtrip_simple_string () =
  let original = Resp.SimpleString "test" in
  let serialized = Resp.serialize original in
  match Resp.parse serialized with
  | Some (parsed, "") ->
      check resp_testable "Roundtrip SimpleString" original parsed
  | _ -> fail "Roundtrip failed"

let test_roundtrip_bulk_string () =
  let original = Resp.BulkString "hello world" in
  let serialized = Resp.serialize original in
  match Resp.parse serialized with
  | Some (parsed, "") ->
      check resp_testable "Roundtrip BulkString" original parsed
  | _ -> fail "Roundtrip failed"

let test_roundtrip_integer () =
  let original = Resp.Integer 12345 in
  let serialized = Resp.serialize original in
  match Resp.parse serialized with
  | Some (parsed, "") ->
      check resp_testable "Roundtrip Integer" original parsed
  | _ -> fail "Roundtrip failed"

let test_roundtrip_array () =
  let original = Resp.Array [
    Resp.BulkString "SET";
    Resp.BulkString "key";
    Resp.BulkString "value"
  ] in
  let serialized = Resp.serialize original in
  match Resp.parse serialized with
  | Some (parsed, "") ->
      check resp_testable "Roundtrip Array" original parsed
  | _ -> fail "Roundtrip failed"

let test_roundtrip_nested_array () =
  let original = Resp.Array [
    Resp.Array [Resp.Integer 1; Resp.Integer 2];
    Resp.BulkString "test";
    Resp.Null
  ] in
  let serialized = Resp.serialize original in
  match Resp.parse serialized with
  | Some (parsed, "") ->
      check resp_testable "Roundtrip nested array" original parsed
  | _ -> fail "Roundtrip failed"

let test_roundtrip_null () =
  let original = Resp.Null in
  let serialized = Resp.serialize original in
  match Resp.parse serialized with
  | Some (parsed, "") ->
      check resp_testable "Roundtrip Null" original parsed
  | _ -> fail "Roundtrip failed"

(* Edge Cases *)
let test_bulk_string_with_crlf () =
  let original = Resp.BulkString "hello\r\nworld" in
  let serialized = Resp.serialize original in
  check string "BulkString with CRLF serialized correctly"
    "$12\r\nhello\r\nworld\r\n"
    serialized;
  match Resp.parse serialized with
  | Some (parsed, "") ->
      check resp_testable "BulkString with CRLF roundtrip" original parsed
  | _ -> fail "Failed to parse BulkString with embedded CRLF"

let test_large_bulk_string () =
  let large_data = String.make 10000 'x' in
  let original = Resp.BulkString large_data in
  let serialized = Resp.serialize original in
  match Resp.parse serialized with
  | Some (parsed, "") ->
      check resp_testable "Large BulkString roundtrip" original parsed
  | _ -> fail "Failed to parse large BulkString"

let test_large_array () =
  let items = List.init 100 (fun i -> Resp.Integer i) in
  let original = Resp.Array items in
  let serialized = Resp.serialize original in
  match Resp.parse serialized with
  | Some (parsed, "") ->
      check resp_testable "Large array roundtrip" original parsed
  | _ -> fail "Failed to parse large array"

let test_deeply_nested_array () =
  let rec make_nested depth =
    if depth = 0 then Resp.Integer 42
    else Resp.Array [make_nested (depth - 1)]
  in
  let original = make_nested 10 in
  let serialized = Resp.serialize original in
  match Resp.parse serialized with
  | Some (parsed, "") ->
      check resp_testable "Deeply nested array roundtrip" original parsed
  | _ -> fail "Failed to parse deeply nested array"

(* Test Suite *)
let () =
  run "RESP" [
    "serialization", [
      test_case "SimpleString" `Quick test_serialize_simple_string;
      test_case "SimpleError" `Quick test_serialize_simple_error;
      test_case "Integer" `Quick test_serialize_integer;
      test_case "Negative integer" `Quick test_serialize_negative_integer;
      test_case "Zero" `Quick test_serialize_zero;
      test_case "BulkString" `Quick test_serialize_bulk_string;
      test_case "Empty BulkString" `Quick test_serialize_empty_bulk_string;
      test_case "Null" `Quick test_serialize_null;
      test_case "Empty array" `Quick test_serialize_empty_array;
      test_case "Array" `Quick test_serialize_array;
      test_case "Nested array" `Quick test_serialize_nested_array;
      test_case "Mixed array" `Quick test_serialize_mixed_array;
    ];
    "parsing", [
      test_case "SimpleString" `Quick test_parse_simple_string;
      test_case "SimpleError" `Quick test_parse_simple_error;
      test_case "Integer" `Quick test_parse_integer;
      test_case "Negative integer" `Quick test_parse_negative_integer;
      test_case "BulkString" `Quick test_parse_bulk_string;
      test_case "Empty BulkString" `Quick test_parse_empty_bulk_string;
      test_case "Null" `Quick test_parse_null;
      test_case "Empty array" `Quick test_parse_empty_array;
      test_case "Array" `Quick test_parse_array;
      test_case "Nested array" `Quick test_parse_nested_array;
      test_case "With remainder" `Quick test_parse_with_remainder;
      test_case "Multiple commands" `Quick test_parse_multiple_commands;
    ];
    "incomplete", [
      test_case "Incomplete SimpleString" `Quick test_parse_incomplete_simple_string;
      test_case "Incomplete BulkString length" `Quick test_parse_incomplete_bulk_string_length;
      test_case "Incomplete BulkString data" `Quick test_parse_incomplete_bulk_string_data;
      test_case "Incomplete array" `Quick test_parse_incomplete_array;
      test_case "Empty string" `Quick test_parse_empty_string;
      test_case "Invalid type" `Quick test_parse_invalid_type;
      test_case "Invalid integer" `Quick test_parse_invalid_integer;
      test_case "Invalid bulk length" `Quick test_parse_invalid_bulk_length;
    ];
    "parse_many", [
      test_case "Empty" `Quick test_parse_many_empty;
      test_case "Single" `Quick test_parse_many_single;
      test_case "Multiple" `Quick test_parse_many_multiple;
      test_case "With remainder" `Quick test_parse_many_with_remainder;
      test_case "Incomplete" `Quick test_parse_many_incomplete;
    ];
    "roundtrip", [
      test_case "SimpleString" `Quick test_roundtrip_simple_string;
      test_case "BulkString" `Quick test_roundtrip_bulk_string;
      test_case "Integer" `Quick test_roundtrip_integer;
      test_case "Array" `Quick test_roundtrip_array;
      test_case "Nested array" `Quick test_roundtrip_nested_array;
      test_case "Null" `Quick test_roundtrip_null;
    ];
    "edge_cases", [
      test_case "BulkString with CRLF" `Quick test_bulk_string_with_crlf;
      test_case "Large BulkString" `Quick test_large_bulk_string;
      test_case "Large array" `Quick test_large_array;
      test_case "Deeply nested array" `Quick test_deeply_nested_array;
    ];
  ]

(* Storage unit tests *)

open Alcotest
open Codecrafters_redis

(* Custom testable for Resp.t option *)
let resp_testable = testable (fun ppf r ->
  Format.fprintf ppf "%s" (Resp.serialize r)
) (=)

let option_resp_testable = option resp_testable

(* Basic Operations Tests *)
let test_set_get () =
  let storage = Storage.create () in
  Storage.set storage "key1" (Resp.BulkString "value1") None;
  check option_resp_testable "get returns set value"
    (Some (Resp.BulkString "value1"))
    (Storage.get storage "key1")

let test_get_nonexistent () =
  let storage = Storage.create () in
  check option_resp_testable "get nonexistent returns None"
    None
    (Storage.get storage "nonexistent")

let test_overwrite () =
  let storage = Storage.create () in
  Storage.set storage "key" (Resp.BulkString "value1") None;
  Storage.set storage "key" (Resp.BulkString "value2") None;
  check option_resp_testable "overwrite updates value"
    (Some (Resp.BulkString "value2"))
    (Storage.get storage "key")

let test_delete () =
  let storage = Storage.create () in
  Storage.set storage "key" (Resp.BulkString "value") None;
  Storage.delete storage "key";
  check option_resp_testable "delete removes key"
    None
    (Storage.get storage "key")

(* Expiry Tests - Fully Deterministic with Mock Time *)
let test_expiry_before_expiration () =
  let current_time = ref 1000.0 in
  let mock_time () = !current_time in
  let storage = Storage.create ~get_time:mock_time () in

  (* Set key to expire at time 1100 *)
  Storage.set storage "key" (Resp.BulkString "value") (Some 1100.0);

  (* At time 1000, should not be expired *)
  check option_resp_testable "not expired before expiration time"
    (Some (Resp.BulkString "value"))
    (Storage.get storage "key")

let test_expiry_at_expiration () =
  let current_time = ref 1000.0 in
  let mock_time () = !current_time in
  let storage = Storage.create ~get_time:mock_time () in

  (* Set key to expire at time 1100 *)
  Storage.set storage "key" (Resp.BulkString "value") (Some 1100.0);

  (* Advance time to exactly expiration time *)
  current_time := 1100.0;

  (* At time 1100, should not be expired (> not >=) *)
  check option_resp_testable "not expired at exact expiration time"
    (Some (Resp.BulkString "value"))
    (Storage.get storage "key")

let test_expiry_after_expiration () =
  let current_time = ref 1000.0 in
  let mock_time () = !current_time in
  let storage = Storage.create ~get_time:mock_time () in

  (* Set key to expire at time 1100 *)
  Storage.set storage "key" (Resp.BulkString "value") (Some 1100.0);

  (* Advance time past expiration *)
  current_time := 1101.0;

  (* Should be expired *)
  check option_resp_testable "expired after expiration time"
    None
    (Storage.get storage "key")

let test_expiry_removes_key () =
  let current_time = ref 1000.0 in
  let mock_time () = !current_time in
  let storage = Storage.create ~get_time:mock_time () in

  Storage.set storage "key" (Resp.BulkString "value") (Some 1100.0);

  (* First get at time 1000 - not expired *)
  let _ = Storage.get storage "key" in

  (* Advance time past expiration *)
  current_time := 1101.0;

  (* First get after expiration should return None and remove key *)
  check option_resp_testable "first get after expiration returns None"
    None
    (Storage.get storage "key");

  (* Second get should also return None (key was removed) *)
  check option_resp_testable "subsequent get also returns None"
    None
    (Storage.get storage "key")

let test_no_expiry () =
  let current_time = ref 1000.0 in
  let mock_time () = !current_time in
  let storage = Storage.create ~get_time:mock_time () in

  Storage.set storage "key" (Resp.BulkString "value") None;

  (* Advance time arbitrarily far *)
  current_time := 999999.0;

  (* Should never expire *)
  check option_resp_testable "no expiry remains accessible"
    (Some (Resp.BulkString "value"))
    (Storage.get storage "key")

let test_overwrite_expiry () =
  let current_time = ref 1000.0 in
  let mock_time () = !current_time in
  let storage = Storage.create ~get_time:mock_time () in

  (* Set with expiry *)
  Storage.set storage "key" (Resp.BulkString "old") (Some 1100.0);

  (* Overwrite with no expiry *)
  Storage.set storage "key" (Resp.BulkString "new") None;

  (* Advance time past original expiration *)
  current_time := 1200.0;

  (* Should not be expired (overwrite removed expiry) *)
  check option_resp_testable "overwrite removes expiry"
    (Some (Resp.BulkString "new"))
    (Storage.get storage "key")

let test_overwrite_with_new_expiry () =
  let current_time = ref 1000.0 in
  let mock_time () = !current_time in
  let storage = Storage.create ~get_time:mock_time () in

  (* Set with expiry at 1100 *)
  Storage.set storage "key" (Resp.BulkString "old") (Some 1100.0);

  (* Overwrite with new expiry at 1200 *)
  Storage.set storage "key" (Resp.BulkString "new") (Some 1200.0);

  (* At time 1150, old expiry passed but new one hasn't *)
  current_time := 1150.0;

  (* Should use new expiry *)
  check option_resp_testable "uses new expiry time"
    (Some (Resp.BulkString "new"))
    (Storage.get storage "key");

  (* At time 1250, new expiry has passed *)
  current_time := 1250.0;

  check option_resp_testable "expires at new time"
    None
    (Storage.get storage "key")

(* Keys Listing Tests *)
let test_keys_empty () =
  let storage = Storage.create () in
  check (list string) "keys on empty storage"
    []
    (Storage.keys storage)

let test_keys_multiple () =
  let storage = Storage.create () in
  Storage.set storage "key1" (Resp.BulkString "val1") None;
  Storage.set storage "key2" (Resp.BulkString "val2") None;
  Storage.set storage "key3" (Resp.BulkString "val3") None;

  let keys = Storage.keys storage |> List.sort String.compare in
  check (list string) "keys returns all keys"
    ["key1"; "key2"; "key3"]
    keys

let test_keys_filters_expired () =
  let current_time = ref 1000.0 in
  let mock_time () = !current_time in
  let storage = Storage.create ~get_time:mock_time () in

  Storage.set storage "expired" (Resp.BulkString "val1") (Some 1050.0);
  Storage.set storage "valid1" (Resp.BulkString "val2") None;
  Storage.set storage "valid2" (Resp.BulkString "val3") (Some 1200.0);

  (* Advance time to expire first key *)
  current_time := 1100.0;

  let keys = Storage.keys storage |> List.sort String.compare in
  check (list string) "keys filters out expired"
    ["valid1"; "valid2"]
    keys

let test_keys_after_expiry_cleanup () =
  let current_time = ref 1000.0 in
  let mock_time () = !current_time in
  let storage = Storage.create ~get_time:mock_time () in

  Storage.set storage "key1" (Resp.BulkString "val") (Some 1050.0);
  Storage.set storage "key2" (Resp.BulkString "val") None;

  (* Advance time and access expired key (triggers cleanup) *)
  current_time := 1100.0;
  let _ = Storage.get storage "key1" in

  (* Keys should not include the cleaned up key *)
  let keys = Storage.keys storage |> List.sort String.compare in
  check (list string) "keys excludes cleaned up expired keys"
    ["key2"]
    keys

(* Edge Cases *)
let test_empty_key () =
  let storage = Storage.create () in
  Storage.set storage "" (Resp.BulkString "empty key value") None;
  check option_resp_testable "empty string as key"
    (Some (Resp.BulkString "empty key value"))
    (Storage.get storage "")

let test_large_value () =
  let storage = Storage.create () in
  let large_value = String.make 10000 'x' in
  Storage.set storage "large" (Resp.BulkString large_value) None;
  check option_resp_testable "large value storage"
    (Some (Resp.BulkString large_value))
    (Storage.get storage "large")

let test_many_keys () =
  let storage = Storage.create ~capacity:10 () in
  (* Insert more keys than initial capacity to test resizing *)
  for i = 0 to 99 do
    let key = "key" ^ string_of_int i in
    Storage.set storage key (Resp.BulkString ("val" ^ string_of_int i)) None
  done;

  (* Verify all keys are accessible *)
  for i = 0 to 99 do
    let key = "key" ^ string_of_int i in
    match Storage.get storage key with
    | Some (Resp.BulkString v) ->
        check string ("value for " ^ key) ("val" ^ string_of_int i) v
    | _ -> fail ("Missing key: " ^ key)
  done

let test_different_resp_types () =
  let storage = Storage.create () in
  Storage.set storage "simple" (Resp.SimpleString "simple") None;
  Storage.set storage "bulk" (Resp.BulkString "bulk") None;
  Storage.set storage "int" (Resp.Integer 42) None;
  Storage.set storage "array" (Resp.Array [Resp.BulkString "a"; Resp.BulkString "b"]) None;

  check option_resp_testable "SimpleString"
    (Some (Resp.SimpleString "simple")) (Storage.get storage "simple");
  check option_resp_testable "BulkString"
    (Some (Resp.BulkString "bulk")) (Storage.get storage "bulk");
  check option_resp_testable "Integer"
    (Some (Resp.Integer 42)) (Storage.get storage "int");
  check option_resp_testable "Array"
    (Some (Resp.Array [Resp.BulkString "a"; Resp.BulkString "b"]))
    (Storage.get storage "array")

let test_zero_expiry () =
  let current_time = ref 1000.0 in
  let mock_time () = !current_time in
  let storage = Storage.create ~get_time:mock_time () in

  (* Set with expiry at time 0 (epoch) *)
  Storage.set storage "key" (Resp.BulkString "value") (Some 0.0);

  (* Should be immediately expired *)
  check option_resp_testable "zero expiry is expired"
    None
    (Storage.get storage "key")

let test_negative_expiry () =
  let current_time = ref 1000.0 in
  let mock_time () = !current_time in
  let storage = Storage.create ~get_time:mock_time () in

  (* Set with negative expiry time *)
  Storage.set storage "key" (Resp.BulkString "value") (Some (-100.0));

  (* Should be immediately expired *)
  check option_resp_testable "negative expiry is expired"
    None
    (Storage.get storage "key")

(* Test Suite *)
let () =
  run "Storage" [
    "basic", [
      test_case "set/get" `Quick test_set_get;
      test_case "get nonexistent" `Quick test_get_nonexistent;
      test_case "overwrite" `Quick test_overwrite;
      test_case "delete" `Quick test_delete;
    ];
    "expiry", [
      test_case "before expiration" `Quick test_expiry_before_expiration;
      test_case "at expiration" `Quick test_expiry_at_expiration;
      test_case "after expiration" `Quick test_expiry_after_expiration;
      test_case "removes key" `Quick test_expiry_removes_key;
      test_case "no expiry" `Quick test_no_expiry;
      test_case "overwrite expiry" `Quick test_overwrite_expiry;
      test_case "overwrite with new expiry" `Quick test_overwrite_with_new_expiry;
      test_case "zero expiry" `Quick test_zero_expiry;
      test_case "negative expiry" `Quick test_negative_expiry;
    ];
    "keys", [
      test_case "empty storage" `Quick test_keys_empty;
      test_case "multiple keys" `Quick test_keys_multiple;
      test_case "filters expired" `Quick test_keys_filters_expired;
      test_case "after cleanup" `Quick test_keys_after_expiry_cleanup;
    ];
    "edge_cases", [
      test_case "empty key" `Quick test_empty_key;
      test_case "large value" `Quick test_large_value;
      test_case "many keys" `Quick test_many_keys;
      test_case "different RESP types" `Quick test_different_resp_types;
    ];
  ]

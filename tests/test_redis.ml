open Alcotest

let test_encode_and_decode_empty_database () =
  let redis =
    Redis.init ~replication_role:Redis.master ~dbfilename:"" ~dir:"" ()
  in
  let redis_str = Redis.to_string redis in
  let maybe_other_redis =
    Redis.of_string redis_str ~dbfilename:"" ~dir:""
      ~replication_role:Redis.master
  in
  check bool "successful deserialization" (Result.is_ok maybe_other_redis) true;
  let module OrderedStringDouble = struct
    type t = string * string

    let compare (k1, v1) (k2, v2) =
      let c1 = String.compare k1 k2 in
      match c1 with 0 -> compare v1 v2 | _ -> c1
  end in
  let module S = Set.Make (OrderedStringDouble) in
  let other_redis = maybe_other_redis |> Result.get_ok in
  let metadata_redis = redis |> Redis.get_all_metadata |> S.of_seq in
  let metadata_other_redis =
    other_redis |> Redis.get_all_metadata |> S.of_seq
  in
  check bool "Equal metadata" (S.equal metadata_redis metadata_other_redis) true

let test_encode_and_decode_database_with_string () =
  let open Redis in
  let redis = init ~dbfilename:"" ~dir:"" ~replication_role:master () in
  let set_command =
    Command.Set
      {
        index = 0;
        key = "foo";
        value = Resp.RBulkString "bar";
        now = Int64.of_int 0;
        duration = None;
      }
  in
  let client_id, _ = connect redis in
  let _ = handle_command client_id redis set_command in
  let redis_str = to_string redis in
  let maybe_other_redis =
    of_string redis_str ~replication_role:master ~dir:"" ~dbfilename:""
  in
  print_endline (String.escaped redis_str);
  let _ = Result.map_error print_endline maybe_other_redis in
  check bool "successfully decoded" (Result.is_ok maybe_other_redis) true

let test_encode_and_decode_database_with_string_with_miliseconds_duration () =
  let open Redis in
  let redis = init ~dbfilename:"" ~dir:"" ~replication_role:master () in
  let set_command =
    Command.Set
      {
        index = 0;
        key = "foo";
        value = Resp.RBulkString "bar";
        now = Int64.of_int 0;
        duration = Some (Int64.of_int 123);
      }
  in
  let client_id, _ = connect redis in
  let _ = handle_command client_id redis set_command in
  let redis_str = to_string redis in
  let maybe_other_redis =
    of_string redis_str ~replication_role:master ~dir:"" ~dbfilename:""
  in
  print_endline (String.escaped redis_str);
  let _ = Result.map_error print_endline maybe_other_redis in
  check bool "successfully decoded" (Result.is_ok maybe_other_redis) true

let test_encode_and_decode_database_with_string_with_seconds_duration () =
  let open Redis in
  let redis = init ~dbfilename:"" ~dir:"" ~replication_role:master () in
  let set_command =
    Command.Set
      {
        index = 0;
        key = "foo";
        value = Resp.RBulkString "bar";
        now = Int64.of_int 0;
        duration = Some (Int64.of_int 123000);
      }
  in
  let client_id, _ = connect redis in
  let _ = handle_command client_id redis set_command in
  let redis_str = to_string redis in
  let maybe_other_redis =
    of_string redis_str ~replication_role:master ~dir:"" ~dbfilename:""
  in
  print_endline (String.escaped redis_str);
  let _ = Result.map_error print_endline maybe_other_redis in
  check bool "successfully decoded" (Result.is_ok maybe_other_redis) true

let () =
  run "Redis tests"
    [
      ( "serialization and deserialization of empty database",
        [
          test_case "encode and decode empty database" `Quick
            test_encode_and_decode_empty_database;
        ] );
      ( "serialization and deserialization of a non empty database",
        [
          test_case "encode and decode database with string without duration"
            `Quick test_encode_and_decode_database_with_string;
          test_case
            "encode and decode database with string with miliseconds duration"
            `Quick
            test_encode_and_decode_database_with_string_with_miliseconds_duration;
          test_case
            "encode and decode database with string with seconds duration"
            `Quick
            test_encode_and_decode_database_with_string_with_seconds_duration;
        ] );
    ]

(* Database unit tests (public API only)

   These tests intentionally exercise only Database's public surface.
*)

open Alcotest
open Codecrafters_redis

let mk_user_conn () =
  (* Dummy channels; Database.handle_user_resp needs them for some commands.
     For our unit tests we don't rely on actual IO happening. *)
  let ic_r, ic_w = Lwt_io.pipe () in
  let oc_r, oc_w = Lwt_io.pipe () in
  (* Close unused ends to avoid leaks. *)
  Lwt.async (fun () -> Lwt_io.close ic_w);
  Lwt.async (fun () -> Lwt_io.close oc_r);
  Peer_conn.user ~ic:ic_r ~oc:oc_w ~addr:"test-client"

let resp_cmd_of (cmd : Command.t) : Resp.t =
  match cmd with
  | Command.Ping -> Resp.Array [ Resp.BulkString "PING" ]
  | Command.Echo (Resp.BulkString s) ->
      Resp.Array [ Resp.BulkString "ECHO"; Resp.BulkString s ]
  | Command.Get k -> Resp.Array [ Resp.BulkString "GET"; Resp.BulkString k ]
  | Command.Set { key; value = Resp.BulkString v; expiry_ms = None } ->
      Resp.Array
        [ Resp.BulkString "SET"; Resp.BulkString key; Resp.BulkString v ]
  | Command.InfoReplication ->
      Resp.Array [ Resp.BulkString "INFO"; Resp.BulkString "replication" ]
  | _ -> failwith "unsupported in unit test"

let run_user (db : Database.t) (cmd : Resp.t) : Database.user_step =
  let conn = mk_user_conn () in
  match
    Lwt_main.run
      (Database.handle_user_resp db cmd ~current_time:1000.0 conn)
  with
  | Ok step -> step
  | Error `ReadOnlyReplica -> fail "unexpected READONLY"
  | Error (#Command.error) -> fail "unexpected command error"

let test_role_master () =
  let db = Database.create Config.default in
  check bool "as_replica = None" true (Option.is_none (Database.as_replica db))

let test_role_replica () =
  let config = { Config.default with replicaof = Some ("localhost", 6379) } in
  let db = Database.create config in
  check bool "as_replica = Some" true (Option.is_some (Database.as_replica db))

let test_master_set_get () =
  let db = Database.create Config.default in
  (match run_user db (resp_cmd_of (Command.Set { key = "k"; value = Resp.BulkString "v"; expiry_ms = None })) with
  | `Reply r ->
      check string "SET -> OK" (Resp.serialize (Resp.SimpleString "OK"))
        (Resp.serialize r)
  | _ -> fail "expected reply");
  (match run_user db (resp_cmd_of (Command.Get "k")) with
  | `Reply r ->
      check string "GET -> v" (Resp.serialize (Resp.BulkString "v"))
        (Resp.serialize r)
  | _ -> fail "expected reply")

let test_replica_rejects_user_writes () =
  let config = { Config.default with replicaof = Some ("localhost", 6379) } in
  let db = Database.create config in
  let conn = mk_user_conn () in
  match
    Lwt_main.run
      (Database.handle_user_resp db
         (resp_cmd_of
            (Command.Set { key = "k"; value = Resp.BulkString "v"; expiry_ms = None }))
         ~current_time:1000.0 conn)
  with
  | Error `ReadOnlyReplica -> ()
  | _ -> fail "expected READONLY"

let test_info_replication_master () =
  let db = Database.create Config.default in
  match run_user db (resp_cmd_of Command.InfoReplication) with
  | `Reply (Resp.BulkString info) ->
      check bool "contains role:master" true
        (String.contains info 'm')
  | _ -> fail "expected bulkstring reply"

let suite () =
  [
    ( "role",
      [
        test_case "master role" `Quick test_role_master;
        test_case "replica role" `Quick test_role_replica;
      ] );
    ( "user-policy",
      [
        test_case "master set/get" `Quick test_master_set_get;
        test_case "replica rejects user writes" `Quick
          test_replica_rejects_user_writes;
      ] );
    ( "info",
      [ test_case "INFO replication on master" `Quick test_info_replication_master ] );
  ]

let () = Alcotest.run "Database" (suite ())

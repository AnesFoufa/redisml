(* Test helpers for database testing *)
open Lwt.Syntax

type running_db =
  | Master of Codecrafters_redis.Database.master Codecrafters_redis.Database.t
  | Replica of
      Codecrafters_redis.Database.connected_replica
      Codecrafters_redis.Database.t

let initialize_db created_db =
  Lwt_main.run (
    match created_db with
    | Codecrafters_redis.Database.CreatedMaster master_db ->
        Lwt.return (Ok (Master master_db))
    | Codecrafters_redis.Database.CreatedReplica replica_db ->
        let* result = Codecrafters_redis.Database.connect_replica replica_db in
        Lwt.return (Result.map (fun db -> Replica db) result)
  )

let create_db config =
  Unix.putenv "CODECRAFTERS_REDIS_SKIP_REPLICA_CONNECT" "1";
  match initialize_db (Codecrafters_redis.Database.create config) with
  | Ok db -> db
  | Error `FailedConnection -> failwith "Failed to connect replica in tests"

(* Create mock Lwt channels that discard output *)
let create_mock_channels () =
  let (ic_dummy, oc_dummy) = Lwt_io.pipe () in
  Lwt.return (ic_dummy, oc_dummy, "127.0.0.1:0")

(* Execute a RESP command synchronously for testing *)
let exec_command resp_cmd db ~current_time =
  Lwt_main.run (
    let* (ic, oc, address) = create_mock_channels () in
    match Codecrafters_redis.Command.parse resp_cmd with
    | Error _ -> Lwt.return Codecrafters_redis.Resp.Null
    | Ok parsed ->
        let handle_master_result result =
          match result with
          | Codecrafters_redis.Database.Respond resp -> Lwt.return resp
          | Codecrafters_redis.Database.ConnectionTakenOver ->
              let* data = Lwt_io.read ~count:4096 ic in
              (match Codecrafters_redis.Resp.parse data with
               | Some (resp, _) -> Lwt.return resp
               | None -> Lwt.return Codecrafters_redis.Resp.Null)
          | Codecrafters_redis.Database.Silent ->
              Lwt.return Codecrafters_redis.Resp.Null
        in
        let master_command : type a. Codecrafters_redis.Database.master Codecrafters_redis.Database.t -> a Codecrafters_redis.Command.t -> Codecrafters_redis.Resp.t Lwt.t =
          fun master_db cmd ->
            let* result =
              Codecrafters_redis.Database.handle_master_command master_db cmd
                ~current_time ~original_resp:resp_cmd ~ic ~oc ~address
            in
            handle_master_result result
        in
        match db with
        | Master master_db ->
            (match parsed with
             | Codecrafters_redis.Command.Read cmd -> master_command master_db cmd
             | Codecrafters_redis.Command.Write cmd -> master_command master_db cmd)
        | Replica replica_db ->
            (match parsed with
             | Codecrafters_redis.Command.Read cmd ->
                 Codecrafters_redis.Database.handle_replica_command replica_db cmd
                   ~current_time
             | Codecrafters_redis.Command.Write _ ->
                 Lwt.return (Codecrafters_redis.Resp.SimpleError "ERR write command not allowed on replica"))
  )

(* Check if database is in replica role by examining INFO output *)
let is_replica db =
  let resp_cmd = Codecrafters_redis.Resp.Array [
    Codecrafters_redis.Resp.BulkString "INFO";
    Codecrafters_redis.Resp.BulkString "replication"
  ] in
  let result = exec_command resp_cmd db ~current_time:0.0 in
  match result with
  | Codecrafters_redis.Resp.BulkString info ->
      String.contains info 's' && (
        try
          let _ = Str.search_forward (Str.regexp "role:slave") info 0 in
          true
        with Not_found -> false
      )
  | _ -> false

(* Check if database is in master role by examining INFO output *)
let is_master db =
  let result = exec_command
    (Codecrafters_redis.Resp.Array [
      Codecrafters_redis.Resp.BulkString "INFO";
      Codecrafters_redis.Resp.BulkString "replication"
    ]) db ~current_time:0.0 in
  match result with
  | Codecrafters_redis.Resp.BulkString info ->
      (try
        let _ = Str.search_forward (Str.regexp "role:master") info 0 in
        true
      with Not_found -> false)
  | _ -> false

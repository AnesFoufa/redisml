(* Database implementation *)

type t = {
  storage : Storage.t;
  config : Config.t;
  replica_manager : Replica_manager.t;
  mutable replication_offset : int;  (* Bytes processed from master *)
}

let create config = {
  storage = Storage.create ();
  config;
  replica_manager = Replica_manager.create ();
  replication_offset = 0;
}

let get_config db = db.config

let is_replica db = Config.is_replica db.config

(* Execute a command and return response *)
let execute_command cmd db =
  let storage = db.storage in
  match cmd with
  | Command.Ping ->
      Resp.SimpleString "PONG"

  | Command.Echo msg ->
      msg

  | Command.Get key -> (
      match Storage.get storage key with
      | Some resp_value -> resp_value
      | None -> Resp.Null)

  | Command.Set (key, value, duration_ms) ->
      let expires_at = match duration_ms with
        | Some ms -> Some (Unix.gettimeofday () +. (float ms /. 1000.0))
        | None -> None
      in
      Storage.set storage key value expires_at;
      Resp.SimpleString "OK"

  | Command.InfoReplication ->
      let role = if is_replica db then "slave" else "master" in
      let info = Printf.sprintf "# Replication\nrole:%s\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0" role in
      Resp.BulkString info

  | Command.Replconf replconf_cmd -> (
      match replconf_cmd with
      | Command.ReplconfGetAck ->
          (* Respond with REPLCONF ACK <offset> *)
          Resp.Array [
            Resp.BulkString "REPLCONF";
            Resp.BulkString "ACK";
            Resp.BulkString (string_of_int db.replication_offset)
          ]
      | _ ->
          (* For other REPLCONF commands, return OK *)
          Resp.SimpleString "OK")

  | Command.Psync (_replid, _offset) ->
      Resp.SimpleString "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"

(* Determine if command should be propagated *)
let should_propagate_command db cmd =
  match cmd with
  | Command.Set _ when not (is_replica db) -> true
  | _ -> false

(* Handle command with I/O and return optional response *)
let handle_command db cmd ~original_resp ~channel ~address =
  let open Lwt.Syntax in
  match cmd with
  | Command.Psync _ ->
      (* PSYNC: Send FULLRESYNC response, then handle RDB + replica in background *)
      let response = execute_command cmd db in
      let* () = Lwt_io.write channel (Resp.serialize response) in
      let* () = Lwt_io.flush channel in

      (* Spawn background task to send RDB and register replica *)
      let _ = Lwt.async (fun () ->
        (* Send RDB file *)
        let rdb_response =
          Printf.sprintf "$%d\r\n%s"
            (String.length Replica_manager.empty_rdb)
            Replica_manager.empty_rdb
        in
        let* () = Lwt_io.write channel rdb_response in
        let* () = Lwt_io.flush channel in

        (* Register replica for command propagation *)
        Replica_manager.register_replica db.replica_manager ~channel ~address
      ) in
      Lwt.return_none  (* Response already sent *)

  | _ ->
      (* Normal command: execute and return response *)
      let response = execute_command cmd db in

      (* Propagate to replicas if this is a write command on master *)
      let* () =
        if should_propagate_command db cmd then
          Replica_manager.propagate_to_replicas db.replica_manager ~command:original_resp
        else
          Lwt.return_unit
      in

      Lwt.return_some response

(* Increment replication offset (called when processing commands from master) *)
let increment_offset db bytes =
  db.replication_offset <- db.replication_offset + bytes

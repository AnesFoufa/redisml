(* Database implementation with phantom types *)

(* Phantom type markers *)
type master
type connected_replica
type disconnected_replica

(* Shared replica state - same record used for both connected and disconnected *)
type replica_state = { mutable replication_offset : int }

(* GADT-style role variant that links phantom type with concrete data *)
type _ role =
  | Master : Replica_manager.t -> master role
  | DisconnectedReplica : replica_state -> disconnected_replica role
  | ConnectedReplica : replica_state -> connected_replica role

(* Phantom typed database *)
type 'a t = {
  storage : Storage.t;
  config : Config.t;
  role : 'a role;
}

(* Result of create - only two possibilities *)
type created_db =
  | CreatedMaster of master t
  | CreatedReplica of disconnected_replica t

type replica_connection_error = [ `FailedConnection ]

let replica_connection_error_to_string = function
  | `FailedConnection -> "failed to connect to master"

let create cfg =
  let storage = Storage.create () in

  (* Load RDB file if configured *)
  (match cfg.Config.dir, cfg.Config.dbfilename with
   | Some dir, Some dbfilename ->
       let filepath = Filename.concat dir dbfilename in
       (match Rdb.parse_file filepath with
        | Ok pairs ->
            List.iter (fun (key, _value, expiry) ->
              Storage.set storage key _value expiry
            ) pairs
        | Error err ->
            Printf.eprintf "Warning: Failed to load RDB file: %s\n%!"
              (Rdb.error_to_string err))
   | _ -> ());

  (* Create appropriate database based on configuration *)
  if Config.is_replica cfg then
    CreatedReplica {
      storage;
      config = cfg;
      role = DisconnectedReplica { replication_offset = 0 };
    }
  else
    CreatedMaster {
      storage;
      config = cfg;
      role = Master (Replica_manager.create ());
    }

let get_config db = db.config

let promote_disconnected_replica (db : disconnected_replica t) =
  let (DisconnectedReplica state) = db.role in
  {
    storage = db.storage;
    config = db.config;
    role = ConnectedReplica state;
  }

(* Execute a command and return response - works for any role and command type *)
let execute_command : type cmd db. cmd Command.t -> db t -> current_time:float -> Resp.t =
  fun cmd db ~current_time ->
  let storage = db.storage in
  match cmd with
  | Command.Ping ->
      Resp.SimpleString "PONG"

  | Command.Echo msg ->
      msg

  | Command.Get key -> (
      match Storage.get storage ~current_time key with
      | Some resp_value -> resp_value
      | None -> Resp.Null)

  | Command.Set { key; value; expiry_ms } ->
      let expires_at = match expiry_ms with
        | Some ms -> Some (current_time +. (float ms /. 1000.0))
        | None -> None
      in
      Storage.set storage key value expires_at;
      Resp.SimpleString "OK"

  | Command.InfoReplication ->
      let role_str = match db.role with
        | Master _ -> "master"
        | DisconnectedReplica _ -> "slave"
        | ConnectedReplica _ -> "slave"
      in
      let info = Printf.sprintf "# Replication\nrole:%s\nmaster_replid:%s\nmaster_repl_offset:%d" role_str Constants.master_repl_id Constants.master_repl_offset in
      Resp.BulkString info

  | Command.Replconf replconf_cmd -> (
      match replconf_cmd with
      | Command.ReplconfGetAck ->
          (* Respond with REPLCONF ACK <offset> *)
          (match db.role with
           | ConnectedReplica { replication_offset } ->
               Resp.Array [
                 Resp.BulkString "REPLCONF";
                 Resp.BulkString "ACK";
                 Resp.BulkString (string_of_int replication_offset)
               ]
           | DisconnectedReplica { replication_offset } ->
               Resp.Array [
                 Resp.BulkString "REPLCONF";
                 Resp.BulkString "ACK";
                 Resp.BulkString (string_of_int replication_offset)
               ]
           | Master _ ->
               Resp.SimpleError "ERR GETACK received on master")
      | _ ->
          (* For other REPLCONF commands, return OK *)
          Resp.SimpleString "OK")

  | Command.Psync { replication_id = _; offset = _ } ->
      (match db.role with
       | Master _ ->
           Resp.SimpleString (Printf.sprintf "FULLRESYNC %s %d" Constants.master_repl_id Constants.master_repl_offset)
       | DisconnectedReplica _ | ConnectedReplica _ ->
           Resp.SimpleError "ERR PSYNC not allowed on replica")

  | Command.Wait _ ->
      (* WAIT is handled specially in handle_command because it needs async I/O *)
      Resp.SimpleError "ERR WAIT must be handled via handle_command"

  | Command.ConfigGet param ->
      let param_name, param_value = match param with
        | Command.Dir -> "dir", Option.value ~default:"" db.config.dir
        | Command.Dbfilename -> "dbfilename", Option.value ~default:"" db.config.dbfilename
      in
      Resp.Array [Resp.BulkString param_name; Resp.BulkString param_value]

  | Command.Keys _pattern ->
      (* For now, only support "*" pattern (return all keys) *)
      let keys = Storage.keys db.storage ~current_time in
      Resp.Array (List.map (fun k -> Resp.BulkString k) keys)

(* Determine if command should be propagated - master only *)
let should_propagate_command : type a. a Command.t -> bool = function
  | Command.Set _ -> true
  | _ -> false

type command_result =
  | Respond of Resp.t
  | Silent
  | ConnectionTakenOver

(* Handle command on master database *)
let handle_master_command : type a. master t -> a Command.t -> current_time:float -> original_resp:Resp.t -> ic:Lwt_io.input_channel -> oc:Lwt_io.output_channel -> address:string -> command_result Lwt.t =
  fun db cmd ~current_time ~original_resp ~ic ~oc ~address ->
  let open Lwt.Syntax in
  let (Master replica_manager) = db.role in
  match cmd with
  | Command.Psync _ ->
      (* PSYNC: Send FULLRESYNC response, then RDB, then register replica *)
      let response = execute_command cmd db ~current_time in
      let* () = Lwt_io.write oc (Resp.serialize response) in
      let* () = Lwt_io.flush oc in

      (* Send RDB file *)
      let rdb_response =
        Printf.sprintf "$%d\r\n%s"
          (String.length Replica_manager.empty_rdb)
          Replica_manager.empty_rdb
      in
      let* () = Lwt_io.write oc rdb_response in
      let* () = Lwt_io.flush oc in

      (* Register replica for command propagation *)
      let* () = Replica_manager.register_replica replica_manager ~ic ~oc ~address in

      Lwt.return ConnectionTakenOver

  | Command.Wait { num_replicas; timeout_ms } ->
      (* WAIT: Send GETACK to replicas and wait for responses *)
      (* Use a longer timeout if the provided one is small to account for multiple replicas *)
      let effective_timeout = max timeout_ms Constants.min_wait_timeout_ms in
      let* num_acked = Replica_manager.wait_for_replicas replica_manager ~num_replicas ~timeout_ms:effective_timeout in
      Lwt.return (Respond (Resp.Integer num_acked))

  | _ ->
      (* Normal command: execute and return response *)
      let response = execute_command cmd db ~current_time in

      (* Propagate to replicas if this is a write command *)
      let* () =
        if should_propagate_command cmd then begin
          Replica_manager.propagate_to_replicas replica_manager ~command:original_resp
        end else
          Lwt.return_unit
      in

      Lwt.return (Respond response)

(* Handle command on connected replica database - only accepts read commands *)
let handle_replica_command (db : connected_replica t) (cmd : Command.read Command.t) ~current_time =
  Lwt.return (execute_command cmd db ~current_time)

(* Increment replication offset - connected replica only *)
let increment_offset db bytes =
  let (ConnectedReplica state) = db.role in
  state.replication_offset <- state.replication_offset + bytes

(* Connect to master and perform handshake *)
let connect_to_master ~(database : disconnected_replica t) ~host ~master_port ~replica_port =
  let open Lwt.Syntax in
  let connected_db = promote_disconnected_replica database in
  match Sys.getenv_opt "CODECRAFTERS_REDIS_SKIP_REPLICA_CONNECT" with
  | Some "1" -> Lwt.return connected_db
  | _ ->
  let* () = Lwt_io.eprintlf "Connecting to master at %s:%d" host master_port in

  (* Resolve hostname to IP address *)
  let inet_addr =
    try
      Unix.inet_addr_of_string host
    with Failure _ ->
      let host_entry = Unix.gethostbyname host in
      host_entry.Unix.h_addr_list.(0)
  in

  let master_addr = Unix.ADDR_INET (inet_addr, master_port) in
  let* (ic, oc) = Lwt_io.open_connection master_addr in

  (* Step 1: Send PING *)
  let ping_cmd = Resp.Array [Resp.BulkString "PING"] in
  let* () = Lwt_io.write oc (Resp.serialize ping_cmd) in
  let* () = Lwt_io.flush oc in

  (* Read PONG response *)
  let* _pong = Lwt_io.read ~count:1024 ic in
  let* () = Lwt_io.eprintlf "Received PONG from master" in

  (* Step 2: Send REPLCONF listening-port *)
  let replconf_port_cmd = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "listening-port";
    Resp.BulkString (string_of_int replica_port)
  ] in
  let* () = Lwt_io.write oc (Resp.serialize replconf_port_cmd) in
  let* () = Lwt_io.flush oc in

  (* Read OK response *)
  let* _ok1 = Lwt_io.read ~count:1024 ic in
  let* () = Lwt_io.eprintlf "Received OK from REPLCONF listening-port" in

  (* Step 3: Send REPLCONF capa psync2 *)
  let replconf_capa_cmd = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "capa";
    Resp.BulkString "psync2"
  ] in
  let* () = Lwt_io.write oc (Resp.serialize replconf_capa_cmd) in
  let* () = Lwt_io.flush oc in

  (* Read OK response *)
  let* _ok2 = Lwt_io.read ~count:1024 ic in
  let* () = Lwt_io.eprintlf "Received OK from REPLCONF capa" in

  (* Step 4: Send PSYNC ? -1 (request full sync) *)
  let psync_cmd = Resp.Array [
    Resp.BulkString "PSYNC";
    Resp.BulkString "?";
    Resp.BulkString "-1"
  ] in
  let* () = Lwt_io.write oc (Resp.serialize psync_cmd) in
  let* () = Lwt_io.flush oc in

  (* Helper: Read and parse FULLRESYNC response *)
  let read_fullresync_response reader ic =
    let* result = Buffer_reader.read_until_parsed reader ic ~count:4096 ~parser:Resp.parse in
    match result with
    | Ok _resp ->
        Lwt_io.eprintlf "Received FULLRESYNC from master"
    | Error `Eof ->
        Lwt.fail_with "Master connection closed before FULLRESYNC"
    | Error `BufferOverflow ->
        Lwt.fail_with "FULLRESYNC response too large"
  in

  (* Helper: Read RDB file from buffer *)
  let read_rdb_file ic buffer =
    let rec read_header () =
      let contents = Buffer.contents buffer in
      if String.length contents > 0 && contents.[0] = '$' then (
        try
          let newline_pos = String.index contents '\n' in
          let length_str = String.sub contents 1 (newline_pos - 2) in
          let rdb_length = int_of_string length_str in
          let remaining = String.sub contents (newline_pos + 1) (String.length contents - newline_pos - 1) in
          Buffer.clear buffer;
          Buffer.add_string buffer remaining;
          Lwt.return rdb_length
        with Not_found ->
          let* data = Lwt_io.read ~count:4096 ic in
          Buffer.add_string buffer data;
          read_header ()
      ) else (
        let* data = Lwt_io.read ~count:4096 ic in
        Buffer.add_string buffer data;
        read_header ()
      )
    in
    let rec read_data needed =
      let contents = Buffer.contents buffer in
      if String.length contents >= needed then (
        let remaining = String.sub contents needed (String.length contents - needed) in
        Buffer.clear buffer;
        Buffer.add_string buffer remaining;
        let* () = Lwt_io.eprintlf "Received RDB file (%d bytes)" needed in
        Lwt.return ()
      ) else (
        let* data = Lwt_io.read ~count:4096 ic in
        Buffer.add_string buffer data;
        read_data needed
      )
    in
    let* rdb_length = read_header () in
    read_data rdb_length
  in

  (* Helper: Process commands already in buffer *)
  let process_buffered_commands reader oc =
    Buffer_reader.process_all_buffered reader ~parser:Resp.parse ~f:(fun cmd ->
      let cmd_bytes = String.length (Resp.serialize cmd) in
      match Command.parse cmd with
      | Ok (Command.Read read_cmd) ->
          let current_time = Unix.gettimeofday () in
          let* resp =
            handle_replica_command connected_db read_cmd ~current_time
          in
          increment_offset connected_db cmd_bytes;
          (match read_cmd with
           | Command.Replconf _ ->
               let* () = Lwt_io.write oc (Resp.serialize resp) in
               Lwt_io.flush oc
           | _ -> Lwt.return_unit)
      | Ok (Command.Write write_cmd) ->
          let current_time = Unix.gettimeofday () in
          let _resp = execute_command write_cmd connected_db ~current_time in
          increment_offset connected_db cmd_bytes;
          Lwt.return_unit
      | Error _ -> Lwt.return_unit
    )
  in

  (* Spawn background task to handle replication stream *)
  let _ = Lwt.async (fun () ->
    let reader = Buffer_reader.create () in
    let buffer = Buffer.create 4096 in  (* For RDB reading which has custom format *)

    (* Read FULLRESYNC response *)
    let* () = read_fullresync_response reader ic in

    (* Read RDB file - copy data from reader to buffer for custom RDB parsing *)
    let () =
      let contents = Buffer_reader.contents reader in
      Buffer.add_string buffer contents;
      Buffer_reader.reset reader None
    in
    let* () = read_rdb_file ic buffer in

    (* Copy any remaining data from RDB buffer back to reader *)
    let () =
      let remaining = Buffer.contents buffer in
      Buffer.clear buffer;
      Buffer_reader.reset reader (Some remaining)
    in

    (* Continuously read and process commands from master *)
    let rec replication_loop () =
      Lwt.catch
        (fun () ->
          (* Process any commands already in buffer *)
          let* () = process_buffered_commands reader oc in

          (* Read more data from master *)
          let* result = Buffer_reader.read_from_channel reader ic ~count:4096 in
          match result with
          | `Eof ->
              Lwt_io.eprintlf "Master connection closed"
          | `BufferOverflow ->
              Lwt_io.eprintlf "Master sent too much data"
          | `Ok ->
              replication_loop ()
        )
        (fun exn ->
          Lwt_io.eprintlf "Error processing master commands: %s" (Printexc.to_string exn))
    in
    replication_loop ()
  ) in

  Lwt.return connected_db

let connect_replica (db : disconnected_replica t) =
  let open Lwt.Syntax in
  Lwt.catch
    (fun () ->
      let* connected_db =
        match db.config.Config.replicaof with
        | Some (host, master_port) ->
            connect_to_master ~database:db ~host ~master_port
              ~replica_port:db.config.Config.port
        | None ->
            Lwt.return (promote_disconnected_replica db)
      in
      Lwt.return (Ok connected_db))
    (fun _exn ->
      Lwt.return (Error `FailedConnection))

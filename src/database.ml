open Typed_state

type master = Typed_state.master

type replica = Typed_state.replica

type 'r db = 'r Typed_state.database

type t = Typed_state.any_database

(* Create a new database with the given configuration *)
let create (config : Config.t) : t =
  let storage = Storage.create () in

  (* Load RDB file if configured *)
  (match config.dir, config.dbfilename with
   | Some dir, Some dbfilename ->
       let filepath = Filename.concat dir dbfilename in
       (match Rdb.parse_file filepath with
        | Ok pairs ->
            List.iter
              (fun (key, value, expiry) -> Storage.set storage key value expiry)
              pairs
        | Error err ->
            Printf.eprintf "Warning: Failed to load RDB file: %s\n%!"
              (Rdb.error_to_string err))
   | _ -> ());

  if Config.is_replica config then
    AnyDb
      {
        storage;
        config;
        role = Replica_state { offset = 0 };
      }
  else
    AnyDb
      {
        storage;
        config;
        role = Master_state { replicas = Replica_manager.create () };
      }

let get_config (AnyDb db) = db.config

let _is_replica (AnyDb db) =
  match db.role with
  | Replica_state _ -> true
  | Master_state _ -> false

let as_master (AnyDb db) =
  match db.role with
  | Master_state _ -> Some (db : master db)
  | Replica_state _ -> None

let as_replica (AnyDb db) =
  match db.role with
  | Replica_state _ -> Some (db : replica db)
  | Master_state _ -> None

let as_master_exn t =
  match as_master t with
  | Some db -> db
  | None -> failwith "Expected master database"

let as_replica_exn t =
  match as_replica t with
  | Some db -> db
  | None -> failwith "Expected replica database"

(* Execute a command and return response *)
let execute_command (cmd : Command.t) (AnyDb db) ~current_time : Resp.t =
  let storage = db.storage in
  match cmd with
  | Command.Ping -> Resp.SimpleString "PONG"
  | Command.Echo msg -> msg
  | Command.Get key -> (
      match Storage.get storage ~current_time key with
      | Some resp_value -> resp_value
      | None -> Resp.Null)
  | Command.Set { key; value; expiry_ms } ->
      let expires_at =
        match expiry_ms with
        | Some ms -> Some (current_time +. (float ms /. 1000.0))
        | None -> None
      in
      Storage.set storage key value expires_at;
      Resp.SimpleString "OK"
  | Command.InfoReplication ->
      let role_str =
        match db.role with
        | Master_state _ -> "master"
        | Replica_state _ -> "slave"
      in
      let info =
        Printf.sprintf "# Replication\nrole:%s\nmaster_replid:%s\nmaster_repl_offset:%d"
          role_str Constants.master_repl_id Constants.master_repl_offset
      in
      Resp.BulkString info
  | Command.Replconf replconf_cmd -> (
      match replconf_cmd with
      | Command.ReplconfGetAck -> (
          match db.role with
          | Replica_state { offset } ->
              Resp.Array
                [
                  Resp.BulkString "REPLCONF";
                  Resp.BulkString "ACK";
                  Resp.BulkString (string_of_int offset);
                ]
          | Master_state _ -> Resp.SimpleError "ERR GETACK received on master")
      | _ -> Resp.SimpleString "OK")
  | Command.Psync { replication_id = _; offset = _ } -> (
      match db.role with
      | Master_state _ ->
          Resp.SimpleString
            (Printf.sprintf "FULLRESYNC %s %d" Constants.master_repl_id
               Constants.master_repl_offset)
      | Replica_state _ -> Resp.SimpleError "ERR PSYNC not allowed on replica")
  | Command.Wait _ ->
      (* WAIT is handled specially in handle_command because it needs async I/O *)
      Resp.SimpleError "ERR WAIT must be handled via handle_command"
  | Command.ConfigGet param ->
      let param_name, param_value =
        match param with
        | Command.Dir -> "dir", Option.value ~default:"" db.config.dir
        | Command.Dbfilename ->
            "dbfilename", Option.value ~default:"" db.config.dbfilename
      in
      Resp.Array [ Resp.BulkString param_name; Resp.BulkString param_value ]
  | Command.Keys _pattern ->
      let keys = Storage.keys db.storage ~current_time in
      Resp.Array (List.map (fun k -> Resp.BulkString k) keys)

(* Determine if command should be propagated (master-only) *)
let should_propagate_command (_db : master db) (cmd : Command.t) : bool =
  match cmd with
  | Command.Set _ -> true
  | _ -> false

(* Handle command with I/O and return optional response *)
let handle_command (t : t) cmd ~current_time ~original_resp ~ic ~oc ~address =
  let open Lwt.Syntax in
  match t with
  | AnyDb db -> (
      match cmd with
      | Command.Psync _ -> (
          match db.role with
          | Master_state { replicas } ->
              let response = execute_command cmd t ~current_time in
              let* () = Lwt_io.write oc (Resp.serialize response) in
              let* () = Lwt_io.flush oc in

              (* Send RDB file *)
              let rdb_response =
                Printf.sprintf "$%d\r\n%s" (String.length Replica_manager.empty_rdb)
                  Replica_manager.empty_rdb
              in
              let* () = Lwt_io.write oc rdb_response in
              let* () = Lwt_io.flush oc in

              let* () = Replica_manager.register_replica replicas ~ic ~oc ~address in
              Lwt.return_none
          | Replica_state _ ->
              Lwt.return_some (Resp.SimpleError "ERR PSYNC not allowed on replica"))

      | Command.Wait { num_replicas; timeout_ms } -> (
          match db.role with
          | Master_state { replicas } ->
              let effective_timeout = max timeout_ms Constants.min_wait_timeout_ms in
              let* num_acked =
                Replica_manager.wait_for_replicas replicas ~num_replicas
                  ~timeout_ms:effective_timeout
              in
              Lwt.return_some (Resp.Integer num_acked)
          | Replica_state _ ->
              Lwt.return_some (Resp.SimpleError "ERR WAIT not allowed on replica"))

      | _ ->
          let response = execute_command cmd t ~current_time in

          let* () =
            match db.role with
            | Master_state { replicas } ->
                let master_db = (db : master db) in
                if should_propagate_command master_db cmd then
                  let* () = Lwt_io.eprintlf "Propagating command to replicas" in
                  Replica_manager.propagate_to_replicas replicas ~command:original_resp
                else Lwt.return_unit
            | Replica_state _ -> Lwt.return_unit
          in

          Lwt.return_some response)

type user_step = [ `Reply of Resp.t | `NoReply | `Takeover ]

let handle_user_resp (t : t) (resp_cmd : Resp.t) ~current_time ~ic ~oc ~address =
  let open Lwt.Syntax in
  match t with
  | AnyDb db -> (
      match db.role with
      | Master_state _ -> (
          (* For now, masters accept full Command.t parsing/handling. *)
          match Command.parse resp_cmd with
          | Error e -> Lwt.return (Error e)
          | Ok cmd ->
              let* resp_opt =
                handle_command t cmd ~current_time ~original_resp:resp_cmd ~ic ~oc ~address
              in
              (match resp_opt, cmd with
              | None, Command.Psync _ -> Lwt.return (Ok `Takeover)
              | None, _ -> Lwt.return (Ok `NoReply)
              | Some r, _ -> Lwt.return (Ok (`Reply r))))
      | Replica_state _ -> (
          match User_command.parse_for_replica resp_cmd with
          | Ok read_cmd ->
              let cmd = User_command.to_command read_cmd in
              let* resp_opt =
                handle_command t cmd ~current_time ~original_resp:resp_cmd ~ic ~oc ~address
              in
              (match resp_opt with
              | None -> Lwt.return (Ok `NoReply)
              | Some r -> Lwt.return (Ok (`Reply r)))
          | Error `ReadOnlyReplica -> Lwt.return (Error `ReadOnlyReplica)
          | Error (#Command.error as e) -> Lwt.return (Error e)))

(* Handle a RESP value received from the upstream master (replication stream). *)
let handle_replication_resp (db : replica db) (resp_cmd : Resp.t) ~current_time ~ic ~oc =
  match Command.parse resp_cmd with
  | Error e -> Lwt.return (Error e)
  | Ok cmd -> (
      match cmd with
      | Command.Set _params ->
          (* Apply replicated writes to local storage. *)
          let t : t = AnyDb db in
          let _resp = execute_command cmd t ~current_time in
          let _ = ic and _ = oc in
          Lwt.return (Ok None)
      | Command.Replconf Command.ReplconfGetAck ->
          (* Reply with ACK <offset>. *)
          let t : t = AnyDb db in
          let resp = execute_command cmd t ~current_time in
          let _ = ic and _ = oc in
          Lwt.return (Ok (Some resp))
      | Command.Ping ->
          (* Ignore pings in stream. *)
          let _ = ic and _ = oc in
          Lwt.return (Ok None)
      | _ ->
          let _ = ic and _ = oc in
          Lwt.return (Error `DisallowedFromMaster))

(* Increment replication offset (replica-only) *)
let increment_offset (db : replica db) (bytes : int) : unit =
  match db.role with
  | Replica_state state -> state.offset <- state.offset + bytes
  | Master_state _ ->
      (* Unreachable by types, kept only for exhaustiveness under strict warnings. *)
      ()

open Typed_state

(* Re-exported type aliases (per database.mli) *)
type master = Typed_state.master

type replica = Typed_state.replica

type 'r db = 'r Typed_state.database

type server = Master of master db | Replica of replica db

(* Create a new database with the given configuration *)
let create (config : Config.t) : server =
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
    Replica (make_replica ~storage ~config)
  else
    Master (make_master ~storage ~config)

let get_config (server : server) : Config.t =
  match server with
  | Master db -> config db
  | Replica db -> config db

(* ===== Pure execution (role-aware) ===== *)

let execute_common ~(storage : Storage.t) ~(config : Config.t) (cmd : Command.t)
    ~current_time : Resp.t =
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
  | Command.ConfigGet param ->
      let param_name, param_value =
        match param with
        | Command.Dir -> "dir", Option.value ~default:"" config.dir
        | Command.Dbfilename -> "dbfilename", Option.value ~default:"" config.dbfilename
      in
      Resp.Array [ Resp.BulkString param_name; Resp.BulkString param_value ]
  | Command.Keys _pattern ->
      let keys = Storage.keys storage ~current_time in
      Resp.Array (List.map (fun k -> Resp.BulkString k) keys)
  | _ -> Resp.SimpleError "ERR unsupported command"

let execute_master (db : master db) (cmd : Command.t) ~current_time : Resp.t =
  let storage = storage db in
  let cfg = config db in
  match cmd with
  | Command.InfoReplication ->
      let info =
        Printf.sprintf "# Replication\nrole:%s\nmaster_replid:%s\nmaster_repl_offset:%d"
          "master" Constants.master_repl_id Constants.master_repl_offset
      in
      Resp.BulkString info
  | Command.Psync { replication_id = _; offset = _ } ->
      Resp.SimpleString
        (Printf.sprintf "FULLRESYNC %s %d" Constants.master_repl_id
           Constants.master_repl_offset)
  | Command.Replconf replconf_cmd -> (
      match replconf_cmd with
      | Command.ReplconfGetAck -> Resp.SimpleError "ERR GETACK received on master"
      | _ -> Resp.SimpleString "OK")
  | Command.Wait _ ->
      (* WAIT is handled specially in handle_command because it needs async I/O *)
      Resp.SimpleError "ERR WAIT must be handled via handle_command"
  | _ -> execute_common ~storage ~config:cfg cmd ~current_time

let execute_replica (db : replica db) (cmd : Command.t) ~current_time : Resp.t =
  let storage = storage db in
  let cfg = config db in
  match cmd with
  | Command.InfoReplication ->
      let info =
        Printf.sprintf "# Replication\nrole:%s\nmaster_replid:%s\nmaster_repl_offset:%d"
          "slave" Constants.master_repl_id Constants.master_repl_offset
      in
      Resp.BulkString info
  | Command.Psync _ -> Resp.SimpleError "ERR PSYNC not allowed on replica"
  | Command.Wait _ -> Resp.SimpleError "ERR WAIT not allowed on replica"
  | Command.Replconf replconf_cmd -> (
      match replconf_cmd with
      | Command.ReplconfGetAck ->
          let offset = replica_offset db in
          Resp.Array
            [
              Resp.BulkString "REPLCONF";
              Resp.BulkString "ACK";
              Resp.BulkString (string_of_int offset);
            ]
      | _ -> Resp.SimpleString "OK")
  | _ -> execute_common ~storage ~config:cfg cmd ~current_time

(* Determine if command should be propagated (master-only) *)
let should_propagate_command (_db : master db) (cmd : Command.t) : bool =
  match cmd with
  | Command.Set _ -> true
  | _ -> false

(* ===== Internal command handler (NOT EXPORTED) ===== *)

let handle_command_master (db : master db) cmd ~current_time ~original_resp ~ic ~oc
    ~address =
  let open Lwt.Syntax in
  match cmd with
  | Command.Psync _ ->
      (* PSYNC: Send FULLRESYNC response, then RDB, then register replica *)
      let response = execute_master db cmd ~current_time in
      let* () = Lwt_io.write oc (Resp.serialize response) in
      let* () = Lwt_io.flush oc in

      let rdb_response =
        Printf.sprintf "$%d\r\n%s" (String.length Replica_manager.empty_rdb)
          Replica_manager.empty_rdb
      in
      let* () = Lwt_io.write oc rdb_response in
      let* () = Lwt_io.flush oc in

      let* () = Replica_manager.register_replica (replicas db) ~ic ~oc ~address in
      Lwt.return_none

  | Command.Wait { num_replicas; timeout_ms } ->
      let effective_timeout = max timeout_ms Constants.min_wait_timeout_ms in
      let* num_acked =
        Replica_manager.wait_for_replicas (replicas db) ~num_replicas
          ~timeout_ms:effective_timeout
      in
      Lwt.return_some (Resp.Integer num_acked)

  | _ ->
      let response = execute_master db cmd ~current_time in

      let* () =
        if should_propagate_command db cmd then
          let* () = Lwt_io.eprintlf "Propagating command to replicas" in
          Replica_manager.propagate_to_replicas (replicas db) ~command:original_resp
        else Lwt.return_unit
      in

      Lwt.return_some response

let handle_command_replica (db : replica db) cmd ~current_time ~original_resp ~ic ~oc
    ~address =
  let _ = original_resp and _ = address in
  (* Replicas do not propagate writes to other replicas in this implementation. *)
  let response = execute_replica db cmd ~current_time in
  let _ = ic and _ = oc in
  Lwt.return_some response

(* ===== Public safe entrypoints ===== *)

type user_step = [ `Reply of Resp.t | `NoReply | `Takeover ]

let handle_user_resp_master (db : master db) (resp_cmd : Resp.t) ~current_time
    (conn : Peer_conn.user Peer_conn.t) =
  let ic = Peer_conn.ic conn in
  let oc = Peer_conn.oc conn in
  let address = Peer_conn.addr conn in
  let open Lwt.Syntax in
  match Command.parse resp_cmd with
  | Error e -> Lwt.return (Error e)
  | Ok cmd ->
      let* resp_opt =
        handle_command_master db cmd ~current_time ~original_resp:resp_cmd ~ic ~oc ~address
      in
      (match resp_opt, cmd with
      | None, Command.Psync _ -> Lwt.return (Ok `Takeover)
      | None, _ -> Lwt.return (Ok `NoReply)
      | Some r, _ -> Lwt.return (Ok (`Reply r)))

let handle_user_resp_replica (db : replica db) (resp_cmd : Resp.t) ~current_time
    (conn : Peer_conn.user Peer_conn.t) =
  let ic = Peer_conn.ic conn in
  let oc = Peer_conn.oc conn in
  let address = Peer_conn.addr conn in
  let open Lwt.Syntax in
  match User_command.parse_for_replica resp_cmd with
  | Ok read_cmd ->
      let cmd = User_command.to_command read_cmd in
      let* resp_opt =
        handle_command_replica db cmd ~current_time ~original_resp:resp_cmd ~ic ~oc ~address
      in
      (match resp_opt with
      | None -> Lwt.return (Ok `NoReply)
      | Some r -> Lwt.return (Ok (`Reply r)))
  | Error `ReadOnlyReplica -> Lwt.return (Error `ReadOnlyReplica)
  | Error (#Command.error as e) -> Lwt.return (Error e)

let handle_replication_resp (db : replica db) (resp_cmd : Resp.t) ~current_time
    (_conn : Peer_conn.upstream_master Peer_conn.t) =
  match Command.parse resp_cmd with
  | Error e -> Lwt.return (Error e)
  | Ok cmd -> (
      match cmd with
      | Command.Set _params ->
          let _resp = execute_replica db cmd ~current_time in
          Lwt.return (Ok None)
      | Command.Replconf Command.ReplconfGetAck ->
          let resp = execute_replica db cmd ~current_time in
          Lwt.return (Ok (Some resp))
      | Command.Ping -> Lwt.return (Ok None)
      | _ -> Lwt.return (Error `DisallowedFromMaster))

(* Increment replication offset (replica-only) *)
let increment_offset (db : replica db) (bytes : int) : unit =
  set_replica_offset db (replica_offset db + bytes)

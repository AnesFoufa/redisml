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

(* ===== Typed execution functions ===== *)

(* Execute a read command - works on any role *)
let execute_read : type r. r db -> User_command.read User_command.t -> current_time:float -> Resp.t =
  fun db cmd ~current_time ->
    let st = storage db in
    let cfg = config db in
    (* GADT exhaustiveness: Set : write t cannot appear when cmd : read t *)
    (match[@warning "-8"] cmd with
    | User_command.Ping -> Resp.SimpleString "PONG"
    | User_command.Echo msg -> msg
    | User_command.Get key -> (
        match Storage.get st ~current_time key with
        | Some resp_value -> resp_value
        | None -> Resp.Null)
    | User_command.InfoReplication ->
        let role_str = if is_master db then "master" else "slave" in
        let info =
          Printf.sprintf "# Replication\nrole:%s\nmaster_replid:%s\nmaster_repl_offset:%d"
            role_str Constants.master_repl_id Constants.master_repl_offset
        in
        Resp.BulkString info
    | User_command.ConfigGet param ->
        let param_name, param_value =
          match param with
          | Command.Dir -> "dir", Option.value ~default:"" cfg.dir
          | Command.Dbfilename -> "dbfilename", Option.value ~default:"" cfg.dbfilename
        in
        Resp.Array [ Resp.BulkString param_name; Resp.BulkString param_value ]
    | User_command.Keys _pattern ->
        let keys = Storage.keys st ~current_time in
        Resp.Array (List.map (fun k -> Resp.BulkString k) keys))

(* Execute a write command - master only (compile-time enforced) *)
let execute_write : master db -> User_command.write User_command.t -> current_time:float -> Resp.t =
  fun db cmd ~current_time ->
    let st = storage db in
    (* GADT exhaustiveness: read constructors cannot appear when cmd : write t *)
    (match[@warning "-8"] cmd with
    | User_command.Set { key; value; expiry_ms } ->
        let expires_at =
          match expiry_ms with
          | Some ms -> Some (current_time +. (float ms /. 1000.0))
          | None -> None
        in
        Storage.set st key value expires_at;
        Resp.SimpleString "OK")

(* Internal: apply a SET to replica storage (for replication stream) *)
let apply_set_to_replica (db : replica db) (params : Command.set_params)
    ~current_time : unit =
  let st = storage db in
  let expires_at =
    match params.expiry_ms with
    | Some ms -> Some (current_time +. (float ms /. 1000.0))
    | None -> None
  in
  Storage.set st params.key params.value expires_at

(* Type-indexed response for replication commands *)
type _ repl_response =
  | Must_respond : Resp.t -> Repl_command.response_required repl_response
  | No_response : Repl_command.no_response repl_response

(* Execute a replication command - replica only (compile-time enforced) *)
let execute_repl : type r. replica db -> r Repl_command.t -> current_time:float -> r repl_response =
  fun db cmd ~current_time ->
    match cmd with
    | Repl_command.Apply_set params ->
        apply_set_to_replica db params ~current_time;
        No_response
    | Repl_command.Replconf_getack ->
        let offset = replica_offset db in
        let resp =
          Resp.Array
            [
              Resp.BulkString "REPLCONF";
              Resp.BulkString "ACK";
              Resp.BulkString (string_of_int offset);
            ]
        in
        Must_respond resp
    | Repl_command.Ignore ->
        No_response

(* Determine if command should be propagated (master-only) *)
let should_propagate : User_command.write User_command.t -> bool = fun cmd ->
  (* GADT exhaustiveness: read constructors cannot appear when cmd : write t *)
  (match[@warning "-8"] cmd with
  | User_command.Set _ -> true)

(* ===== Internal command handler for master-specific commands ===== *)

let handle_psync (db : master db) ~ic ~oc ~address =
  let open Lwt.Syntax in
  let response =
    Resp.SimpleString
      (Printf.sprintf "FULLRESYNC %s %d" Constants.master_repl_id
         Constants.master_repl_offset)
  in
  let* () = Lwt_io.write oc (Resp.serialize response) in
  let* () = Lwt_io.flush oc in

  let rdb_response =
    Printf.sprintf "$%d\r\n%s" (String.length Replica_manager.empty_rdb)
      Replica_manager.empty_rdb
  in
  let* () = Lwt_io.write oc rdb_response in
  let* () = Lwt_io.flush oc in

  let* () = Replica_manager.register_replica (replicas db) ~ic ~oc ~address in
  Lwt.return `Takeover

let handle_wait (db : master db) ~num_replicas ~timeout_ms =
  let open Lwt.Syntax in
  let effective_timeout = max timeout_ms Constants.min_wait_timeout_ms in
  let* num_acked =
    Replica_manager.wait_for_replicas (replicas db) ~num_replicas
      ~timeout_ms:effective_timeout
  in
  Lwt.return (`Reply (Resp.Integer num_acked))

(* ===== Public safe entrypoints ===== *)

type user_step = [ `Reply of Resp.t | `NoReply | `Takeover ]

let handle_user_resp_master (db : master db) (resp_cmd : Resp.t) ~current_time
    (conn : Peer_conn.user Peer_conn.t) =
  let ic = Peer_conn.ic conn in
  let oc = Peer_conn.oc conn in
  let address = Peer_conn.addr conn in
  let open Lwt.Syntax in
  match Master_command.parse resp_cmd with
  | Error e -> Lwt.return (Error e)
  | Ok (Master_command.Packed cmd) -> (
      match cmd with
      | Master_command.Psync _params ->
          let* step = handle_psync db ~ic ~oc ~address in
          Lwt.return (Ok step)
      | Master_command.Wait { num_replicas; timeout_ms } ->
          let* step = handle_wait db ~num_replicas ~timeout_ms in
          Lwt.return (Ok step)
      | Master_command.Replconf _ ->
          Lwt.return (Ok (`Reply (Resp.SimpleString "OK")))
      | Master_command.Read read_cmd ->
          let response = execute_read db read_cmd ~current_time in
          Lwt.return (Ok (`Reply response))
      | Master_command.Write write_cmd ->
          let response = execute_write db write_cmd ~current_time in
          let* () =
            if should_propagate write_cmd then
              let* () = Lwt_io.eprintlf "Propagating command to replicas" in
              Replica_manager.propagate_to_replicas (replicas db) ~command:resp_cmd
            else Lwt.return_unit
          in
          Lwt.return (Ok (`Reply response)))

let handle_user_resp_replica (db : replica db) (resp_cmd : Resp.t) ~current_time
    (conn : Peer_conn.user Peer_conn.t) =
  let _ = conn in
  match User_command.parse_for_replica resp_cmd with
  | Ok read_cmd ->
      let response = execute_read db read_cmd ~current_time in
      Lwt.return (Ok (`Reply response))
  | Error `ReadOnlyReplica -> Lwt.return (Error `ReadOnlyReplica)
  | Error (#Command.error as e) -> Lwt.return (Error e)

(* Increment replication offset (replica-only) *)
let increment_offset (db : replica db) (bytes : int) : unit =
  set_replica_offset db (replica_offset db + bytes)

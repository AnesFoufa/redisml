open Lwt.Syntax
open Codecrafters_redis

type running_db =
  | Master of Database.master Database.t
  | Replica of Database.connected_replica Database.t

(* Centralized error handler - converts any module error to string *)
let error_to_string (err : [< Command.error | Config.error | Rdb.error ]) :
    string =
  match err with
  (* Command errors *)
  | `UnknownCommand -> "unknown command"
  | `MalformedCommand msg -> msg
  | `InvalidPort p -> Printf.sprintf "invalid port: %d" p
  | `InvalidExpiry e -> Printf.sprintf "invalid expiry: %d" e
  | `InvalidTimeout t -> Printf.sprintf "invalid timeout: %d" t
  | `InvalidNumReplicas n -> Printf.sprintf "invalid num replicas: %d" n
  (* Config errors *)
  | `InvalidReplicaofFormat s ->
      Printf.sprintf "invalid --replicaof format: %s" s
  | `InvalidArgument arg -> Printf.sprintf "invalid argument: %s" arg
  (* RDB errors *)
  | `FileNotFound path -> Printf.sprintf "file not found: %s" path
  | `InvalidHeader -> "invalid RDB header"
  | `UnexpectedEOF ctx -> Printf.sprintf "unexpected EOF: %s" ctx
  | `CorruptedData ctx -> Printf.sprintf "corrupted data: %s" ctx
  | `UnsupportedEncoding enc -> Printf.sprintf "unsupported encoding: %d" enc
  | `UnsupportedValueType typ -> Printf.sprintf "unsupported value type: %d" typ
  | `IoError msg -> Printf.sprintf "I/O error: %s" msg

(* Handle a single command - delegates to Database module *)
let handle_command db resp_cmd ic oc addr =
  let open Lwt.Syntax in
  match Command.parse resp_cmd with
  | Ok parsed -> (
      let current_time = Unix.gettimeofday () in
      let master_command : type a. Database.master Database.t -> a Command.t -> Database.command_result Lwt.t =
        fun db cmd ->
          Database.handle_master_command db cmd ~current_time
            ~original_resp:resp_cmd ~ic ~oc ~address:addr
      in
      let* result = match db with
        | Master master_db ->
            (match parsed with
             | Command.Read cmd -> master_command master_db cmd
             | Command.Write cmd -> master_command master_db cmd)
        | Replica replica_db ->
            (match parsed with
             | Command.Read cmd ->
                 let* resp = Database.handle_replica_command replica_db cmd ~current_time in
                 Lwt.return (Database.Respond resp)
             | Command.Write _ ->
                 Lwt.return (Database.Respond (Resp.SimpleError "ERR write command not allowed on replica")))
      in
      match result with
      | Database.ConnectionTakenOver ->
          raise Protocol.Replication_takeover
      | Database.Respond resp ->
          let* () = Lwt_io.write oc (Resp.serialize resp) in
          Lwt_io.flush oc
      | Database.Silent -> Lwt.return_unit)
  | Error err ->
      let error_msg = error_to_string err in
      let* () =
        Lwt_io.write oc (Resp.serialize (Resp.SimpleError ("ERR " ^ error_msg)))
      in
      Lwt_io.flush oc

(* Start the server *)
let start_server db =
  let config = match db with
    | Master db -> Database.get_config db
    | Replica db -> Database.get_config db
  in
  let* () =
    Lwt_io.eprintlf "Starting Redis server on port %d" config.Config.port
  in

  let listen_addr = Unix.ADDR_INET (Unix.inet_addr_any, config.Config.port) in

  (* Create server that properly handles connections *)
  Lwt_io.establish_server_with_client_address listen_addr
    (fun client_addr (ic, oc) ->
      Protocol.handle_connection ~client_addr ~channels:(ic, oc)
        ~command_handler:(handle_command db) ())

(* Main entry point *)
let () =
  let parsed_config = Config.parse_args_or_default Sys.argv in
  let created_db = Database.create parsed_config in

  Lwt_main.run
    (Lwt.catch
       (fun () ->
         (* Initialize database - connects replica to master if needed *)
         let init_db created_db : (running_db, Database.replica_connection_error) result Lwt.t =
           match created_db with
           | Database.CreatedMaster master_db ->
               Lwt.return (Ok (Master master_db))
           | Database.CreatedReplica replica_db ->
               Database.connect_replica replica_db
               |> Lwt.map (Result.map (fun db -> Replica db))
         in
         let* running_db =
           let* init_result = init_db created_db in
           match init_result with
           | Ok db -> Lwt.return db
           | Error err ->
               Lwt.fail_with (Database.replica_connection_error_to_string err)
         in
         let* _server = start_server running_db in
         (* Wait forever *)
         let forever, _ = Lwt.wait () in
        forever)
       (fun exn ->
         let* () = Lwt_io.eprintlf "Fatal error: %s" (Printexc.to_string exn) in
         Lwt.return_unit))

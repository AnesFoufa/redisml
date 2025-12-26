open Lwt.Syntax

(* Global state *)
let storage = Storage.create ()
let config = ref Config.default

(* Replica tracking *)
let replicas : Lwt_io.output_channel list ref = ref []

(* Handle a single command and return the response *)
let handle_command resp_cmd =
  match Command.parse resp_cmd with
  | Some cmd -> Command.execute cmd storage !config
  | None -> Resp.SimpleError "ERR unknown command"

(* Propagate command to all connected replicas *)
let propagate_to_replicas cmd_resp =
  let cmd_str = Resp.serialize cmd_resp in
  let rec send_to_all remaining successful =
    match remaining with
    | [] ->
        replicas := successful;
        Lwt.return_unit
    | replica_oc :: rest ->
        Lwt.catch
          (fun () ->
            let* () = Lwt_io.write replica_oc cmd_str in
            let* () = Lwt_io.flush replica_oc in
            send_to_all rest (replica_oc :: successful))
          (fun _exn ->
            (* Replica disconnected, skip it *)
            send_to_all rest successful)
  in
  send_to_all !replicas []

(* Register a new replica connection *)
let register_replica oc addr =
  replicas := oc :: !replicas;
  Lwt_io.eprintlf "Registered replica from %s" addr

(* Handle a client connection *)
let handle_connection client_addr (ic, oc) =
  let addr_str = match client_addr with
    | Unix.ADDR_INET (addr, port) ->
        Printf.sprintf "%s:%d" (Unix.string_of_inet_addr addr) port
    | Unix.ADDR_UNIX s -> s
  in
  (* Buffer for incomplete messages *)
  let buffer = ref "" in

  let rec loop () =
    Lwt.catch
      (fun () ->
        let* data = Lwt_io.read ~count:4096 ic in
        if String.length data = 0 then
          (* Connection closed *)
          Lwt.return_unit
        else (
          (* Add to buffer *)
          buffer := !buffer ^ data;

          (* Empty RDB file (hex encoded) - minimal valid RDB *)
          let empty_rdb = "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2" in

          (* Try to parse and process commands from buffer *)
          let rec process_buffer () =
            match Resp.parse !buffer with
            | Some (cmd, rest) ->
                (* Successfully parsed a command *)
                buffer := rest;
                let response = handle_command cmd in

                (* Propagate write commands to replicas if we're the master *)
                let* () =
                  match Command.parse cmd with
                  | Some (Command.Set _) when not (Config.is_replica !config) ->
                      propagate_to_replicas cmd
                  | _ -> Lwt.return_unit
                in

                let response_str = Resp.serialize response in
                let* () = Lwt_io.write oc response_str in
                let* () = Lwt_io.flush oc in

                (* Check if this was a PSYNC command - need to send RDB file after *)
                let* () =
                  match Command.parse cmd with
                  | Some (Command.Psync _) ->
                      (* Send empty RDB file as bulk string *)
                      let rdb_response = Printf.sprintf "$%d\r\n%s" (String.length empty_rdb) empty_rdb in
                      let* () = Lwt_io.write oc rdb_response in
                      let* () = Lwt_io.flush oc in
                      (* Register this connection as a replica *)
                      register_replica oc addr_str
                  | _ -> Lwt.return_unit
                in
                process_buffer ()
            | None ->
                (* Incomplete command, wait for more data *)
                Lwt.return_unit
          in

          let* () = process_buffer () in
          loop ()
        ))
      (fun _exn ->
        (* Connection error or closed - remove from replica list if present *)
        replicas := List.filter (fun r -> r != oc) !replicas;
        Lwt.return_unit)
  in
  loop ()

(* Connect to master and perform handshake *)
let connect_to_master host master_port replica_port =
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

  (* Read FULLRESYNC response *)
  let* _fullresync = Lwt_io.read ~count:1024 ic in
  let* () = Lwt_io.eprintlf "Received FULLRESYNC from master" in

  Lwt.return_unit

(* Start the server *)
let start_server config =
  let* () = Lwt_io.eprintlf "Starting Redis server on port %d" config.Config.port in

  (* If running as replica, connect to master *)
  let* () =
    match config.Config.replicaof with
    | Some (host, master_port) -> connect_to_master host master_port config.Config.port
    | None -> Lwt.return_unit
  in

  let listen_addr = Unix.ADDR_INET (Unix.inet_addr_any, config.Config.port) in

  (* Create server that properly handles connections *)
  Lwt_io.establish_server_with_client_address listen_addr (fun client_addr (ic, oc) ->
      handle_connection client_addr (ic, oc)
  )

(* Main entry point *)
let () =
  config := Config.parse_args Sys.argv;

  Lwt_main.run (
    Lwt.catch
      (fun () ->
        let* _server = start_server !config in
        (* Wait forever *)
        let forever, _ = Lwt.wait () in
        forever)
      (fun exn ->
        let* () = Lwt_io.eprintlf "Fatal error: %s" (Printexc.to_string exn) in
        Lwt.return_unit)
  )

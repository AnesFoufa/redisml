open Lwt.Syntax

(* Global state *)
let storage = Storage.create ()
let config = ref Config.default

(* Handle a single command and return the response *)
let handle_command resp_cmd =
  match Command.parse resp_cmd with
  | Some cmd -> Command.execute cmd storage !config
  | None -> Resp.SimpleError "ERR unknown command"

(* Handle a client connection *)
let handle_connection (ic, oc) =
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

          (* Try to parse and process commands from buffer *)
          let rec process_buffer () =
            match Resp.parse !buffer with
            | Some (cmd, rest) ->
                (* Successfully parsed a command *)
                buffer := rest;
                let response = handle_command cmd in
                let response_str = Resp.serialize response in
                let* () = Lwt_io.write oc response_str in
                let* () = Lwt_io.flush oc in
                process_buffer ()
            | None ->
                (* Incomplete command, wait for more data *)
                Lwt.return_unit
          in

          let* () = process_buffer () in
          loop ()
        ))
      (fun _exn ->
        (* Connection error or closed *)
        Lwt.return_unit)
  in
  loop ()

(* Connect to master and perform handshake *)
let connect_to_master host port =
  let* () = Lwt_io.eprintlf "Connecting to master at %s:%d" host port in

  (* Resolve hostname to IP address *)
  let inet_addr =
    try
      Unix.inet_addr_of_string host
    with Failure _ ->
      let host_entry = Unix.gethostbyname host in
      host_entry.Unix.h_addr_list.(0)
  in

  let master_addr = Unix.ADDR_INET (inet_addr, port) in
  let* (ic, oc) = Lwt_io.open_connection master_addr in

  (* Send PING *)
  let ping_cmd = Resp.Array [Resp.BulkString "PING"] in
  let* () = Lwt_io.write oc (Resp.serialize ping_cmd) in
  let* () = Lwt_io.flush oc in

  (* Read PONG response *)
  let* _response = Lwt_io.read ~count:1024 ic in
  let* () = Lwt_io.eprintlf "Received response from master" in

  Lwt.return_unit

(* Start the server *)
let start_server config =
  let* () = Lwt_io.eprintlf "Starting Redis server on port %d" config.Config.port in

  (* If running as replica, connect to master *)
  let* () =
    match config.Config.replicaof with
    | Some (host, port) -> connect_to_master host port
    | None -> Lwt.return_unit
  in

  let listen_addr = Unix.ADDR_INET (Unix.inet_addr_any, config.Config.port) in

  (* Create server that properly handles connections *)
  Lwt_io.establish_server_with_client_address listen_addr (fun _client_addr (ic, oc) ->
      handle_connection (ic, oc)
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

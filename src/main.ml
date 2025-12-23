open Lwt.Syntax

(* Handle a single command and return the response *)
let handle_command = function
  | Resp.Array [ Resp.BulkString cmd ] when String.lowercase_ascii cmd = "ping" ->
      Resp.SimpleString "PONG"
  | Resp.Array (Resp.BulkString cmd :: args)
    when String.lowercase_ascii cmd = "echo" -> (
      match args with
      | [ Resp.BulkString msg ] -> Resp.BulkString msg
      | _ -> Resp.SimpleError "ERR wrong number of arguments for 'echo' command")
  | _ -> Resp.SimpleError "ERR unknown command"

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

(* Start the server *)
let start_server port =
  let* () = Lwt_io.eprintlf "Starting Redis server on port %d" port in

  let listen_addr = Unix.ADDR_INET (Unix.inet_addr_any, port) in

  (* Create server that properly handles connections *)
  Lwt_io.establish_server_with_client_address listen_addr (fun _client_addr (ic, oc) ->
      handle_connection (ic, oc)
  )

(* Main entry point *)
let () =
  Lwt_main.run (
    Lwt.catch
      (fun () ->
        let* _server = start_server 6379 in
        (* Wait forever *)
        let forever, _ = Lwt.wait () in
        forever)
      (fun exn ->
        let* () = Lwt_io.eprintlf "Fatal error: %s" (Printexc.to_string exn) in
        Lwt.return_unit)
  )

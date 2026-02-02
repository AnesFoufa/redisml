open Lwt.Syntax

(* Next-step requested by the command handler. *)
type handler_step = [ `Continue | `Takeover ]

(* Callback types *)
type command_handler = Resp.t -> Peer_conn.user Peer_conn.t -> handler_step Lwt.t

(* Handle a client connection *)
let handle_connection ~client_addr ~channels:(ic, oc)
    ~command_handler () =

  let addr_str = match client_addr with
    | Unix.ADDR_INET (addr, port) ->
        Printf.sprintf "%s:%d" (Unix.string_of_inet_addr addr) port
    | Unix.ADDR_UNIX s -> s
  in

  (* Buffer reader for incomplete messages with DOS protection *)
  let reader = Buffer_reader.create ~max_size:(Some Constants.protocol_max_buffer) () in

  let conn = Peer_conn.user ~ic ~oc ~addr:addr_str in

  let rec drain_buffer () =
    match Buffer_reader.parse_next reader ~parser:Resp.parse with
    | None -> Lwt.return `Continue
    | Some cmd ->
        let* step = command_handler cmd conn in
        (match step with
        | `Continue -> drain_buffer ()
        | `Takeover -> Lwt.return `Takeover)
  in

  let rec loop () =
    let* result =
      Buffer_reader.read_from_channel reader ic ~count:Constants.protocol_read_buffer
    in
    match result with
    | `Eof ->
        (* Connection closed *)
        Lwt.return_unit
    | `BufferOverflow ->
        (* Buffer too large - disconnect client *)
        let* () =
          Lwt_io.eprintlf "Client %s exceeded buffer limit, disconnecting" addr_str
        in
        Lwt.return_unit
    | `Ok -> (
        let* step = drain_buffer () in
        match step with
        | `Continue -> loop ()
        | `Takeover ->
            (* Connection is now a replica - keep channels open but stop reading *)
            let forever, _ = Lwt.wait () in
            forever)
  in

  (* Wrap in finalize to ensure channels are closed on error *)
  Lwt.finalize
    (fun () -> loop ())
    (fun () ->
      Lwt.catch
        (fun () ->
          let* () = Lwt_io.close ic in
          Lwt_io.close oc)
        (fun _exn ->
          (* Ignore errors during cleanup *)
          Lwt.return_unit))

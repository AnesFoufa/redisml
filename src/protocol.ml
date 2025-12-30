open Lwt.Syntax

(* Exception to signal that connection should be handed over to replication *)
exception Replication_takeover

(* Callback types *)
type command_handler = Resp.t -> Lwt_io.input_channel -> Lwt_io.output_channel -> string -> unit Lwt.t

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

  let rec loop () =
    Lwt.catch
      (fun () ->
        let* result = Buffer_reader.read_from_channel reader ic ~count:Constants.protocol_read_buffer in
        match result with
        | `Eof ->
            (* Connection closed *)
            Lwt.return_unit
        | `BufferOverflow ->
            (* Buffer too large - disconnect client *)
            let* () = Lwt_io.eprintlf "Client %s exceeded buffer limit, disconnecting" addr_str in
            Lwt.return_unit
        | `Ok ->
            (* Process all complete commands in buffer *)
            let* () = Buffer_reader.process_all_buffered reader
              ~parser:Resp.parse
              ~f:(fun cmd -> command_handler cmd ic oc addr_str)
            in
            loop ()
        )
      (fun exn ->
        match exn with
        | Replication_takeover ->
            (* Connection is now a replica - keep channels open but stop reading *)
            (* Sleep forever without closing connection *)
            let forever, _ = Lwt.wait () in
            forever
        | _ ->
            (* Connection error or closed *)
            Lwt.return_unit)
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

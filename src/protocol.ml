open Lwt.Syntax

(* Callback types *)
type command_handler = Resp.t -> Lwt_io.output_channel -> string -> unit Lwt.t

(* Handle a client connection *)
let handle_connection ~client_addr ~channels:(ic, oc)
    ~command_handler () =

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

          (* Try to parse and process commands from buffer *)
          let rec process_buffer () =
            match Resp.parse !buffer with
            | Some (cmd, rest) ->
                (* Successfully parsed a command *)
                buffer := rest;

                (* Execute command - handler sends response *)
                let* () = command_handler cmd oc addr_str in

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

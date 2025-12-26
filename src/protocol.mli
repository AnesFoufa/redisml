(* Redis protocol connection handler *)

(* Callback type for command processing - receives command, output channel, and client address.
   The handler is responsible for sending the response. *)
type command_handler = Resp.t -> Lwt_io.output_channel -> string -> unit Lwt.t

(* Handle a client connection - reads commands, processes, and sends responses *)
val handle_connection :
  client_addr:Unix.sockaddr ->
  channels:(Lwt_io.input_channel * Lwt_io.output_channel) ->
  command_handler:command_handler ->
  unit ->
  unit Lwt.t

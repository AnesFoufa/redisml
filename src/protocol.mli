(* Redis protocol connection handler *)

(* Next-step requested by the command handler. *)
type handler_step = [ `Continue | `Takeover ]

(* Callback type for command processing - receives command, input/output channels, and client address.
   The handler is responsible for sending the response. *)
type command_handler = Resp.t -> Peer_conn.user Peer_conn.t -> handler_step Lwt.t

(* Handle a client connection - reads commands, processes, and sends responses *)
val handle_connection :
  client_addr:Unix.sockaddr ->
  channels:(Lwt_io.input_channel * Lwt_io.output_channel) ->
  command_handler:command_handler ->
  unit ->
  unit Lwt.t

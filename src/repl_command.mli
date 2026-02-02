(** Repl_command

    Typed classification of commands received by a *replica* from its upstream
    master during replication.

    This is intentionally separate from {!User_command} to prevent accidentally
    treating master-stream traffic as user traffic.

    The type is a GADT indexed by whether a response is required, enabling
    compile-time enforcement of response handling.
*)

(** Phantom type: command requires a response to master *)
type response_required

(** Phantom type: command does not require a response *)
type no_response

(** Replication command GADT indexed by response requirement *)
type _ t =
  | Apply_set : Command.set_params -> no_response t
  | Replconf_getack : response_required t
  | Ignore : no_response t

(** Existential wrapper for parsing (response type unknown until runtime) *)
type packed = Packed : _ t -> packed

type parse_error =
  [ Command.error
  | `DisallowedFromMaster
  ]

val parse : Resp.t -> (packed, parse_error) result
(** Parse a RESP value received from the upstream master into a replication-stream
    command classification. Returns a [packed] existential since the response
    type is determined at runtime. *)

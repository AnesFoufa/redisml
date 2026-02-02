(** Repl_command

    Typed classification of commands received by a *replica* from its upstream
    master during replication.

    This is intentionally separate from {!User_command} to prevent accidentally
    treating master-stream traffic as user traffic.
*)

type t =
  | Apply_set of Command.set_params
  | Replconf_getack
  | Ignore

type parse_error =
  [ Command.error
  | `DisallowedFromMaster
  ]

val parse : Resp.t -> (t, parse_error) result
(** Parse a RESP value received from the upstream master into a replication-stream
    command classification.
*)

val to_command : t -> Command.t option
(** Convert to the existing untyped [Command.t] when appropriate.

    - [Apply_set] -> [Some (Command.Set ...)]
    - [Replconf_getack] -> [Some (Command.Replconf Command.ReplconfGetAck)]
    - [Ignore] -> [None]
*)

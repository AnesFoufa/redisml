(** User_command

    Typed classification of commands that may arrive from a *user* connection.

    Policy:
    - On masters: read + write user commands may be executed.
    - On replicas: only read user commands may be executed; writes are rejected
      with a READONLY error.

    This is intentionally separate from replication-stream commands.
*)

type read

type write

type _ t =
  | Ping : read t
  | Echo : Resp.t -> read t
  | Get : string -> read t
  | InfoReplication : read t
  | ConfigGet : Command.config_param -> read t
  | Keys : string -> read t
  | Set : Command.set_params -> write t

(** Result of parsing a RESP value received from a user connection. *)
type parse_error =
  [ Command.error
  | `ReadOnlyReplica  (** write attempted on a read-only replica *)
  ]

val parse_for_master : Resp.t -> ([ `Read of read t | `Write of write t | `Disallowed ], parse_error) result
(** Parse a user command for a master.

    Commands that are not part of the user-command subset return [`Disallowed].
*)

val parse_for_replica : Resp.t -> (read t, parse_error) result
(** Parse a user command for a replica.

    Writes return [`ReadOnlyReplica].
*)

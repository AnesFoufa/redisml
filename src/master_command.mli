(** Master_command

    Typed classification of all commands that may arrive on a master's user
    connection. This includes:
    - User commands (read/write operations from clients)
    - Protocol commands (Psync, Wait, Replconf from replicas establishing replication)

    Using a GADT indexed by command kind ensures type-safe dispatch.
*)

(** Command kind phantom types *)
type user_read
type user_write
type protocol_psync
type protocol_wait
type protocol_replconf

(** Master command GADT indexed by kind *)
type _ t =
  | Read : User_command.read User_command.t -> user_read t
  | Write : User_command.write User_command.t -> user_write t
  | Psync : Command.psync_params -> protocol_psync t
  | Wait : Command.wait_params -> protocol_wait t
  | Replconf : Command.replconf_command -> protocol_replconf t

(** Existential wrapper for parsing (kind unknown until runtime) *)
type packed = Packed : _ t -> packed

type parse_error = Command.error

val parse : Resp.t -> (packed, parse_error) result
(** Parse a RESP value into a typed master command.

    Returns the appropriate variant based on the command type.
    Unknown or malformed commands return an error.
*)

(** Extract the underlying user read command if applicable *)
val as_read : user_read t -> User_command.read User_command.t

(** Extract the underlying user write command if applicable *)
val as_write : user_write t -> User_command.write User_command.t

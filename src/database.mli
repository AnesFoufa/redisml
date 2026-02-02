(* Database - Encapsulates all server state *)

(** Role phantom types (re-exported from {!Typed_state}). *)
type master = Typed_state.master

type replica = Typed_state.replica

(** Role-indexed database. *)
type 'r db = 'r Typed_state.database

(** Runtime-selected server (role chosen from config at startup). *)
type server = Master of master db | Replica of replica db

(** Create a new database with the given configuration. *)
val create : Config.t -> server

(** Get the current configuration. *)
val get_config : server -> Config.t

(** A user-level step requested by the database layer. *)
type user_step = [ `Reply of Resp.t | `NoReply | `Takeover ]

(** Handle a RESP command received from a user connection.

    Exposed as two entrypoints to keep role constraints type-safe:

    - Master: can accept replication takeover (PSYNC)
    - Replica: enforces READONLY for writes
*)
val handle_user_resp_master :
  master db ->
  Resp.t ->
  current_time:float ->
  Peer_conn.user Peer_conn.t ->
  (user_step, Command.error) result Lwt.t

val handle_user_resp_replica :
  replica db ->
  Resp.t ->
  current_time:float ->
  Peer_conn.user Peer_conn.t ->
  (user_step, User_command.parse_error) result Lwt.t

(** Handle a RESP value received from the upstream master.

    This is *replication-stream* traffic, not user traffic.

    Returns [Ok (Some resp)] when we must reply to upstream (e.g. ACK), [Ok None]
    for one-way stream commands, or [Error _] for malformed/disallowed input.
*)
val handle_replication_resp :
  replica db ->
  Resp.t ->
  current_time:float ->
  Peer_conn.upstream_master Peer_conn.t ->
  (Resp.t option, [ Command.error | `DisallowedFromMaster ]) result Lwt.t

(** Increment replication offset (replica-only). *)
val increment_offset : replica db -> int -> unit

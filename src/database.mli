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

(** {1 Typed Execution Functions}

    These functions enforce role constraints at compile time.
*)

(** Execute a read command. Works on any role. *)
val execute_read :
  _ db ->
  User_command.read User_command.t ->
  current_time:float ->
  Resp.t

(** Execute a write command. Master only (compile-time enforced). *)
val execute_write :
  master db ->
  User_command.write User_command.t ->
  current_time:float ->
  Resp.t

(** Type-indexed response for replication commands. *)
type _ repl_response =
  | Must_respond : Resp.t -> Repl_command.response_required repl_response
  | No_response : Repl_command.no_response repl_response

(** Execute a replication command. Replica only (compile-time enforced).
    Returns a type-indexed response based on the command's response requirement. *)
val execute_repl :
  replica db ->
  'r Repl_command.t ->
  current_time:float ->
  'r repl_response

(** {1 High-Level User Handlers}

    These handle parsing, execution, and protocol-specific behavior.
*)

(** A user-level step requested by the database layer. *)
type user_step = [ `Reply of Resp.t | `NoReply | `Takeover ]

(** Handle a RESP command received from a user connection on master.

    Can accept replication takeover (PSYNC), handle WAIT, etc.
*)
val handle_user_resp_master :
  master db ->
  Resp.t ->
  current_time:float ->
  Peer_conn.user Peer_conn.t ->
  (user_step, Command.error) result Lwt.t

(** Handle a RESP command received from a user connection on replica.

    Enforces READONLY for writes via {!User_command.parse_for_replica}.
*)
val handle_user_resp_replica :
  replica db ->
  Resp.t ->
  current_time:float ->
  Peer_conn.user Peer_conn.t ->
  (user_step, User_command.parse_error) result Lwt.t

(** Increment replication offset (replica-only). *)
val increment_offset : replica db -> int -> unit

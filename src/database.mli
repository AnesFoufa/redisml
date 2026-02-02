(* Database - Encapsulates all server state *)

(** Role phantom types (re-exported from {!Typed_state}). *)
type master = Typed_state.master

type replica = Typed_state.replica

(** Role-indexed database. *)
type 'r db = 'r Typed_state.database

(** Existential database (role chosen from config at runtime). *)
type t = Typed_state.any_database

(** Create a new database with the given configuration. *)
val create : Config.t -> t

(** Get the current configuration. *)
val get_config : t -> Config.t

(** Narrowing helpers.

    These are the only remaining runtime checks around roles; they should only be
    used at the edges where role is chosen from Config.
*)
val as_master : t -> master db option
val as_replica : t -> replica db option
val as_master_exn : t -> master db
val as_replica_exn : t -> replica db

(** Handle a command and return optional response.

    Note: even though [t] is existential, internally this function immediately
    dispatches to role-indexed logic.
*)
val handle_command :
  t ->
  Command.t ->
  current_time:float ->
  original_resp:Resp.t ->
  ic:Lwt_io.input_channel ->
  oc:Lwt_io.output_channel ->
  address:string ->
  Resp.t option Lwt.t

(** Increment replication offset (replica-only). *)
val increment_offset : replica db -> int -> unit

(* For testing only *)
val execute_command : Command.t -> t -> current_time:float -> Resp.t
val should_propagate_command : master db -> Command.t -> bool
val is_replica : t -> bool

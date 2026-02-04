(* Database - Encapsulates all server state with phantom types for role safety *)

(*
 * Phantom Types for Compile-Time Role Safety
 *
 * The database uses phantom types to enforce role-specific operations at compile time:
 * - 'a t: Phantom typed database where 'a is either 'master' or 'replica'
 * - master: Phantom type marker indicating master role
 * - replica: Phantom type marker indicating replica role
 * - db: Dynamic variant used when role is determined at runtime (e.g., create)
 *
 * Benefits:
 * - Master-only operations (PSYNC, WAIT, propagation) require master t
 * - Replica-only operations (increment_offset, GETACK) require replica t
 * - Role violations caught at compile time, not runtime
 * - No need for runtime role checks in typed functions
 *)

(* Phantom typed database - 'a is either master or replica *)
type 'a t

(* Phantom type markers for roles *)
type master
type replica

(* Dynamic role wrapper for when role is determined at runtime *)
type db = Master of master t | Replica of replica t

(* Create a new database with the given configuration *)
val create : Config.t -> db

(* Get the current configuration from any database *)
val get_config : 'a t -> Config.t

(* Handle commands on a master database *)
val handle_master_command :
  master t ->
  Command.t ->
  current_time:float ->
  original_resp:Resp.t ->
  ic:Lwt_io.input_channel ->
  oc:Lwt_io.output_channel ->
  address:string ->
  Resp.t option Lwt.t

(* Handle commands on a replica database *)
val handle_replica_command :
  replica t ->
  Command.t ->
  current_time:float ->
  original_resp:Resp.t ->
  ic:Lwt_io.input_channel ->
  oc:Lwt_io.output_channel ->
  address:string ->
  Resp.t option Lwt.t

(* Increment replication offset (replica-only operation) *)
val increment_offset : replica t -> int -> unit

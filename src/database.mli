(* Database - Encapsulates all server state with phantom types for role safety *)

(*
 * Phantom Types for Compile-Time Role Safety
 *
 * The database uses phantom types to enforce role-specific operations at compile time:
 * - 'a t: Phantom typed database where 'a is master, connected_replica, or disconnected_replica
 * - master: Phantom type marker indicating master role
 * - connected_replica: Phantom type marker for replica connected to master
 * - disconnected_replica: Internal marker for replica before connection (not exposed)
 *
 * Lifecycle:
 * - create returns created_db (CreatedMaster or CreatedReplica with disconnected state)
 * - connect_replica connects replicas and returns connected_replica t
 *
 * Benefits:
 * - Master-only operations (PSYNC, WAIT, propagation) require master t
 * - Replica-only operations (GETACK) require connected_replica t
 * - Cannot call replica operations before connection is established
 * - Role violations caught at compile time, not runtime
 *)

(* Phantom typed database - 'a is master, connected_replica, or disconnected_replica *)
type 'a t

(* Phantom type markers for roles - only expose what's needed *)
type master
type connected_replica
type disconnected_replica

(* Result of create - only two possibilities, replica starts disconnected *)
type created_db = private
  | CreatedMaster of master t
  | CreatedReplica of disconnected_replica t

(* Create a new database with the given configuration.
   Returns CreatedMaster or CreatedReplica (disconnected). *)
val create : Config.t -> created_db

type replica_connection_error = [ `FailedConnection ]

val replica_connection_error_to_string : replica_connection_error -> string

(* Connect a replica to its master. *)
val connect_replica :
  disconnected_replica t ->
  (connected_replica t, [> replica_connection_error ]) result Lwt.t

(* Get the current configuration from any database *)
val get_config : 'a t -> Config.t

type command_result =
  | Respond of Resp.t
  | Silent
  | ConnectionTakenOver

(* Handle commands on a master database *)
val handle_master_command :
  master t ->
  Command.t ->
  current_time:float ->
  original_resp:Resp.t ->
  ic:Lwt_io.input_channel ->
  oc:Lwt_io.output_channel ->
  address:string ->
  command_result Lwt.t

(* Handle commands on a connected replica database *)
val handle_replica_command :
  connected_replica t ->
  Command.t ->
  current_time:float ->
  Resp.t Lwt.t

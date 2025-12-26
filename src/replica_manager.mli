(* ReplicaManager - Master-side logic for managing connected replicas *)

(* Opaque type for replica manager - manages connected replicas *)
type t

(* Create a new replica manager *)
val create : unit -> t

(* Register a new replica connection *)
val register_replica :
  t ->
  channel:Lwt_io.output_channel ->
  address:string ->
  unit Lwt.t

(* Propagate a command to all connected replicas *)
val propagate_to_replicas :
  t ->
  command:Resp.t ->
  unit Lwt.t

(* Get the empty RDB file for PSYNC response *)
val empty_rdb : string

(* Remove a replica from tracking (called on disconnect) *)
val remove_replica :
  t ->
  channel:Lwt_io.output_channel ->
  unit

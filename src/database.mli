(* Database - Encapsulates all server state *)

(* Opaque database type containing storage, configuration, and replication state *)
type t

(* Create a new database with the given configuration *)
val create : Config.t -> t

(* Get the current configuration *)
val get_config : t -> Config.t

(* Handle a command and return optional response *)
val handle_command :
  t ->
  Command.t ->
  original_resp:Resp.t ->
  ic:Lwt_io.input_channel ->
  oc:Lwt_io.output_channel ->
  address:string ->
  Resp.t option Lwt.t

(* Increment replication offset (for tracking bytes from master) *)
val increment_offset : t -> int -> unit

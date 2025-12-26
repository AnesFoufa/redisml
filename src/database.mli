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
  channel:Lwt_io.output_channel ->
  address:string ->
  Resp.t option Lwt.t

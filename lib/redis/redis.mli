type t

type replication_role =
  | Master
  | Slave of { master_host : string; master_port : int }

val init :
  ?replication_role:replication_role ->
  dbfilename:string ->
  dir:string ->
  unit ->
  t

val get_database : t -> int -> Database.t
val set_database : t -> int -> unit
val get_dir : t -> string
val get_dbfilename : t -> string
val get_replication_role : t -> replication_role
val get_metadata : t -> string -> string option

module Database = Database

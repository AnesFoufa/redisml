type t

exception FailedHandshake of string

val init :
  port:int ->
  master_host:string ->
  master_port:int ->
  t * Rdb.metadata * Rdb.databases

val get_master_host : t -> string
val get_master_port : t -> int

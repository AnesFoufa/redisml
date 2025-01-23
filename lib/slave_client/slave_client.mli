type t

val init : port:int -> master_host:string -> master_port:int -> t
val handshake : t -> unit
val get_master_host : t -> string
val get_master_port : t -> int

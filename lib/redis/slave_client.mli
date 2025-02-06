type t

type update = {
  key : string;
  value : Resp.t;
  now : Int64.t;
  duration : Int64.t option;
}

type message =
  | No_op
  | Updates of update list
  | Full_sync of (Rdb.metadata * Rdb.databases)

exception FailedHandshake of string

val init : port:int -> master_host:string -> master_port:int -> t
val get_master_host : t -> string
val get_master_port : t -> int
val poll : t -> message

type t

type message =
  | No_op
  | Updates of Master_command.update list
  | Full_sync of (Rdb.metadata * Rdb.databases)

val init : port:int -> master_host:string -> master_port:int -> t
val poll : t -> message

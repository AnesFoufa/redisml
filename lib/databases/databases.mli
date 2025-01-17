type t
type record = { value : Resp.t; created_at : int; duration : int option }
type database = (string, record) Hashtbl.t

val init : dbfilename:string -> dir:string -> t
val get_database : t -> int -> database
val set_database : t -> int -> unit
val get_dir : t -> string
val get_dbfilename : t -> string

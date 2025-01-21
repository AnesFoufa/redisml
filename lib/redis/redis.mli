type t

val init : dbfilename:string -> dir:string -> t
val get_database : t -> int -> Database.t
val set_database : t -> int -> unit
val get_dir : t -> string
val get_dbfilename : t -> string
val get_metadata : t -> string -> string option

module Database = Database

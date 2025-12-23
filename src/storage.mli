(* In-memory key-value storage *)

type t

val create : unit -> t
val set : t -> string -> string -> unit
val get : t -> string -> string option
val delete : t -> string -> unit
val keys : t -> string list

(* In-memory key-value storage with expiry support *)

type t

val create : unit -> t
val set : t -> string -> string -> float option -> unit
val get : t -> string -> string option
val delete : t -> string -> unit
val keys : t -> string list

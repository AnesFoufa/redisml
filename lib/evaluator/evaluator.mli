type t

val init : Databases.t -> t
val evaluate : t -> now:int -> Resp.t -> Resp.t

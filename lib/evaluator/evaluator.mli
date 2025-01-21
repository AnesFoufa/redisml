type t

val init : Databases.t -> t
val evaluate : t -> now:Int64.t -> Resp.t -> Resp.t

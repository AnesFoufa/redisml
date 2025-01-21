type t

val init : Redis.t -> t
val evaluate : t -> now:Int64.t -> Resp.t -> Resp.t

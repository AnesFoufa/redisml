type t

val init : Redis.t -> t * string Event.channel
val evaluate : t -> now:Int64.t -> Resp.t -> Resp.t

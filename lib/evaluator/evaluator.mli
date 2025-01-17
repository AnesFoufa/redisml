type t

val init : dbfilename:string -> dir:string -> t
val evaluate : t -> now:int -> Resp.t -> Resp.t

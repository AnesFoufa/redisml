type t

val of_resp : Resp.t -> (t, string) Stdlib.result
val handle : (string, Resp.t) Hashtbl.t -> t -> Resp.t

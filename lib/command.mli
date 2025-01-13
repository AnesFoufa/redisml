type t
type state = (string, Resp.t) Hashtbl.t

val of_resp : Resp.t -> (t, string) Stdlib.result
val handle : state -> t -> Resp.t

type t
type state

val init_state : dbfilename:string -> dir:string -> state
val of_resp : Resp.t -> (t, string) Stdlib.result
val handle : state -> t -> Resp.t

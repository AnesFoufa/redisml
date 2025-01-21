type record = { value : Resp.t; expire : Int64.t option }
type t = (string, record) Hashtbl.t

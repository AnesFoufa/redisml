type t = private
  | Ping
  | Echo of string
  | Get of string
  | Set of { key : string; value : Resp.t; duration : Int64.t option }
  | Config_get_dir
  | Config_get_dbfilename
  | Keys
  | Select of int

val of_resp : Resp.t -> (t, string) Stdlib.result

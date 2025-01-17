type t = private
  | Ping
  | Echo of string
  | Get of string
  | Set of { key : string; value : Resp.t; duration : int option }
  | Config_get_dir
  | Config_get_dbfilename
  | Keys

val of_resp : Resp.t -> (t, string) Stdlib.result

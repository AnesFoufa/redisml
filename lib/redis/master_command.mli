type update = {
  key : string;
  value : Resp.t;
  now : Int64.t;
  duration : Int64.t option;
}

type t =
  | Update of update
  | Ping
  | Getack
  | Pong
  | Ok
  | Full_resync
  | Rdb_resync of (Rdb.metadata * Rdb.databases)

val length : t -> int

type config = { dir : string; dbfilename : string }
type record = { value : Resp.t; created_at : int; duration : int option }
type database = (string, record) Hashtbl.t
type dbs = (int, database) Hashtbl.t
type t = { dbs : dbs; config : config }

let init ~dbfilename ~dir =
  let config = { dir; dbfilename } in
  let database = Hashtbl.create 256 in
  let dbs = Hashtbl.create 16 in
  Hashtbl.add dbs 0 database;
  { dbs; config }

let get_database databases i = Hashtbl.find databases.dbs i
let get_dir databases = databases.config.dir
let get_dbfilename databases = databases.config.dbfilename

let set_database databases i =
  if Hashtbl.mem databases.dbs i then ()
  else Hashtbl.add databases.dbs i (Hashtbl.create 256)

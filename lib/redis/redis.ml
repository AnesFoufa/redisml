type replication_role =
  | Master of { replid : string; repl_offset : int }
  | Slave of { master_host : string; master_port : int }

type config = {
  dir : string;
  dbfilename : string;
  replication_role : replication_role;
}

type t = { metadata : Rdb.metadata; databases : Rdb.databases; config : config }

module Database = Database

let master =
  Master
    { replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"; repl_offset = 0 }

let get_metadata redis name = Hashtbl.find_opt redis.metadata name

exception UnparsedRDB

let init ~replication_role ~dbfilename ~dir () =
  let file_path = Filename.concat dir dbfilename in
  let config = { dir; dbfilename; replication_role } in
  try
    let ic = open_in file_path in
    let buf = Bytes.create 2048 in
    let nb_read = input ic buf 0 2048 in
    let content = Bytes.sub_string buf 0 nb_read in
    match Rdb.of_string content with
    | Ok (metadata, databases) -> { metadata; databases; config }
    | Error _err -> raise UnparsedRDB
  with Sys_error _ | UnparsedRDB ->
    let database = Hashtbl.create 256 in
    let databases = Hashtbl.create 16 in
    Hashtbl.add databases 0 database;
    let metadata = Hashtbl.create 32 in
    { metadata; databases; config }

let get_database databases i = Hashtbl.find databases.databases i
let get_dir databases = databases.config.dir
let get_dbfilename databases = databases.config.dbfilename
let get_replication_role redis = redis.config.replication_role

let set_database databases i =
  if Hashtbl.mem databases.databases i then ()
  else Hashtbl.add databases.databases i (Hashtbl.create 256)

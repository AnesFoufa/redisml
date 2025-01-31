type replication_role =
  | Master of { replid : string; repl_offset : int }
  | Slave of { master_host : string; master_port : int }

module Command = struct
  type t =
    | Ping
    | Echo of string
    | Get of { index : int; key : string; now : Int64.t }
    | Set of {
        index : int;
        key : string;
        value : Resp.t;
        duration : Int64.t option;
        now : Int64.t;
      }
    | Keys of { index : int; now : Int64.t }
    | Config_get_dir
    | Config_get_dbfilename
    | Info_replication
    | Repl_conf
    | Psync
    | Select of int
end

type config = {
  dir : string;
  dbfilename : string;
  replication_role : replication_role;
}

type t = {
  metadata : Rdb.metadata;
  databases : Rdb.databases;
  config : config;
  in_chan : (Command.t * (Resp.t * string option) Event.channel) Event.channel;
}

module Database = Database

let master =
  Master
    { replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"; repl_offset = 0 }

let get_metadata redis name = Hashtbl.find_opt redis.metadata name

module Handlers = struct
  let ping = Resp.RString "PONG"
  let echo something = Resp.RBulkString something

  let get_value now record =
    let open Database in
    match record with
    | { value; expire = None } -> value
    | { value; expire = Some timestamp } when timestamp >= now -> value
    | _ -> Resp.NullBulk

  let get_database redis i =
    match Hashtbl.find_opt redis.databases i with
    | Some database -> database
    | None ->
        let database = Hashtbl.create 256 in
        Hashtbl.add redis.databases i database;
        database

  let get redis index key ~now =
    let database = get_database redis index in
    let res =
      Hashtbl.find_opt database key
      |> Option.map (get_value now)
      |> Option.value ~default:Resp.NullBulk
    in
    if Resp.equal res Resp.NullBulk then Hashtbl.remove database key else ();
    res

  let set ?duration redis index key value ~now =
    let database = get_database redis index in
    let expire = duration |> Option.map (fun t -> Int64.add t now) in
    Hashtbl.remove database key;
    Hashtbl.add database key { value; expire };
    Resp.RString "OK"

  let keys redis index now =
    let open Database in
    let database = get_database redis index in
    database |> Hashtbl.to_seq
    |> Seq.filter (fun (_key, { expire; _ }) ->
           Option.fold ~none:true ~some:(fun exp -> exp >= now) expire)
    |> Seq.map (fun (key, _value) -> Resp.RBulkString key)
    |> List.of_seq
    |> fun x -> Resp.RArray x

  let info_replication redis =
    let response =
      match redis.replication_role with
      | Master { replid; repl_offset } ->
          Printf.sprintf
            "role:master\n\rmaster_replid:%s\n\rmaster_repl_offset:%d" replid
            repl_offset
      | Slave _ -> "role:slave"
    in
    let response_with_header = Printf.sprintf "#Replication\n\r%s" response in
    Resp.RBulkString response_with_header
end

exception UnparsedRDB

let get_rdb redis = Rdb.to_string redis.metadata redis.databases (* hello *)

let format_rdb rdb_string =
  let length = String.length rdb_string in
  Printf.sprintf "$%d\r\n%s" length rdb_string

let process_command redis = function
  | Command.Ping -> (Handlers.ping, None)
  | Command.Echo something -> (Handlers.echo something, None)
  | Command.Get { index; key; now } -> (Handlers.get redis index key ~now, None)
  | Command.Set { index; key; value; duration; now } ->
      (Handlers.set ?duration redis index key value ~now, None)
  | Keys { index; now } -> (Handlers.keys redis index now, None)
  | Command.Info_replication -> (Handlers.info_replication redis.config, None)
  | Command.Repl_conf -> (Resp.RString "OK", None)
  | Command.Config_get_dir ->
      (RArray [ RBulkString "dir"; RBulkString redis.config.dir ], None)
  | Command.Config_get_dbfilename ->
      ( RArray [ RBulkString "dbfilename"; RBulkString redis.config.dbfilename ],
        None )
  | Command.Psync -> (
      match redis.config.replication_role with
      | Master { replid; repl_offset } ->
          ( RString (Printf.sprintf "FULLRESYNC %s %d" replid repl_offset),
            Some (get_rdb redis |> format_rdb) )
      | Slave _ -> (RError "Can't be handled by a slave", None))
  | Command.Select i ->
      let _ = Handlers.get_database redis i in
      (RString "OK", None)

let start redis =
  let open Event in
  let open Domainslib in
  let pool = Domainslib.Task.setup_pool ~num_domains:1 () in
  let rec work () =
    try
      let command, response_channel = Event.receive redis.in_chan |> sync in
      let response = process_command redis command in
      let e = Event.send response_channel response in
      sync e;
      work ()
    with _ -> work ()
  in
  let _ = Task.async pool work in
  ()

let init_metadata =
  [
    ("used-mem", "1098928");
    ("redis-bits", "64");
    ("aof-base", "0");
    ("ctime", "1706821741");
    ("redis-ver", "7.2.0");
  ]
  |> List.to_seq |> Hashtbl.of_seq

let init ~replication_role ~dbfilename ~dir () =
  let file_path = Filename.concat dir dbfilename in
  let config = { dir; dbfilename; replication_role } in
  let in_chan = Event.new_channel () in
  let redis =
    try
      let ic = open_in file_path in
      let buf = Bytes.create 2048 in
      let nb_read = input ic buf 0 2048 in
      let content = Bytes.sub_string buf 0 nb_read in
      match Rdb.of_string content with
      | Ok (metadata, databases) -> { metadata; databases; config; in_chan }
      | Error _err -> raise UnparsedRDB
    with Sys_error _ | UnparsedRDB ->
      let databases = Hashtbl.create 16 in
      let metadata = init_metadata in
      { metadata; databases; config; in_chan }
  in
  start redis;
  redis

let handle_command redis command =
  let out_chan = Event.new_channel () in
  Event.send redis.in_chan (command, out_chan) |> Event.sync;
  Event.receive out_chan |> Event.sync

let to_string redis = Rdb.to_string redis.metadata redis.databases

let of_string str ~replication_role ~dbfilename ~dir =
  let config = { dir; dbfilename; replication_role } in
  let in_chan = Event.new_channel () in
  let ( let* ) = Result.bind in
  let* metadata, databases = Rdb.of_string str in
  let redis = { metadata; databases; config; in_chan } in
  start redis;
  Ok redis

let get_all_metadata redis = redis.metadata |> Hashtbl.to_seq

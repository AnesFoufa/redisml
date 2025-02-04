type replication_role =
  | Master of { replid : string; repl_offset : int }
  | Slave of { master_host : string; master_port : int }

type client_id = Uuidm.t

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

  type command_error = NotConnected | NotHandeledBySlave
end

type config = {
  dir : string;
  dbfilename : string;
  replication_role : replication_role;
}

module Uuidm = struct
  include Uuidm

  let hash u = Uuidm.to_string u |> String.hash
end

module ClientTable = Hashtbl.Make (Uuidm)

type connection_message =
  | Connect of (client_id * string Event.channel) Event.channel
  | Disconnect of client_id * unit Event.channel

type incoming_message =
  | DatabaseCommand of {
      command : Command.t;
      client_id : client_id;
      out_chan : (Resp.t, Command.command_error) Result.t Event.channel;
    }
  | ConnectionCommand of connection_message

type t = {
  metadata : Rdb.metadata;
  databases : Rdb.databases;
  config : config;
  in_chan : incoming_message Event.channel;
  client_table : string Event.channel ClientTable.t;
  stream_buffer : (string * string Event.channel) Queue.t;
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

let format_rdb rdb_string =
  let length = String.length rdb_string in
  Printf.sprintf "$%d\r\n%s" length rdb_string

let process_command client_channel redis = function
  | Command.Ping -> Handlers.ping
  | Command.Echo something -> Handlers.echo something
  | Command.Get { index; key; now } -> Handlers.get redis index key ~now
  | Command.Set { index; key; value; duration; now } ->
      Handlers.set ?duration redis index key value ~now
  | Keys { index; now } -> Handlers.keys redis index now
  | Command.Info_replication -> Handlers.info_replication redis.config
  | Command.Repl_conf -> Resp.RString "OK"
  | Command.Config_get_dir ->
      RArray [ RBulkString "dir"; RBulkString redis.config.dir ]
  | Command.Config_get_dbfilename ->
      RArray [ RBulkString "dbfilename"; RBulkString redis.config.dbfilename ]
  | Command.Psync -> (
      match redis.config.replication_role with
      | Master { replid; repl_offset } ->
          let rdb_string =
            Rdb.to_string redis.metadata redis.databases |> format_rdb
          in
          Queue.push (rdb_string, client_channel) redis.stream_buffer;
          RString (Printf.sprintf "FULLRESYNC %s %d" replid repl_offset)
      | Slave _ -> RError "Can't be handled by a slave")
  | Command.Select i ->
      let _ = Handlers.get_database redis i in
      RString "OK"

let handle_database_command redis client_id command response_channel =
  match ClientTable.find_opt redis.client_table client_id with
  | Some client_channel ->
      let response = process_command client_channel redis command in
      let e = Event.send response_channel (Ok response) in
      Event.sync e
  | None ->
      Event.send response_channel (Error Command.NotConnected) |> Event.sync

let random_state = Random.State.make_self_init ()

let handle_connection redis = function
  | Connect out_chan ->
      let client_id = Uuidm.v4_gen random_state () in
      let stream_channel = Event.new_channel () in
      ClientTable.add redis.client_table client_id stream_channel;
      Event.send out_chan (client_id, stream_channel) |> Event.sync
  | Disconnect (client_id, out_chan) ->
      ClientTable.remove redis.client_table client_id;
      Event.send out_chan () |> Event.sync

let handle_message redis = function
  | ConnectionCommand connection_message ->
      handle_connection redis connection_message
  | DatabaseCommand { client_id; out_chan; command } ->
      handle_database_command redis client_id command out_chan

let start redis =
  let open Domainslib in
  let pool = Domainslib.Task.setup_pool ~num_domains:1 () in
  let rec work () =
    try
      (match Event.receive redis.in_chan |> Event.poll with
      | Some incoming_message -> handle_message redis incoming_message
      | None -> ());
      (match Queue.take redis.stream_buffer with
      | exception Queue.Empty -> ()
      | message, channel -> Event.send channel message |> Event.sync);
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
  let client_table = ClientTable.create 256 in
  let stream_buffer = Queue.create () in
  let redis =
    try
      let ic = open_in file_path in
      let buf = Bytes.create 2048 in
      let nb_read = input ic buf 0 2048 in
      let content = Bytes.sub_string buf 0 nb_read in
      match Rdb.of_string content with
      | Ok (metadata, databases) ->
          { metadata; databases; config; in_chan; client_table; stream_buffer }
      | Error _err -> raise UnparsedRDB
    with Sys_error _ | UnparsedRDB ->
      let databases = Hashtbl.create 16 in
      let metadata = init_metadata in
      { metadata; databases; config; in_chan; client_table; stream_buffer }
  in
  start redis;
  redis

let handle_command client_id redis command =
  let out_chan = Event.new_channel () in
  Event.send redis.in_chan (DatabaseCommand { client_id; command; out_chan })
  |> Event.sync;
  Event.receive out_chan |> Event.sync

let to_string redis = Rdb.to_string redis.metadata redis.databases

let of_string str ~replication_role ~dbfilename ~dir =
  let client_table = ClientTable.create 256 in
  let config = { dir; dbfilename; replication_role } in
  let in_chan = Event.new_channel () in
  let stream_buffer = Queue.create () in
  let ( let* ) = Result.bind in
  let* metadata, databases = Rdb.of_string str in
  let redis =
    { metadata; databases; config; in_chan; client_table; stream_buffer }
  in
  start redis;
  Ok redis

let get_all_metadata redis = redis.metadata |> Hashtbl.to_seq

let connect redis =
  let chan = Event.new_channel () in
  Event.send redis.in_chan (ConnectionCommand (Connect chan)) |> Event.sync;
  Event.receive chan |> Event.sync

let disconnect client_id redis =
  let chan = Event.new_channel () in
  Event.send redis.in_chan (ConnectionCommand (Disconnect (client_id, chan)))
  |> Event.sync;
  Event.receive chan |> Event.sync

open Unix

type update = {
  key : string;
  value : Resp.t;
  now : Int64.t;
  duration : Int64.t option;
}

type command = Update of update | Ping | Getack
type state = Pinging | Replconf_capa | Replconf_port | Psync | Updating

type client = {
  port : int;
  master_host : string;
  master_port : int;
  mutable state : state;
  mutable socket : file_descr;
  mutable updates_buffer : update list;
  mutable metadata_databases_buffer : (Rdb.metadata * Rdb.databases) option;
  mutable processed_bytes : int;
}

let connect master_host master_port =
  let master_inet_addr =
    if master_host = "localhost" then inet_addr_loopback
    else inet_addr_of_string master_host
  in
  let socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt socket SO_REUSEADDR true;
  let sockaddr = ADDR_INET (master_inet_addr, master_port) in
  connect socket sockaddr;
  socket

let new_client port master_host master_port =
  let socket = connect master_host master_port in
  {
    port;
    master_host;
    master_port;
    state = Pinging;
    socket;
    updates_buffer = [];
    metadata_databases_buffer = None;
    processed_bytes = 0;
  }

let reinit_client client =
  let socket = connect client.master_host client.master_port in
  client.state <- Pinging;
  client.socket <- socket;
  client.updates_buffer <- [];
  client.metadata_databases_buffer <- None;
  client.processed_bytes <- 0

type message =
  | No_op
  | Updates of update list
  | Full_sync of (Rdb.metadata * Rdb.databases)

type t = message Event.channel Event.channel

exception ConnectionLost

let read_from_socket ready_socket =
  let buffer = Bytes.create 2056 in
  let bytes_read = Unix.read ready_socket buffer 0 2056 in
  if bytes_read = 0 then raise ConnectionLost;
  Bytes.sub_string buffer 0 bytes_read

let write_command command socket =
  let message = command |> Resp.to_string |> Bytes.of_string in
  write socket message 0 (Bytes.length message)

let rdb_sync =
  let is_digit = function '0' .. '9' -> true | _ -> false in
  let open Angstrom in
  let* _ = char '$' in
  let* digits_str = Angstrom.take_while1 is_digit in
  let _nb_chars = int_of_string digits_str in
  let* _ = string "\r\n" in
  Rdb.rdb

let parse_commands resps now =
  let open Resp in
  resps
  |> List.filter_map (fun resp ->
         match resp with
         | RArray [ RBulkString set; RBulkString key; value ]
           when set |> String.uppercase_ascii |> String.equal "SET" ->
             Some (Update { key; value; now; duration = None })
         | RArray
             [
               RBulkString set;
               RBulkString key;
               value;
               RBulkString px;
               RBulkString duration;
             ]
           when set |> String.uppercase_ascii |> String.equal "SET"
                && px |> String.uppercase_ascii |> String.equal "PX" ->
             Some
               (Update
                  { key; value; now; duration = Int64.of_string_opt duration })
         | RArray [ RBulkString ping ]
           when ping |> String.uppercase_ascii |> String.equal "PING" ->
             Some Ping
         | RArray [ RBulkString replconf; RBulkString getack; RBulkString "*" ]
           when replconf |> String.uppercase_ascii |> String.equal "REPLCONF"
                && getack |> String.uppercase_ascii |> String.equal "GETACK" ->
             Some Getack
         | _ -> None)

let commands_metadata_databases now =
  let open Angstrom in
  let* first_resps = many Resp.resp in
  let* maybe_metadata_databases = option None (rdb_sync >>| fun x -> Some x) in
  let* last_resps = many Resp.resp in
  let resps = first_resps @ last_resps in
  let commands = parse_commands resps now in
  return (commands, maybe_metadata_databases)

let update_to_resp =
  let open Resp in
  function
  | { key; value; now = _now; duration = None } ->
      RArray [ RBulkString "SET"; RBulkString key; value ]
  | { key; value; now = _now; duration = Some d } ->
      RArray
        [
          RBulkString "SET";
          RBulkString key;
          value;
          RBulkString "px";
          RBulkString (Int64.to_string d);
        ]

let command_length = function
  | Ping -> 14
  | Getack -> 37
  | Update update -> update_to_resp update |> Resp.to_string |> String.length

let handle_command client command =
  (match command with
  | Update update -> client.updates_buffer <- update :: client.updates_buffer
  | Ping -> ()
  | Getack ->
      let open Resp in
      let processed_bytes = client.processed_bytes |> Int.to_string in
      let ack_response =
        RArray
          [
            RBulkString "REPLCONF";
            RBulkString "ACK";
            RBulkString processed_bytes;
          ]
      in
      let buffer = ack_response |> to_string |> Bytes.of_string in
      let _ = Unix.write client.socket buffer 0 (Bytes.length buffer) in
      ());
  client.processed_bytes <- client.processed_bytes + command_length command

let handle_master_messages client =
  let commands, maybe_metadata_databases =
    let master_messages = read_from_socket client.socket in
    let now = Unix.gettimeofday () *. 1000. |> Int64.of_float in

    Angstrom.parse_string ~consume:Angstrom.Consume.Prefix
      (commands_metadata_databases now)
      master_messages
    |> Result.get_ok
  in
  List.iter (handle_command client) commands;
  match maybe_metadata_databases with
  | Some (metadata, databases) ->
      client.metadata_databases_buffer <- Some (metadata, databases)
  | None -> ()

let psync client =
  let psync_command =
    Resp.RArray [ RBulkString "PSYNC"; RBulkString "?"; RBulkString "-1" ]
  in
  let _ = write_command psync_command client.socket in
  ()

let ping socket =
  let ping_command = Resp.RArray [ Resp.RBulkString "PING" ] in
  let _ = write_command ping_command socket in
  ()

let replconf_capa socket =
  let replconf_capa_command =
    Resp.RArray
      [ RBulkString "REPLCONF"; RBulkString "capa"; RBulkString "psync2" ]
  in
  let _ = write_command replconf_capa_command socket in
  ()

let replconf_port socket port =
  let replconf_capa_command =
    Resp.RArray
      [
        RBulkString "REPLCONF";
        RBulkString "listening-port";
        RBulkString (string_of_int port);
      ]
  in
  let _ = write_command replconf_capa_command socket in
  ()

let rec loop client (in_channel : t) () =
  try
    let read_socks, _, _ = select [ client.socket ] [] [] 0.01 in
    if read_socks |> List.is_empty |> Bool.not then
      handle_master_messages client;
    let _, write_socks, _ = select [] [ client.socket ] [] 0.01 in
    if write_socks |> List.is_empty |> Bool.not then
      match client.state with
      | Pinging ->
          ping client.socket;
          client.state <- Replconf_port;
          loop client in_channel ()
      | Replconf_port ->
          replconf_port client.socket client.port;
          client.state <- Replconf_capa;
          loop client in_channel ()
      | Replconf_capa ->
          replconf_capa client.socket;
          client.state <- Psync;
          loop client in_channel ()
      | Psync ->
          psync client;
          client.state <- Updating;
          loop client in_channel ()
      | Updating ->
          (match client.metadata_databases_buffer with
          | Some (metadata, databases) ->
              let out_channel = Event.receive in_channel |> Event.sync in
              Event.send out_channel (Full_sync (metadata, databases))
              |> Event.sync;
              client.metadata_databases_buffer <- None
          | None -> ());
          if client.updates_buffer |> List.is_empty |> Bool.not then (
            let out_channel = Event.receive in_channel |> Event.sync in
            Event.send out_channel (Updates client.updates_buffer) |> Event.sync;
            client.updates_buffer <- []);
          loop client in_channel ()
  with ConnectionLost ->
    reinit_client client;
    loop client in_channel ()

let init ~port ~master_host ~master_port =
  let client = new_client port master_host master_port in
  let channel = Event.new_channel () in
  let pool = Domainslib.Task.setup_pool ~num_domains:1 () in
  let _ = Domainslib.Task.async pool (loop client channel) in
  channel

let poll (channel : t) =
  let response_channel = Event.new_channel () in
  match Event.send channel response_channel |> Event.poll with
  | Some () -> Event.receive response_channel |> Event.sync
  | None -> No_op

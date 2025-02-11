open Unix

let write_command command socket =
  let message = command |> Resp.to_string |> Bytes.of_string in
  write socket message 0 (Bytes.length message)

let ping socket =
  let ping_command = Resp.RArray [ Resp.RBulkString "PING" ] in
  let _ = write_command ping_command socket in
  ()

type state =
  | Pinging of unit Debouncer.t
  | Replconf_capa of unit Debouncer.t
  | Replconf_port of unit Debouncer.t
  | Psync of unit Debouncer.t
  | Updating

type client = {
  port : int;
  master_host : string;
  master_port : int;
  mutable state : state;
  mutable socket : file_descr;
  mutable updates_buffer : Master_command.update list;
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
    state = Pinging (Debouncer.init ~seconds:1 ~op:(fun () -> ping socket) ());
    socket;
    updates_buffer = [];
    metadata_databases_buffer = None;
    processed_bytes = 0;
  }

let reinit_client client =
  let socket = connect client.master_host client.master_port in
  client.state <-
    Pinging (Debouncer.init ~seconds:1 ~op:(fun () -> ping socket) ());
  client.socket <- socket;
  client.updates_buffer <- [];
  client.metadata_databases_buffer <- None;
  client.processed_bytes <- 0

type message =
  | No_op
  | Updates of Master_command.update list
  | Full_sync of (Rdb.metadata * Rdb.databases)

type t = message Event.channel Event.channel

exception ConnectionLost

let read_from_socket ready_socket =
  let buffer = Bytes.create 2056 in
  let bytes_read = Unix.read ready_socket buffer 0 2056 in
  if bytes_read = 0 then raise ConnectionLost;
  Bytes.sub_string buffer 0 bytes_read

let rdb_sync =
  let is_digit = function '0' .. '9' -> true | _ -> false in
  let open Angstrom in
  let* _ = char '$' in
  let* digits_str = Angstrom.take_while1 is_digit in
  let _nb_chars = int_of_string digits_str in
  let* _ = string "\r\n" in
  Rdb.rdb

let string_equal_case_insensitive s1 s2 =
  let upper_s1 = s1 |> String.uppercase_ascii in
  let upper_s2 = s2 |> String.uppercase_ascii in
  upper_s1 |> String.equal upper_s2

type master_message = A of Resp.t | B of (Rdb.metadata * Rdb.databases)

let parse_commands now resps =
  let open Resp in
  let open Master_command in
  resps
  |> List.filter_map (fun message ->
         match message with
         | A resp -> (
             match resp with
             | RString pong when pong |> string_equal_case_insensitive "pong" ->
                 Some Pong
             | RString fullresync
               when fullresync |> String.uppercase_ascii
                    |> String.starts_with ~prefix:"FULLRESYNC" ->
                 Some Full_resync
             | RString ok when string_equal_case_insensitive ok "ok" -> Some Ok
             | RArray [ RBulkString set; RBulkString key; value ]
               when string_equal_case_insensitive set "set" ->
                 Some (Update { key; value; now; duration = None })
             | RArray
                 [
                   RBulkString set;
                   RBulkString key;
                   value;
                   RBulkString px;
                   RBulkString duration;
                 ]
               when string_equal_case_insensitive set "set"
                    && string_equal_case_insensitive px "px" ->
                 Some
                   (Update
                      {
                        key;
                        value;
                        now;
                        duration = Int64.of_string_opt duration;
                      })
             | RArray [ RBulkString ping ]
               when string_equal_case_insensitive ping "ping" ->
                 Some Ping
             | RArray
                 [ RBulkString replconf; RBulkString getack; RBulkString "*" ]
               when string_equal_case_insensitive replconf "replconf"
                    && string_equal_case_insensitive getack "getack" ->
                 Some Getack
             | _ -> None)
         | B (metadata, databases) -> Some (Rdb_resync (metadata, databases)))

let master_messages now =
  let open Angstrom in
  let resp_message = Resp.resp >>| fun x -> A x in
  let rdb_message = rdb_sync >>| fun x -> B x in
  let messages = many (resp_message <|> rdb_message) in
  messages >>| parse_commands now

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

let replconf_capa socket =
  let replconf_capa_command =
    Resp.RArray
      [ RBulkString "REPLCONF"; RBulkString "capa"; RBulkString "psync2" ]
  in
  let _ = write_command replconf_capa_command socket in
  ()

let psync socket =
  let psync_command =
    Resp.RArray [ RBulkString "PSYNC"; RBulkString "?"; RBulkString "-1" ]
  in
  let _ = write_command psync_command socket in
  ()

let handle_command client command =
  let new_state =
    let open Master_command in
    match (command, client.state) with
    | Update update, _ ->
        client.updates_buffer <- update :: client.updates_buffer;
        client.state
    | Ping, _ -> client.state
    | Getack, _ ->
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
        client.state
    | Pong, Pinging _ ->
        Replconf_port
          (Debouncer.init ~seconds:1
             ~op:(fun () -> replconf_port client.socket client.port)
             ())
    | Ok, Replconf_port _ ->
        Replconf_capa
          (Debouncer.init ~seconds:1
             ~op:(fun () -> replconf_capa client.socket)
             ())
    | Ok, Replconf_capa _ ->
        Psync (Debouncer.init ~seconds:1 ~op:(fun () -> psync client.socket) ())
    | Full_resync, _ -> Updating
    | Rdb_resync (metadata, databases), _ ->
        client.updates_buffer <- [];
        client.metadata_databases_buffer <- Some (metadata, databases);
        client.state
    | _ -> client.state
  in
  client.state <- new_state;
  client.processed_bytes <-
    client.processed_bytes + Master_command.length command

let handle_master_messages client =
  let commands =
    let master_messages_str = read_from_socket client.socket in
    let now = Unix.gettimeofday () *. 1000. |> Int64.of_float in

    Angstrom.parse_string ~consume:Angstrom.Consume.Prefix (master_messages now)
      master_messages_str
    |> Result.get_ok
  in
  List.iter (handle_command client) commands

let rec loop client (in_channel : t) () =
  try
    let read_socks, _, _ = select [ client.socket ] [] [] 0.01 in
    if read_socks |> List.is_empty |> Bool.not then
      handle_master_messages client;
    let _, write_socks, _ = select [] [ client.socket ] [] 0.01 in
    if write_socks |> List.is_empty |> Bool.not then
      match client.state with
      | Pinging debouncer ->
          Debouncer.debounce debouncer;
          loop client in_channel ()
      | Replconf_port debouncer ->
          Debouncer.debounce debouncer;
          loop client in_channel ()
      | Replconf_capa debouncer ->
          Debouncer.debounce debouncer;
          loop client in_channel ()
      | Psync debouncer ->
          Debouncer.debounce debouncer;
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

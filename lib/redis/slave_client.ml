open Unix

type update = {
  key : string;
  value : Resp.t;
  now : Int64.t;
  duration : Int64.t option;
}

type handshaked = { socket : file_descr; mutable updates_buffer : update list }

type synced = {
  socket : file_descr;
  metadata : Rdb.metadata;
  databases : Rdb.databases;
}

type state =
  | Init
  | Handshaked of handshaked
  | Synced of synced
  | Updating of file_descr

type t = {
  port : int;
  master_host : string;
  master_port : int;
  mutable state : state;
}

type message =
  | No_op
  | Updates of update list
  | Full_sync of (Rdb.metadata * Rdb.databases)

exception FailedHandshake of string

let get_master_host self = self.master_host
let get_master_port self = self.master_port

let write_command command socket =
  let message = command |> Resp.to_string |> Bytes.of_string in
  let _ = write socket message 0 (Bytes.length message) in
  let buffer = Bytes.create 1024 in
  let bytes_read = Unix.read socket buffer 0 1024 in
  let response = Bytes.sub_string buffer 0 bytes_read in
  response

let rdb_sync =
  let is_digit = function '0' .. '9' -> true | _ -> false in
  let open Angstrom in
  let* _ = char '$' in
  let* digits_str = Angstrom.take_while1 is_digit in
  let nb_chars = int_of_string digits_str in
  let* _ = string "\r\n" in
  take nb_chars

let parse_updates resps now =
  let open Resp in
  resps
  |> List.filter_map (fun resp ->
         match resp with
         | RArray [ RBulkString set; RBulkString key; value ]
           when set |> String.uppercase_ascii |> String.equal "SET" ->
             Some { key; value; now; duration = None }
         | _ -> None)

let handshake self =
  let master_inet_addr =
    if self.master_host = "localhost" then inet_addr_loopback
    else inet_addr_of_string self.master_host
  in
  let client_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt client_socket SO_REUSEADDR true;
  let sockaddr = ADDR_INET (master_inet_addr, self.master_port) in
  connect client_socket sockaddr;
  let ping = Resp.RArray [ Resp.RBulkString "PING" ] in
  let replconf_port =
    Resp.RArray
      [
        RBulkString "REPLCONF";
        RBulkString "listening-port";
        RBulkString (string_of_int self.port);
      ]
  in
  let replconf_capa =
    Resp.RArray
      [ RBulkString "REPLCONF"; RBulkString "capa"; RBulkString "psync2" ]
  in
  let psync =
    Resp.RArray [ RBulkString "PSYNC"; RBulkString "?"; RBulkString "-1" ]
  in
  let _ = write_command ping client_socket in
  let _ = write_command replconf_port client_socket in
  let _ = write_command replconf_capa client_socket in
  let _ = write_command psync client_socket in
  let handshaked = { socket = client_socket; updates_buffer = [] } in
  self.state <- Handshaked handshaked

let init ~port ~master_host ~master_port =
  { port; master_host; master_port; state = Init }

exception ConnectionLost

let poll self =
  try
    match self.state with
    | Init ->
        handshake self;
        No_op
    | Handshaked handshaked -> (
        match select [ handshaked.socket ] [] [] 0.0001 with
        | [ ready_socket ], _, _ -> (
            let buffer = Bytes.create 2056 in
            let bytes_read = Unix.read ready_socket buffer 0 2056 in
            if bytes_read = 0 then raise ConnectionLost;
            let response = Bytes.sub_string buffer 0 bytes_read in
            match Resp.many_of_string response with
            | Ok resps ->
                let now = Unix.gettimeofday () *. 1000. |> Int64.of_float in
                let updates = parse_updates resps now in
                handshaked.updates_buffer <- updates @ handshaked.updates_buffer;
                No_op
            | Error _ ->
                (let ( let* ) = Result.bind in
                 let* rdb_string =
                   Angstrom.parse_string ~consume:Angstrom.Consume.All rdb_sync
                     response
                 in
                 let* metadata, databases = Rdb.of_string rdb_string in
                 self.state <-
                   Synced { socket = handshaked.socket; metadata; databases };
                 Ok (Updates handshaked.updates_buffer))
                |> Result.fold ~ok:(fun x -> x) ~error:(fun _ -> No_op))
        | _ -> No_op)
    | Synced { socket; metadata; databases } ->
        self.state <- Updating socket;
        Full_sync (metadata, databases)
    | Updating socket -> (
        match select [ socket ] [] [] 0.0001 with
        | [ ready_socket ], _, _ -> (
            let buffer = Bytes.create 2056 in
            let bytes_read = Unix.read ready_socket buffer 0 2056 in
            if bytes_read = 0 then raise ConnectionLost;
            let response = Bytes.sub_string buffer 0 bytes_read in
            match Resp.many_of_string response with
            | Error _ -> No_op
            | Ok resps ->
                let now = Unix.gettimeofday () *. 1000. |> Int64.of_float in
                Updates (parse_updates resps now))
        | _ -> No_op)
  with ConnectionLost ->
    self.state <- Init;
    No_op

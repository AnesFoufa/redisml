open Unix

type t = {
  port : int;
  master_host : string;
  master_port : int;
  socket : file_descr;
}

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

let read_metadata_and_databases socket =
  let buffer = Bytes.create 1024 in
  let bytes_read = Unix.read socket buffer 0 1024 in
  let response = Bytes.sub_string buffer 0 bytes_read in
  let ( let* ) = Result.bind in
  let* rdb =
    Angstrom.parse_string ~consume:Angstrom.Consume.All rdb_sync response
  in
  let* metadata, databases = Rdb.of_string rdb in
  Ok (metadata, databases)

let handshake self =
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
  let _ = write_command ping self.socket in
  let _ = write_command replconf_port self.socket in
  let _ = write_command replconf_capa self.socket in
  let _ = write_command psync self.socket in
  read_metadata_and_databases self.socket
  |> Result.fold
       ~ok:(fun x -> x)
       ~error:(fun message -> raise (FailedHandshake message))

let init ~port ~master_host ~master_port =
  let master_inet_addr =
    if master_host = "localhost" then inet_addr_loopback
    else inet_addr_of_string master_host
  in
  let client_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt client_socket SO_REUSEADDR true;
  let sockaddr = ADDR_INET (master_inet_addr, master_port) in
  connect client_socket sockaddr;
  let client = { port; master_host; master_port; socket = client_socket } in
  let metadata, databases = handshake client in
  (client, metadata, databases)

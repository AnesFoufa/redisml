open Unix

type t = {
  port : int;
  master_host : string;
  master_port : int;
  master_inet_addr : inet_addr;
}

let get_master_host self = self.master_host
let get_master_port self = self.master_port

let init ~port ~master_host ~master_port =
  let master_inet_addr =
    if master_host = "localhost" then inet_addr_loopback
    else inet_addr_of_string master_host
  in
  { port; master_host; master_port; master_inet_addr }

let write_command command socket =
  let message = command |> Resp.to_string |> Bytes.of_string in
  let _ = write socket message 0 (Bytes.length message) in
  let buffer = Bytes.create 1024 in
  let bytes_read = Unix.read socket buffer 0 1024 in
  let response = Bytes.sub_string buffer 0 bytes_read in
  response

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
  let client_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt client_socket SO_REUSEADDR true;
  let sockaddr = ADDR_INET (self.master_inet_addr, self.master_port) in
  connect client_socket sockaddr;
  let _ = write_command ping client_socket in
  let _ = write_command replconf_port client_socket in
  let _ = write_command replconf_capa client_socket in
  let _ = write_command psync client_socket in
  ()

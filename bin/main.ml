open Unix

let evaluate line evaluator ~now =
  let response_res =
    let ( let* ) = Result.bind in
    let* rvalue = Resp.of_string line in
    Ok (Client.evaluate evaluator ~now rvalue)
  in
  let response_resp =
    match response_res with Ok response -> response | Error m -> Resp.RError m
  in
  Resp.to_string response_resp

let now () = Unix.gettimeofday () *. 1000. |> Int64.of_float

let read client_fd =
  let in_chan = in_channel_of_descr client_fd in
  let buf = Bytes.create 100 in
  let nb_read = input in_chan buf 0 100 in
  if nb_read == 0 then raise End_of_file else ();
  let line = Bytes.sub_string buf 0 nb_read in
  print_endline (String.escaped line);
  line

let print client_fd response_line =
  let out_chan = out_channel_of_descr client_fd in
  print_endline response_line;
  Printf.fprintf out_chan "%s" response_line;
  flush out_chan

let rec repl client_fd evaluator =
  try
    let line = read client_fd in
    let now = now () in
    let response = evaluate line evaluator ~now in
    print client_fd response;
    repl client_fd evaluator
  with End_of_file -> ()

let rec accept_socket_and_repl pool server_socket redis =
  let client_socket, _ = accept server_socket in
  let work () =
    let evaluator = Client.init redis in
    print_endline "New client";
    repl client_socket evaluator;
    close client_socket;
    print_endline "Client left"
  in
  let _ = Thread_pool.add_work pool work in
  accept_socket_and_repl pool server_socket redis

let usage_message =
  "--dir <directory> --dbfilename <file> --port <port> --replicaof <MASTER \
   HOST> <MASTER PORT>"

let dbfilename = ref ""
let dir = ref ""
let port = ref 6379
let replicaof = ref ""
let anon_fun _arg = ()

let specs =
  [
    ( "--dir",
      Arg.Set_string dir,
      "the path to the directory where the RDB file is stored (example: \
       /tmp/redis-data)" );
    ( "--dbfilename",
      Arg.Set_string dbfilename,
      "the name of the RDB file (example: rdbfile)" );
    ("--port", Arg.Set_int port, "port number");
    ( "--replicaof",
      Arg.Set_string replicaof,
      "master's host and port specified by space, Example --replicaof \
       localhost 6397" );
  ]

let parse_host_and_port replicaof =
  let open Angstrom in
  let host_and_port =
    let* host = take_till (Char.equal ' ') in
    let* _ = many1 (char ' ') in
    let* port = take_while (fun _c -> true) in
    match int_of_string_opt port with
    | Some p -> return (host, p)
    | None -> fail "expected a valid port number"
  in
  parse_string ~consume:Consume.All host_and_port replicaof

let write_command command socket =
  let message = command |> Resp.to_string |> Bytes.of_string in
  let _ = write socket message 0 (Bytes.length message) in
  let buffer = Bytes.create 1024 in
  let bytes_read = Unix.read socket buffer 0 1024 in
  let _response = Bytes.sub_string buffer 0 bytes_read in
  ()

let handshake_master master_host master_port slave_port =
  let client_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt client_socket SO_REUSEADDR true;
  let inet_addr =
    if master_host = "localhost" then inet_addr_loopback
    else inet_addr_of_string master_host
  in
  let sockaddr = ADDR_INET (inet_addr, master_port) in
  connect client_socket sockaddr;
  write_command (Resp.RArray [ Resp.RBulkString "PING" ]) client_socket;
  write_command
    (Resp.RArray
       [
         RBulkString "REPLCONF";
         RBulkString "listening-port";
         RBulkString (string_of_int slave_port);
       ])
    client_socket;
  write_command
    (Resp.RArray
       [ RBulkString "REPLCONF"; RBulkString "capa"; RBulkString "psync2" ])
    client_socket;
  close client_socket

let () =
  let open Redis in
  Arg.parse specs anon_fun usage_message;
  let replication_role =
    if !replicaof |> String.equal "" then master
    else
      match parse_host_and_port !replicaof with
      | Ok (host, port) ->
          Redis.Slave { master_host = host; master_port = port }
      | Error err ->
          Printf.sprintf "%s %s" "Incorrect replicaof parameter:" err
          |> failwith
  in
  let () =
    match replication_role with
    | Slave { master_host; master_port } ->
        handshake_master master_host master_port !port
    | Master _ -> ()
  in
  let server_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt server_socket SO_REUSEADDR true;
  bind server_socket (ADDR_INET (inet_addr_of_string "127.0.0.1", !port));
  listen server_socket 1;
  let redis = init ~replication_role ~dbfilename:!dbfilename ~dir:!dir () in
  let pool = Thread_pool.create ~max_num_threads:4 () |> Core.ok_exn in
  accept_socket_and_repl pool server_socket redis

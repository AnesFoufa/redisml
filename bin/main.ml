open Unix

let evaluate line client ~now =
  let response_res =
    let ( let* ) = Result.bind in
    let* rvalue = Resp.of_string line in
    Ok (Client.evaluate client ~now rvalue)
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
  line

let print client_fd response_line =
  let out_chan = out_channel_of_descr client_fd in
  Printf.fprintf out_chan "%s" response_line;
  flush out_chan

let rec repl client_fd stream_channel redis client =
  try
    let readable, _, _ = select [ client_fd ] [] [] 1E-3 in
    if not (List.is_empty readable) then
      let line = read client_fd in
      let now = now () in
      let response = evaluate line client ~now in
      print client_fd response
    else ();
    (match Event.receive stream_channel |> Event.poll with
    | Some message -> print client_fd message
    | None -> ());
    repl client_fd stream_channel redis client
  with End_of_file -> ()

let rec accept_socket_and_repl pool server_socket redis =
  let client_socket, _ = accept server_socket in
  let work () =
    let client, stream_channel = Client.init redis in
    print_endline "New client";
    repl client_socket stream_channel redis client;
    close client_socket;
    print_endline "Client left"
  in
  let _ = Domainslib.Task.async pool work in
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

let () =
  let open Redis in
  Arg.parse specs anon_fun usage_message;
  let replication_role =
    if !replicaof |> String.equal "" then master
    else
      match parse_host_and_port !replicaof with
      | Ok (host, master_port) ->
          Redis.Slave { master_host = host; master_port; port = !port }
      | Error err ->
          Printf.sprintf "%s %s" "Incorrect replicaof parameter:" err
          |> failwith
  in
  let server_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt server_socket SO_REUSEADDR true;
  bind server_socket (ADDR_INET (inet_addr_of_string "127.0.0.1", !port));
  listen server_socket 1;
  let redis = init ~replication_role ~dbfilename:!dbfilename ~dir:!dir () in
  let pool = Domainslib.Task.setup_pool ~num_domains:16 () in
  accept_socket_and_repl pool server_socket redis

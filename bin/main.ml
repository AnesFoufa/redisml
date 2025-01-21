open Unix

let evaluate line evaluator ~now =
  let response_res =
    let ( let* ) = Result.bind in
    let* rvalue = Resp.of_string line in
    Ok (Evaluator.evaluate evaluator ~now rvalue)
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

let rec accept_socket_and_repl pool server_socket databases =
  let client_socket, _ = accept server_socket in
  let work () =
    let evaluator = Evaluator.init databases in
    print_endline "New client";
    repl client_socket evaluator;
    close client_socket;
    print_endline "Client left"
  in
  let _ = Thread_pool.add_work pool work in
  accept_socket_and_repl pool server_socket databases

let usage_message = "--dir <directory> --dbfilename <file>"
let dbfilename = ref ""
let dir = ref ""
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
  ]

let () =
  Arg.parse specs anon_fun usage_message;
  let server_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt server_socket SO_REUSEADDR true;
  bind server_socket (ADDR_INET (inet_addr_of_string "127.0.0.1", 6379));
  listen server_socket 1;
  let databases = Databases.init ~dbfilename:!dbfilename ~dir:!dir in
  let pool = Thread_pool.create ~max_num_threads:4 () |> Core.ok_exn in
  accept_socket_and_repl pool server_socket databases

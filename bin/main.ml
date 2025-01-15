open Unix
open Lib

let handle_input_line line state =
  let ( let* ) = Result.bind in
  let* rvalue = Resp.of_string line in
  print_endline (Resp.show rvalue);
  let* command = Command.of_resp rvalue in
  Ok (Command.handle state command)

let process_line line state =
  let response_or_error = handle_input_line line state in
  Result.fold ~ok:(fun x -> x) ~error:(fun m -> Resp.RError m) response_or_error

let rec handle_client client_fd state =
  let out_chan = out_channel_of_descr client_fd in
  let in_chan = in_channel_of_descr client_fd in
  let buf = Bytes.create 100 in
  try
    let nb_read = input in_chan buf 0 100 in
    if nb_read == 0 then raise End_of_file else ();
    let line = Bytes.sub_string buf 0 nb_read in
    print_endline (String.escaped line);
    let response = process_line line state in
    let response_line = Resp.to_string response in
    print_endline response_line;
    Printf.fprintf out_chan "%s" response_line;
    flush out_chan;
    handle_client client_fd state
  with End_of_file -> ()

let rec accept_socket_and_handle_client pool server_socket state =
  let client_socket, _ = accept server_socket in
  let work () =
    print_endline "New client";
    handle_client client_socket state;
    close client_socket;
    print_endline "Client left"
  in
  let _ = Thread_pool.add_work pool work in
  accept_socket_and_handle_client pool server_socket state

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
  let state = Command.init_state ~dbfilename:!dbfilename ~dir:!dir in
  let server_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt server_socket SO_REUSEADDR true;
  bind server_socket (ADDR_INET (inet_addr_of_string "127.0.0.1", 6379));
  listen server_socket 1;
  let pool = Thread_pool.create ~max_num_threads:4 () |> Core.ok_exn in
  accept_socket_and_handle_client pool server_socket state

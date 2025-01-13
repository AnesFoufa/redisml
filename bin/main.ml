open Unix

type command = Ping | Echo of string

let parse_command = function
  | Resp.RArray [ Resp.RBulkString ping ]
    when String.uppercase_ascii ping |> String.equal "PING" ->
      Stdlib.Ok Ping
  | Resp.RArray [ Resp.RBulkString echo; Resp.RBulkString arg ]
    when echo |> String.uppercase_ascii |> String.equal "ECHO" ->
      Stdlib.Ok (Echo arg)
  | _ -> Stdlib.Error "Unknown command"

let handle_command = function
  | Ping -> Resp.RString "PONG"
  | Echo something -> Resp.RBulkString something

let handle_input_line line =
  let ( let* ) = Stdlib.Result.bind in
  let* rvalue = Resp.of_string line in
  print_endline (Resp.show rvalue);
  let* command = parse_command rvalue in
  Stdlib.Ok (handle_command command)

let rec handle_client client_fd =
  let out_chan = out_channel_of_descr client_fd in
  let in_chan = in_channel_of_descr client_fd in
  let buf = Bytes.create 100 in
  try
    let nb_read = input in_chan buf 0 100 in
    if nb_read == 0 then raise End_of_file else ();
    let line = Bytes.sub_string buf 0 nb_read in
    print_endline (String.escaped line);
    let response_or_error = handle_input_line line in
    let response =
      Stdlib.Result.fold
        ~ok:(fun x -> x)
        ~error:(fun m -> Resp.RError m)
        response_or_error
    in
    let response_line = Resp.to_string response in
    print_endline response_line;
    Printf.fprintf out_chan "%s" response_line;
    flush out_chan;
    handle_client client_fd
  with End_of_file -> ()

let rec accept_socket_and_handle_client pool server_socket =
  let client_socket, _ = accept server_socket in
  let work () =
    print_endline "New client";
    handle_client client_socket;
    close client_socket;
    print_endline "Client left"
  in
  let _ = Thread_pool.add_work pool work in
  accept_socket_and_handle_client pool server_socket

let () =
  let server_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt server_socket SO_REUSEADDR true;
  bind server_socket (ADDR_INET (inet_addr_of_string "127.0.0.1", 6379));
  listen server_socket 1;
  let pool = Thread_pool.create ~max_num_threads:4 () |> Core.ok_exn in
  accept_socket_and_handle_client pool server_socket

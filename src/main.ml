open Unix

let rec handle_client client_fd =
  let out_chan = out_channel_of_descr client_fd in
  try
    let _ = input_line (in_channel_of_descr client_fd) in
    Printf.fprintf out_chan "+PONG\r\n";
    flush out_chan;
    handle_client client_fd
  with End_of_file -> ()

let () =
  let server_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt server_socket SO_REUSEADDR true;
  bind server_socket (ADDR_INET (inet_addr_of_string "127.0.0.1", 6379));
  listen server_socket 1;

  let client_socket, _ = accept server_socket in
  handle_client client_socket;
  close client_socket;
  close server_socket

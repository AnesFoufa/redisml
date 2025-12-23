open Unix

let handle_connection client_socket =
  Printf.eprintf "Client connected\n%!";
  close client_socket

let start_server port =
  Printf.eprintf "Starting Redis server on port %d\n%!" port;
  let server_socket = socket PF_INET SOCK_STREAM 0 in
  setsockopt server_socket SO_REUSEADDR true;
  bind server_socket (ADDR_INET (inet_addr_any, port));
  listen server_socket 5;

  (* Accept connections in a loop *)
  while true do
    let client_socket, _client_addr = accept server_socket in
    (* Handle each connection in a separate thread *)
    let _thread_id =
      Thread.create
        (fun () ->
          handle_connection client_socket)
        ()
    in
    ()
  done

let () =
  try
    start_server 6379
  with
  | Unix_error (err, fn, arg) ->
      Printf.eprintf "Unix error: %s(%s): %s\n%!" fn arg (error_message err);
      exit 1
  | e ->
      Printf.eprintf "Error: %s\n%!" (Printexc.to_string e);
      exit 1

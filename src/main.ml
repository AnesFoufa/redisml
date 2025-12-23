open Unix

let read_from_socket sock =
  let buffer = Bytes.create 1024 in
  let n = recv sock buffer 0 1024 [] in
  if n = 0 then None else Some (Bytes.sub_string buffer 0 n)

let write_to_socket sock data =
  let _ = send sock (Bytes.of_string data) 0 (String.length data) [] in
  ()

let handle_command = function
  | Resp.Array [ Resp.BulkString cmd ] when String.lowercase_ascii cmd = "ping" ->
      Resp.SimpleString "PONG"
  | Resp.Array (Resp.BulkString cmd :: args)
    when String.lowercase_ascii cmd = "echo" -> (
      match args with
      | [ Resp.BulkString msg ] -> Resp.BulkString msg
      | _ -> Resp.SimpleError "ERR wrong number of arguments for 'echo' command")
  | _ -> Resp.SimpleError "ERR unknown command"

let handle_connection client_socket =
  try
    let buffer = ref "" in
    let running = ref true in

    while !running do
      match read_from_socket client_socket with
      | None ->
          (* Connection closed *)
          running := false
      | Some data ->
          buffer := !buffer ^ data;

          (* Try to parse and handle commands *)
          let rec process_buffer buf =
            match Resp.parse buf with
            | Some (cmd, rest) ->
                let response = handle_command cmd in
                let response_str = Resp.serialize response in
                write_to_socket client_socket response_str;
                process_buffer rest
            | None ->
                (* Incomplete message, keep buffer *)
                buf
          in
          buffer := process_buffer !buffer
    done;

    close client_socket
  with
  | Unix_error (err, fn, _) ->
      Printf.eprintf "Connection error in %s: %s\n%!" fn (error_message err);
      close client_socket
  | e ->
      Printf.eprintf "Error handling connection: %s\n%!" (Printexc.to_string e);
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
      Thread.create (fun () -> handle_connection client_socket) ()
    in
    ()
  done

let () =
  try start_server 6379
  with
  | Unix_error (err, fn, arg) ->
      Printf.eprintf "Unix error: %s(%s): %s\n%!" fn arg (error_message err);
      exit 1
  | e ->
      Printf.eprintf "Error: %s\n%!" (Printexc.to_string e);
      exit 1

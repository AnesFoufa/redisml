(* Server configuration *)

open Result_syntax

type t = {
  port: int;
  dir: string option;
  dbfilename: string option;
  replicaof: (string * int) option;
}

type error = [
  | `InvalidPort of int
  | `InvalidReplicaofFormat of string
  | `InvalidArgument of string
]

let default = {
  port = 6379;
  dir = None;
  dbfilename = None;
  replicaof = None;
}

let is_replica config = Option.is_some config.replicaof

let error_to_string = function
  | `InvalidPort port ->
      Printf.sprintf "Invalid port: %d (must be between 1 and 65535)" port
  | `InvalidReplicaofFormat s ->
      Printf.sprintf "Invalid --replicaof format: '%s' (expected 'host port')" s
  | `InvalidArgument arg ->
      Printf.sprintf "Invalid argument: %s" arg

let validate_port port =
  if port < 1 || port > 65535 then
    Error (`InvalidPort port)
  else
    Ok port

(* Parse command-line arguments *)
let parse_args argv =
  let rec parse config i =
    if i >= Array.length argv then Ok config
    else
      match argv.(i) with
      | "--port" when i + 1 < Array.length argv ->
          let arg = argv.(i + 1) in
          let port_opt = int_of_string_opt arg in
          let error_msg = Printf.sprintf "--port requires integer, got '%s'" arg in
          let* port = Option.to_result ~none:(`InvalidArgument error_msg) port_opt in
          let* valid_port = validate_port port in
          parse { config with port = valid_port } (i + 2)
      | "--dir" when i + 1 < Array.length argv ->
          parse { config with dir = Some argv.(i + 1) } (i + 2)
      | "--dbfilename" when i + 1 < Array.length argv ->
          parse { config with dbfilename = Some argv.(i + 1) } (i + 2)
      | "--replicaof" when i + 1 < Array.length argv ->
          (* Parse "host port" format *)
          let arg = argv.(i + 1) in
          let parts = String.split_on_char ' ' arg in
          let* (host, port_str) =
            match parts with
            | [h; p] -> Ok (h, p)
            | _ -> Error (`InvalidReplicaofFormat arg)
          in
          let port_opt = int_of_string_opt port_str in
          let* port = Option.to_result ~none:(`InvalidReplicaofFormat arg) port_opt in
          let* valid_port = validate_port port in
          parse { config with replicaof = Some (host, valid_port) } (i + 2)
      | "--port" | "--dir" | "--dbfilename" | "--replicaof" ->
          Error (`InvalidArgument (Printf.sprintf "%s requires a value" argv.(i)))
      | _ ->
          parse config (i + 1)
  in
  parse default 0

(* Parse with error logging, falls back to default on error *)
let parse_args_or_default argv =
  match parse_args argv with
  | Ok config -> config
  | Error e ->
      Printf.eprintf "Configuration error: %s\nUsing default configuration.\n%!" (error_to_string e);
      default

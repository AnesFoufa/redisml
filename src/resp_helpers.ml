(* High-level RESP parsing helpers for command processing *)

(* Extract command name from RESP array (case-insensitive) *)
let command_name = function
  | Resp.Array (Resp.BulkString cmd :: _) ->
      Some (String.lowercase_ascii cmd)
  | _ -> None

(* Check if RESP value is a command with given name (case-insensitive) *)
let is_command name = function
  | Resp.Array (Resp.BulkString cmd :: _) ->
      String.lowercase_ascii cmd = String.lowercase_ascii name
  | _ -> false

(* Extract bulk string argument at position (0-indexed after command) *)
let bulk_string_arg n = function
  | Resp.Array (_ :: args) when n < List.length args ->
      (match List.nth args n with
       | Resp.BulkString s -> Some s
       | _ -> None)
  | _ -> None

(* Extract all arguments after command as bulk strings *)
let bulk_string_args = function
  | Resp.Array (_ :: args) ->
      let rec extract = function
        | [] -> Some []
        | Resp.BulkString s :: rest -> (
            match extract rest with
            | Some strs -> Some (s :: strs)
            | None -> None)
        | _ :: _ -> None
      in
      extract args
  | _ -> None

(* Extract integer argument at position (parses bulk string to int) *)
let int_arg n = function
  | Resp.Array (_ :: args) when n < List.length args ->
      (match List.nth args n with
       | Resp.BulkString s -> int_of_string_opt s
       | _ -> None)
  | _ -> None

(* Extract two integer arguments *)
let int_args_2 cmd =
  match int_arg 0 cmd, int_arg 1 cmd with
  | Some a, Some b -> Some (a, b)
  | _ -> None

(* Extract three integer arguments *)
let int_args_3 cmd =
  match int_arg 0 cmd, int_arg 1 cmd, int_arg 2 cmd with
  | Some a, Some b, Some c -> Some (a, b, c)
  | _ -> None

(* Extract argument at position (any RESP type) *)
let arg n = function
  | Resp.Array (_ :: args) when n < List.length args ->
      Some (List.nth args n)
  | _ -> None

(* Get number of arguments (excludes command) *)
let arg_count = function
  | Resp.Array (_ :: args) -> Some (List.length args)
  | _ -> None

(* Match command with specific arity (number of arguments) *)
let command_with_arity name arity = function
  | Resp.Array (Resp.BulkString cmd :: args)
    when String.lowercase_ascii cmd = String.lowercase_ascii name
         && List.length args = arity ->
      Some args
  | _ -> None

(* Match command with minimum arity *)
let command_with_min_arity name min_arity = function
  | Resp.Array (Resp.BulkString cmd :: args)
    when String.lowercase_ascii cmd = String.lowercase_ascii name
         && List.length args >= min_arity ->
      Some args
  | _ -> None

(* Extract key-value pairs from arguments (alternating pattern) *)
let key_value_pairs args =
  let rec extract acc = function
    | [] -> Some (List.rev acc)
    | Resp.BulkString k :: v :: rest ->
        extract ((k, v) :: acc) rest
    | _ -> None
  in
  extract [] args

(* Parse optional parameters from argument list *)
let parse_options parser args =
  let rec loop acc = function
    | [] -> Some (List.rev acc)
    | args -> (
        match parser args with
        | Some (opt, rest) -> loop (opt :: acc) rest
        | None -> Some (List.rev acc))
  in
  loop [] args

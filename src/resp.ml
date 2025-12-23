(* RESP (REdis Serialization Protocol) implementation *)

type t =
  | SimpleString of string
  | SimpleError of string
  | Integer of int
  | BulkString of string
  | Array of t list
  | Null

(* Parse a CRLF-terminated line from a string *)
let parse_line s =
  match String.index_opt s '\r' with
  | Some pos when pos + 1 < String.length s && s.[pos + 1] = '\n' ->
      let line = String.sub s 0 pos in
      let rest = String.sub s (pos + 2) (String.length s - pos - 2) in
      Some (line, rest)
  | _ -> None

(* Parse an integer from a string *)
let parse_int s =
  try Some (int_of_string s) with Failure _ -> None

(* Parse a RESP value from a string *)
let rec parse s =
  if String.length s = 0 then None
  else
    match s.[0] with
    | '+' -> parse_simple_string (String.sub s 1 (String.length s - 1))
    | '-' -> parse_simple_error (String.sub s 1 (String.length s - 1))
    | ':' -> parse_integer (String.sub s 1 (String.length s - 1))
    | '$' -> parse_bulk_string (String.sub s 1 (String.length s - 1))
    | '*' -> parse_array (String.sub s 1 (String.length s - 1))
    | _ -> None

and parse_simple_string s =
  match parse_line s with
  | Some (line, rest) -> Some (SimpleString line, rest)
  | None -> None

and parse_simple_error s =
  match parse_line s with
  | Some (line, rest) -> Some (SimpleError line, rest)
  | None -> None

and parse_integer s =
  match parse_line s with
  | Some (line, rest) -> (
      match parse_int line with
      | Some n -> Some (Integer n, rest)
      | None -> None)
  | None -> None

and parse_bulk_string s =
  match parse_line s with
  | Some (len_str, rest) -> (
      match parse_int len_str with
      | Some len when len < 0 ->
          (* Null bulk string *)
          Some (Null, rest)
      | Some len ->
          if String.length rest >= len + 2 then
            let data = String.sub rest 0 len in
            let rest' = String.sub rest (len + 2) (String.length rest - len - 2) in
            Some (BulkString data, rest')
          else None
      | None -> None)
  | None -> None

and parse_array s =
  match parse_line s with
  | Some (len_str, rest) -> (
      match parse_int len_str with
      | Some len -> parse_array_elements len rest []
      | None -> None)
  | None -> None

and parse_array_elements count rest acc =
  if count = 0 then Some (Array (List.rev acc), rest)
  else
    match parse rest with
    | Some (elem, rest') -> parse_array_elements (count - 1) rest' (elem :: acc)
    | None -> None

(* Parse multiple RESP values from a string, return parsed values and remainder *)
let parse_many s =
  let rec loop acc rest =
    match parse rest with
    | Some (value, rest') -> loop (value :: acc) rest'
    | None -> (List.rev acc, rest)
  in
  loop [] s

(* Serialize a RESP value to a string *)
let rec serialize = function
  | SimpleString s -> "+" ^ s ^ "\r\n"
  | SimpleError s -> "-" ^ s ^ "\r\n"
  | Integer n -> ":" ^ string_of_int n ^ "\r\n"
  | BulkString s ->
      "$" ^ string_of_int (String.length s) ^ "\r\n" ^ s ^ "\r\n"
  | Array arr ->
      let serialized = List.map serialize arr |> String.concat "" in
      "*" ^ string_of_int (List.length arr) ^ "\r\n" ^ serialized
  | Null -> "$-1\r\n"

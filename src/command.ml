(* Redis command types and execution *)

type replconf_command =
  | ReplconfListeningPort of int
  | ReplconfCapa of string
  | ReplconfGetAck

type t =
  | Ping
  | Echo of Resp.t
  | Get of string
  | Set of string * Resp.t * int option
  | InfoReplication
  | Replconf of replconf_command
  | Psync of string * int

(* Parse duration options for SET command - returns milliseconds *)
let parse_duration = function
  | Resp.BulkString opt :: Resp.BulkString time_str :: rest -> (
      match String.lowercase_ascii opt, int_of_string_opt time_str with
      | "px", Some ms -> Some ms, rest
      | "ex", Some sec -> Some (sec * 1000), rest
      | _ -> None, rest)
  | rest -> None, rest

(* Parse REPLCONF subcommands *)
let parse_replconf = function
  | [ Resp.BulkString arg1; Resp.BulkString arg2 ] ->
      let arg1_str = String.lowercase_ascii arg1 in
      (match arg1_str with
       | "listening-port" ->
           int_of_string_opt arg2
           |> Option.map (fun port -> Replconf (ReplconfListeningPort port))
       | "capa" ->
           Some (Replconf (ReplconfCapa arg2))
       | "getack" ->
           Some (Replconf ReplconfGetAck)
       | _ -> None)
  | _ -> None

(* Parse a RESP value into a command *)
let parse = function
  | Resp.Array [ Resp.BulkString cmd ]
    when String.lowercase_ascii cmd = "ping" ->
      Some Ping

  | Resp.Array [ Resp.BulkString cmd; msg ]
    when String.lowercase_ascii cmd = "echo" ->
      Some (Echo msg)

  | Resp.Array [ Resp.BulkString cmd; Resp.BulkString key ]
    when String.lowercase_ascii cmd = "get" ->
      Some (Get key)

  | Resp.Array (Resp.BulkString cmd :: Resp.BulkString key :: value :: rest)
    when String.lowercase_ascii cmd = "set" ->
      let duration, _ = parse_duration rest in
      Some (Set (key, value, duration))

  | Resp.Array [ Resp.BulkString cmd; Resp.BulkString section ]
    when String.lowercase_ascii cmd = "info" ->
      if String.contains (String.lowercase_ascii section) 'r' then
        Some InfoReplication
      else
        None

  | Resp.Array (Resp.BulkString cmd :: args)
    when String.lowercase_ascii cmd = "replconf" ->
      parse_replconf args

  | Resp.Array [ Resp.BulkString cmd; Resp.BulkString replid; Resp.BulkString offset ]
    when String.lowercase_ascii cmd = "psync" ->
      let offset_int = int_of_string_opt offset |> Option.value ~default:(-1) in
      Some (Psync (replid, offset_int))

  | _ -> None


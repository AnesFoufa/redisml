(* Redis command types and execution *)

(* Command parsing/validation errors *)
type error = [
  | `InvalidExpiry of int
  | `InvalidPort of int
  | `InvalidTimeout of int
  | `InvalidNumReplicas of int
  | `UnknownCommand
  | `MalformedCommand of string
]

type replconf_command =
  | ReplconfListeningPort of int
  | ReplconfCapa of string
  | ReplconfGetAck

type set_params = {
  key: string;
  value: Resp.t;
  expiry_ms: int option;
}

type psync_params = {
  replication_id: string;
  offset: int;
}

type wait_params = {
  num_replicas: int;
  timeout_ms: int;
}

type config_param =
  | Dir
  | Dbfilename

type read
type write

type _ t =
  | Ping : read t
  | Echo : Resp.t -> read t
  | Get : string -> read t
  | Set : set_params -> write t
  | InfoReplication : read t
  | Replconf : replconf_command -> read t
  | Psync : psync_params -> write t
  | Wait : wait_params -> write t
  | ConfigGet : config_param -> read t
  | Keys : string -> read t

type parsed_t = Read of read t | Write of write t

(* Validation functions using Validation framework *)
let validate_set_params params =
  Validation.validate_option
    (Validation.expiry_ms ~error:(fun ms -> `InvalidExpiry ms))
    params.expiry_ms
  |> Result.map (fun _ -> ())

let validate_replconf_port port =
  Validation.port ~error:(fun p -> `InvalidPort p) port
  |> Result.map (fun _ -> ())

let validate_wait_params params =
  Validation.validate_all [
    (fun () -> Validation.timeout_ms ~error:(fun t -> `InvalidTimeout t) params.timeout_ms |> Result.map (fun _ -> ()));
    (fun () -> Validation.replica_count ~error:(fun n -> `InvalidNumReplicas n) params.num_replicas |> Result.map (fun _ -> ()));
  ]

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
           (match int_of_string_opt arg2 with
            | Some port ->
                (match validate_replconf_port port with
                 | Ok () -> Ok (Replconf (ReplconfListeningPort port))
                 | Error _ as e -> e)
            | None -> Error (`MalformedCommand "REPLCONF listening-port requires integer"))
       | "capa" ->
           Ok (Replconf (ReplconfCapa arg2))
       | "getack" ->
           Ok (Replconf ReplconfGetAck)
       | _ -> Error (`MalformedCommand ("Unknown REPLCONF subcommand: " ^ arg1)))
  | _ -> Error (`MalformedCommand "REPLCONF requires exactly 2 arguments")

(* Parse a RESP value into a command *)
let parse = function
  | Resp.Array [ Resp.BulkString cmd ]
    when String.lowercase_ascii cmd = "ping" ->
      Ok (Read Ping)

  | Resp.Array [ Resp.BulkString cmd; msg ]
    when String.lowercase_ascii cmd = "echo" ->
      Ok (Read (Echo msg))

  | Resp.Array [ Resp.BulkString cmd; Resp.BulkString key ]
    when String.lowercase_ascii cmd = "get" ->
      Ok (Read (Get key))

  | Resp.Array (Resp.BulkString cmd :: Resp.BulkString key :: value :: rest)
    when String.lowercase_ascii cmd = "set" ->
      let expiry_ms, _ = parse_duration rest in
      let params = { key; value; expiry_ms } in
      (match validate_set_params params with
       | Ok () -> Ok (Write (Set params))
       | Error _ as e -> e)

  | Resp.Array [ Resp.BulkString cmd; Resp.BulkString section ]
    when String.lowercase_ascii cmd = "info" ->
      if String.contains (String.lowercase_ascii section) 'r' then
        Ok (Read InfoReplication)
      else
        Error (`MalformedCommand "INFO requires 'replication' section")

  | Resp.Array (Resp.BulkString cmd :: args)
    when String.lowercase_ascii cmd = "replconf" ->
      parse_replconf args |> Result.map (fun cmd -> Read cmd)

  | Resp.Array [ Resp.BulkString cmd; Resp.BulkString replication_id; Resp.BulkString offset_str ]
    when String.lowercase_ascii cmd = "psync" ->
      let offset = int_of_string_opt offset_str |> Option.value ~default:(-1) in
      Ok (Write (Psync { replication_id; offset }))

  | Resp.Array [ Resp.BulkString cmd; Resp.BulkString num_replicas_str; Resp.BulkString timeout_str ]
    when String.lowercase_ascii cmd = "wait" ->
      (match int_of_string_opt num_replicas_str, int_of_string_opt timeout_str with
       | Some num_replicas, Some timeout_ms ->
           let params = { num_replicas; timeout_ms } in
           (match validate_wait_params params with
            | Ok () -> Ok (Write (Wait params))
            | Error _ as e -> e)
       | _ -> Error (`MalformedCommand "WAIT requires two integer arguments"))

  | Resp.Array [ Resp.BulkString cmd; Resp.BulkString subcommand; Resp.BulkString param ]
    when String.lowercase_ascii cmd = "config" && String.lowercase_ascii subcommand = "get" ->
      (match String.lowercase_ascii param with
       | "dir" -> Ok (Read (ConfigGet Dir))
       | "dbfilename" -> Ok (Read (ConfigGet Dbfilename))
       | _ -> Error (`MalformedCommand ("Unknown CONFIG parameter: " ^ param)))

  | Resp.Array [ Resp.BulkString cmd; Resp.BulkString pattern ]
    when String.lowercase_ascii cmd = "keys" ->
      Ok (Read (Keys pattern))

  | Resp.Array (Resp.BulkString _cmd :: _) ->
      Error `UnknownCommand

  | _ ->
      Error (`MalformedCommand "Expected RESP Array with command")

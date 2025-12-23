(* Redis command types and execution *)

type expiry =
  | ExpireMilliseconds of int
  | ExpireSeconds of int
  | NoExpiry

type t =
  | Ping
  | Echo of Resp.t
  | Get of Resp.t
  | Set of Resp.t * Resp.t * expiry

(* Parse expiry options for SET command *)
let parse_expiry = function
  | Resp.BulkString opt :: Resp.BulkString time_str :: rest -> (
      match String.lowercase_ascii opt, int_of_string_opt time_str with
      | "px", Some ms -> ExpireMilliseconds ms, rest
      | "ex", Some sec -> ExpireSeconds sec, rest
      | _ -> NoExpiry, rest)
  | rest -> NoExpiry, rest

(* Parse a RESP value into a command *)
let parse = function
  | Resp.Array [ Resp.BulkString cmd ]
    when String.lowercase_ascii cmd = "ping" ->
      Some Ping

  | Resp.Array [ Resp.BulkString cmd; msg ]
    when String.lowercase_ascii cmd = "echo" ->
      Some (Echo msg)

  | Resp.Array [ Resp.BulkString cmd; key ]
    when String.lowercase_ascii cmd = "get" ->
      Some (Get key)

  | Resp.Array (Resp.BulkString cmd :: key :: value :: rest)
    when String.lowercase_ascii cmd = "set" ->
      let expiry, _ = parse_expiry rest in
      Some (Set (key, value, expiry))

  | _ -> None

(* Calculate Unix timestamp for expiry *)
let calculate_expiry = function
  | ExpireMilliseconds ms -> Some (Unix.gettimeofday () +. (float_of_int ms /. 1000.0))
  | ExpireSeconds sec -> Some (Unix.gettimeofday () +. float_of_int sec)
  | NoExpiry -> None

(* Execute a command against storage *)
let execute cmd storage =
  match cmd with
  | Ping ->
      Resp.SimpleString "PONG"

  | Echo msg ->
      msg

  | Get key -> (
      match Storage.get storage (Resp.serialize key) with
      | Some resp_value -> resp_value
      | None -> Resp.Null)

  | Set (key, value, expiry) ->
      let expires_at = calculate_expiry expiry in
      Storage.set storage (Resp.serialize key) value expires_at;
      Resp.SimpleString "OK"

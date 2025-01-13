type t = Ping | Echo of string | Get of string | Set of (string * Resp.t)

let of_resp = function
  | Resp.RArray [ Resp.RBulkString ping ]
    when String.uppercase_ascii ping |> String.equal "PING" ->
      Stdlib.Ok Ping
  | Resp.RArray [ Resp.RBulkString echo; Resp.RBulkString arg ]
    when echo |> String.uppercase_ascii |> String.equal "ECHO" ->
      Stdlib.Ok (Echo arg)
  | Resp.RArray [ Resp.RBulkString get; Resp.RBulkString key ]
    when get |> String.uppercase_ascii |> String.equal "GET" ->
      Ok (Get key)
  | Resp.RArray [ Resp.RBulkString set; Resp.RBulkString key; value ]
    when set |> String.uppercase_ascii |> String.equal "SET" ->
      Ok (Set (key, value))
  | _ -> Stdlib.Error "Unknown command"

let handle state = function
  | Ping -> Resp.RString "PONG"
  | Echo something -> Resp.RBulkString something
  | Get key ->
      Hashtbl.find_opt state key |> Stdlib.Option.value ~default:Resp.NullBulk
  | Set (key, value) ->
      Hashtbl.add state key value;
      RString "OK"

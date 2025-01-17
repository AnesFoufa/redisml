type t =
  | Ping
  | Echo of string
  | Get of string
  | Set of { key : string; value : Resp.t; duration : int option }
  | Config_get_dir
  | Config_get_dbfilename
  | Keys

let of_resp = function
  | Resp.RArray [ Resp.RBulkString ping ]
    when String.uppercase_ascii ping |> String.equal "PING" ->
      Ok Ping
  | Resp.RArray [ Resp.RBulkString echo; Resp.RBulkString arg ]
    when echo |> String.uppercase_ascii |> String.equal "ECHO" ->
      Ok (Echo arg)
  | Resp.RArray [ Resp.RBulkString get; Resp.RBulkString key ]
    when get |> String.uppercase_ascii |> String.equal "GET" ->
      Ok (Get key)
  | Resp.RArray [ Resp.RBulkString set; Resp.RBulkString key; value ]
    when set |> String.uppercase_ascii |> String.equal "SET" ->
      Ok (Set { key; value; duration = None })
  | Resp.RArray
      [
        Resp.RBulkString set;
        Resp.RBulkString key;
        value;
        RBulkString px;
        RBulkString duration;
      ]
    when set |> String.uppercase_ascii |> String.equal "SET"
         && px |> String.uppercase_ascii |> String.equal "PX" ->
      Ok (Set { key; value; duration = int_of_string_opt duration })
  | Resp.RArray [ RBulkString config; RBulkString get; RBulkString "dir" ]
    when get |> String.uppercase_ascii |> String.equal "GET"
         && config |> String.uppercase_ascii |> String.equal "CONFIG" ->
      Ok Config_get_dir
  | Resp.RArray
      [ RBulkString config; RBulkString get; RBulkString "dbfilename" ]
    when get |> String.uppercase_ascii |> String.equal "GET"
         && config |> String.uppercase_ascii |> String.equal "CONFIG" ->
      Ok Config_get_dbfilename
  | Resp.RArray [ RBulkString keys; RBulkString "*" ]
    when keys |> String.uppercase_ascii |> String.equal "KEYS" ->
      Ok Keys
  | _ -> Error "Unknown command"

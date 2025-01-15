type t =
  | Ping
  | Echo of string
  | Get of string
  | Set of { key : string; value : Resp.t; duration : int option }
  | Config_get_dir
  | Config_get_dbfilename

type record = { value : Resp.t; created_at : int; duration : int option }
type database = (string, record) Hashtbl.t
type config = { dir : string; dbfilename : string }
type state = { database : database; config : config }

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
  | _ -> Error "Unknown command"

let get_value now = function
  | { value; created_at = _created_at; duration = None } -> value
  | { value; created_at; duration = Some duration }
    when created_at + duration >= now ->
      value
  | _ -> Resp.NullBulk

let now () = Unix.gettimeofday () *. 1000. |> Float.to_int

let handle state = function
  | Ping -> Resp.RString "PONG"
  | Echo something -> Resp.RBulkString something
  | Get key ->
      let now = now () in
      let res =
        Hashtbl.find_opt state.database key
        |> Option.map (get_value now)
        |> Option.value ~default:Resp.NullBulk
      in
      if Resp.equal res Resp.NullBulk then Hashtbl.remove state.database key
      else ();
      res
  | Set { key; value; duration } ->
      let created_at = now () in
      Hashtbl.add state.database key { value; created_at; duration };
      RString "OK"
  | Config_get_dir ->
      Resp.RArray [ RBulkString "dir"; RBulkString state.config.dir ]
  | Config_get_dbfilename ->
      Resp.RArray
        [ RBulkString "dbfilename"; RBulkString state.config.dbfilename ]

let init_state ~dbfilename ~dir =
  let config = { dbfilename; dir } in
  let database = Hashtbl.create 256 in
  { config; database }

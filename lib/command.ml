type t =
  | Ping
  | Echo of string
  | Get of string
  | Set of { key : string; value : Resp.t; duration : int option }

type record = { value : Resp.t; created_at : int; duration : int option }
type state = (string, record) Hashtbl.t

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
        Hashtbl.find_opt state key
        |> Option.map (get_value now)
        |> Option.value ~default:Resp.NullBulk
      in
      if Resp.equal res Resp.NullBulk then Hashtbl.remove state key else ();
      res
  | Set { key; value; duration } ->
      let created_at = now () in
      Hashtbl.add state key { value; created_at; duration };
      RString "OK"

let init_state () = Hashtbl.create 256

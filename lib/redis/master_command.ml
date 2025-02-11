type update = {
  key : string;
  value : Resp.t;
  now : Int64.t;
  duration : Int64.t option;
}

type t =
  | Update of update
  | Ping
  | Getack
  | Pong
  | Ok
  | Full_resync
  | Rdb_resync of (Rdb.metadata * Rdb.databases)

let update_to_resp =
  let open Resp in
  function
  | { key; value; now = _now; duration = None } ->
      RArray [ RBulkString "SET"; RBulkString key; value ]
  | { key; value; now = _now; duration = Some d } ->
      RArray
        [
          RBulkString "SET";
          RBulkString key;
          value;
          RBulkString "px";
          RBulkString (Int64.to_string d);
        ]

let length = function
  | Ping -> 14
  | Getack -> 37
  | Update update -> update_to_resp update |> Resp.to_string |> String.length
  | _ -> 0

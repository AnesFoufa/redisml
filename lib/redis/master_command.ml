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

let rdb_sync =
  let is_digit = function '0' .. '9' -> true | _ -> false in
  let open Angstrom in
  let* _ = char '$' in
  let* digits_str = Angstrom.take_while1 is_digit in
  let _nb_chars = int_of_string digits_str in
  let* _ = string "\r\n" in
  Rdb.rdb

type master_message = A of Resp.t | B of (Rdb.metadata * Rdb.databases)

let string_equal_case_insensitive s1 s2 =
  let upper_s1 = s1 |> String.uppercase_ascii in
  let upper_s2 = s2 |> String.uppercase_ascii in
  upper_s1 |> String.equal upper_s2

let parse_commands now resps =
  let open Resp in
  resps
  |> List.filter_map (fun message ->
         match message with
         | A resp -> (
             match resp with
             | RString pong when pong |> string_equal_case_insensitive "pong" ->
                 Some Pong
             | RString fullresync
               when fullresync |> String.uppercase_ascii
                    |> String.starts_with ~prefix:"FULLRESYNC" ->
                 Some Full_resync
             | RString ok when string_equal_case_insensitive ok "ok" -> Some Ok
             | RArray [ RBulkString set; RBulkString key; value ]
               when string_equal_case_insensitive set "set" ->
                 Some (Update { key; value; now; duration = None })
             | RArray
                 [
                   RBulkString set;
                   RBulkString key;
                   value;
                   RBulkString px;
                   RBulkString duration;
                 ]
               when string_equal_case_insensitive set "set"
                    && string_equal_case_insensitive px "px" ->
                 Some
                   (Update
                      {
                        key;
                        value;
                        now;
                        duration = Int64.of_string_opt duration;
                      })
             | RArray [ RBulkString ping ]
               when string_equal_case_insensitive ping "ping" ->
                 Some Ping
             | RArray
                 [ RBulkString replconf; RBulkString getack; RBulkString "*" ]
               when string_equal_case_insensitive replconf "replconf"
                    && string_equal_case_insensitive getack "getack" ->
                 Some Getack
             | _ -> None)
         | B (metadata, databases) -> Some (Rdb_resync (metadata, databases)))

let master_commands now =
  let open Angstrom in
  let resp_message = Resp.resp >>| fun x -> A x in
  let rdb_message = rdb_sync >>| fun x -> B x in
  let messages = many (resp_message <|> rdb_message) in
  messages >>| parse_commands now

let length = function
  | Ping -> 14
  | Getack -> 37
  | Update update -> update_to_resp update |> Resp.to_string |> String.length
  | _ -> 0

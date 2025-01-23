type t = { mutable current_index : int; redis : Redis.t }

let init redis = { current_index = 0; redis }

let get_value now record =
  let open Redis.Database in
  match record with
  | { value; expire = None } -> value
  | { value; expire = Some timestamp } when timestamp >= now -> value
  | _ -> Resp.NullBulk

let get_database client = Redis.get_database client.redis client.current_index

let evaluate_command client ~now command =
  let open Command in
  match command with
  | Ping -> Resp.RString "PONG"
  | Echo something -> Resp.RBulkString something
  | Get key ->
      let database = get_database client in
      let res =
        Hashtbl.find_opt database key
        |> Option.map (get_value now)
        |> Option.value ~default:Resp.NullBulk
      in
      if Resp.equal res Resp.NullBulk then Hashtbl.remove database key else ();
      res
  | Set { key; value; duration } ->
      let database = get_database client in
      let expire = duration |> Option.map (fun t -> Int64.add t now) in
      Hashtbl.remove database key;
      Hashtbl.add database key { value; expire };
      RString "OK"
  | Config_get_dir ->
      Resp.RArray
        [ RBulkString "dir"; RBulkString (Redis.get_dir client.redis) ]
  | Config_get_dbfilename ->
      Resp.RArray
        [
          RBulkString "dbfilename";
          RBulkString (Redis.get_dbfilename client.redis);
        ]
  | Keys ->
      let database = get_database client in
      RArray
        (database |> Hashtbl.to_seq_keys
        |> Seq.map (fun x -> Resp.RBulkString x)
        |> List.of_seq)
  | Select i ->
      client.current_index <- i;
      RString "OK"
  | Info_replication ->
      let response =
        match Redis.get_replication_role client.redis with
        | Redis.Master { replid; repl_offset } ->
            Printf.sprintf
              "role:master\n\rmaster_replid:%s\n\rmaster_repl_offset:%d" replid
              repl_offset
        | Slave _ -> "role:slave"
      in
      let response_with_header = Printf.sprintf "#Replication\n\r%s" response in
      RBulkString response_with_header
  | Repl_conf -> RString "OK"

let evaluate client ~now input =
  let response_res =
    let ( let* ) = Result.bind in
    let* command = Command.of_resp input in
    let response = evaluate_command client ~now command in
    Result.Ok response
  in
  match response_res with
  | Result.Ok response -> response
  | Result.Error m -> Resp.RError m

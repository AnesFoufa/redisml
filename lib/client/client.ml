type t = { mutable current_index : int; databases : Redis.t }

let init databases = { current_index = 0; databases }

let get_value now record =
  let open Redis.Database in
  match record with
  | { value; expire = None } -> value
  | { value; expire = Some timestamp } when timestamp >= now -> value
  | _ -> Resp.NullBulk

let get_database evaluator =
  Redis.get_database evaluator.databases evaluator.current_index

let evaluate_command evaluator ~now command =
  let open Command in
  match command with
  | Ping -> Resp.RString "PONG"
  | Echo something -> Resp.RBulkString something
  | Get key ->
      let database = get_database evaluator in
      let res =
        Hashtbl.find_opt database key
        |> Option.map (get_value now)
        |> Option.value ~default:Resp.NullBulk
      in
      if Resp.equal res Resp.NullBulk then Hashtbl.remove database key else ();
      res
  | Set { key; value; duration } ->
      let database = get_database evaluator in
      let expire = duration |> Option.map (fun t -> Int64.add t now) in
      Hashtbl.remove database key;
      Hashtbl.add database key { value; expire };
      RString "OK"
  | Config_get_dir ->
      Resp.RArray
        [ RBulkString "dir"; RBulkString (Redis.get_dir evaluator.databases) ]
  | Config_get_dbfilename ->
      Resp.RArray
        [
          RBulkString "dbfilename";
          RBulkString (Redis.get_dbfilename evaluator.databases);
        ]
  | Keys ->
      let database = get_database evaluator in
      RArray
        (database |> Hashtbl.to_seq_keys
        |> Seq.map (fun x -> Resp.RBulkString x)
        |> List.of_seq)
  | Select i ->
      evaluator.current_index <- i;
      RString "OK"

let evaluate evaluator ~now input =
  let response_res =
    let ( let* ) = Result.bind in
    let* command = Command.of_resp input in
    let response = evaluate_command evaluator ~now command in
    Result.Ok response
  in
  match response_res with
  | Result.Ok response -> response
  | Result.Error m -> Resp.RError m

type record = { value : Resp.t; created_at : int; duration : int option }
type database = (string, record) Hashtbl.t
type config = { dir : string; dbfilename : string }
type t = { database : database; config : config }

let init ~dbfilename ~dir =
  let config = { dir; dbfilename } in
  let database = Hashtbl.create 256 in
  { database; config }

let get_value now = function
  | { value; created_at = _created_at; duration = None } -> value
  | { value; created_at; duration = Some duration }
    when created_at + duration >= now ->
      value
  | _ -> Resp.NullBulk

let evaluate_command evaluator ~now command =
  let open Command in
  match command with
  | Ping -> Resp.RString "PONG"
  | Echo something -> Resp.RBulkString something
  | Get key ->
      let res =
        Hashtbl.find_opt evaluator.database key
        |> Option.map (get_value now)
        |> Option.value ~default:Resp.NullBulk
      in
      if Resp.equal res Resp.NullBulk then Hashtbl.remove evaluator.database key
      else ();
      res
  | Set { key; value; duration } ->
      let created_at = now in
      Hashtbl.add evaluator.database key { value; created_at; duration };
      RString "OK"
  | Config_get_dir ->
      Resp.RArray [ RBulkString "dir"; RBulkString evaluator.config.dir ]
  | Config_get_dbfilename ->
      Resp.RArray
        [ RBulkString "dbfilename"; RBulkString evaluator.config.dbfilename ]
  | Keys ->
      RArray
        (evaluator.database |> Hashtbl.to_seq_keys
        |> Seq.map (fun x -> Resp.RBulkString x)
        |> List.of_seq)

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

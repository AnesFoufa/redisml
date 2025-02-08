type t = { mutable current_index : int; redis : Redis.t; id : Redis.client_id }

let init redis =
  let id, stream_chanel = Redis.connect redis in
  ({ current_index = 0; redis; id }, stream_chanel)

let evaluate_command client ~now command redis =
  let index = client.current_index in
  let open Command in
  let redis_command =
    match command with
    | Ping -> Redis.Command.Ping
    | Echo something -> Redis.Command.Echo something
    | Command.Get key -> Redis.Command.Get { index; key; now }
    | Set { key; value; duration } ->
        Redis.Command.Set { index; key; value; duration; now }
    | Config_get_dir -> Redis.Command.Config_get_dir
    | Config_get_dbfilename -> Redis.Command.Config_get_dbfilename
    | Keys -> Redis.Command.Keys { index; now }
    | Select i ->
        client.current_index <- i;
        Redis.Command.Select i
    | Info_replication -> Redis.Command.Info_replication
    | Repl_conf -> Redis.Command.Repl_conf
    | Psync -> Redis.Command.Psync
    | Wait -> Redis.Command.Wait
  in
  Redis.handle_command client.id redis redis_command

let evaluate client ~now input =
  let response_res =
    let ( let* ) = Result.bind in
    let* command = Command.of_resp input in
    let response = evaluate_command client ~now command client.redis in
    let resp_response =
      Result.fold
        ~ok:(fun x -> x)
        ~error:(fun _ -> Resp.RError "Unhandled command")
        response
    in
    Result.Ok resp_response
  in
  match response_res with
  | Result.Ok response_res -> response_res
  | Result.Error m -> Resp.RError m

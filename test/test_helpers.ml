(* Test helpers for database testing *)
open Lwt.Syntax

(* Create mock Lwt channels that discard output *)
let create_mock_channels () =
  let (ic_dummy, oc_dummy) = Lwt_io.pipe () in
  Lwt.return (ic_dummy, oc_dummy, "127.0.0.1:0")

(* Execute a command synchronously for testing *)
let exec_command cmd db ~current_time =
  Lwt_main.run (
    let* (ic, oc, address) = create_mock_channels () in
    let original_resp = match cmd with
      | Codecrafters_redis.Command.Ping ->
          Codecrafters_redis.Resp.Array [Codecrafters_redis.Resp.BulkString "PING"]
      | Codecrafters_redis.Command.Echo msg ->
          Codecrafters_redis.Resp.Array [Codecrafters_redis.Resp.BulkString "ECHO"; msg]
      | Codecrafters_redis.Command.Get key ->
          Codecrafters_redis.Resp.Array [
            Codecrafters_redis.Resp.BulkString "GET";
            Codecrafters_redis.Resp.BulkString key
          ]
      | Codecrafters_redis.Command.Set { key; value; expiry_ms } ->
          let base = [
            Codecrafters_redis.Resp.BulkString "SET";
            Codecrafters_redis.Resp.BulkString key;
            value
          ] in
          let with_expiry = match expiry_ms with
            | Some ms -> base @ [
                Codecrafters_redis.Resp.BulkString "PX";
                Codecrafters_redis.Resp.BulkString (string_of_int ms)
              ]
            | None -> base
          in
          Codecrafters_redis.Resp.Array with_expiry
      | Codecrafters_redis.Command.InfoReplication ->
          Codecrafters_redis.Resp.Array [
            Codecrafters_redis.Resp.BulkString "INFO";
            Codecrafters_redis.Resp.BulkString "replication"
          ]
      | Codecrafters_redis.Command.Replconf rcmd ->
          (match rcmd with
           | Codecrafters_redis.Command.ReplconfListeningPort port ->
               Codecrafters_redis.Resp.Array [
                 Codecrafters_redis.Resp.BulkString "REPLCONF";
                 Codecrafters_redis.Resp.BulkString "listening-port";
                 Codecrafters_redis.Resp.BulkString (string_of_int port)
               ]
           | Codecrafters_redis.Command.ReplconfCapa capa ->
               Codecrafters_redis.Resp.Array [
                 Codecrafters_redis.Resp.BulkString "REPLCONF";
                 Codecrafters_redis.Resp.BulkString "capa";
                 Codecrafters_redis.Resp.BulkString capa
               ]
           | Codecrafters_redis.Command.ReplconfGetAck ->
               Codecrafters_redis.Resp.Array [
                 Codecrafters_redis.Resp.BulkString "REPLCONF";
                 Codecrafters_redis.Resp.BulkString "GETACK";
                 Codecrafters_redis.Resp.BulkString "*"
               ])
      | Codecrafters_redis.Command.Psync { replication_id; offset } ->
          Codecrafters_redis.Resp.Array [
            Codecrafters_redis.Resp.BulkString "PSYNC";
            Codecrafters_redis.Resp.BulkString replication_id;
            Codecrafters_redis.Resp.BulkString (string_of_int offset)
          ]
      | Codecrafters_redis.Command.Wait { num_replicas; timeout_ms } ->
          Codecrafters_redis.Resp.Array [
            Codecrafters_redis.Resp.BulkString "WAIT";
            Codecrafters_redis.Resp.BulkString (string_of_int num_replicas);
            Codecrafters_redis.Resp.BulkString (string_of_int timeout_ms)
          ]
      | Codecrafters_redis.Command.ConfigGet param ->
          let param_str = match param with
            | Codecrafters_redis.Command.Dir -> "dir"
            | Codecrafters_redis.Command.Dbfilename -> "dbfilename"
          in
          Codecrafters_redis.Resp.Array [
            Codecrafters_redis.Resp.BulkString "CONFIG";
            Codecrafters_redis.Resp.BulkString "GET";
            Codecrafters_redis.Resp.BulkString param_str
          ]
      | Codecrafters_redis.Command.Keys pattern ->
          Codecrafters_redis.Resp.Array [
            Codecrafters_redis.Resp.BulkString "KEYS";
            Codecrafters_redis.Resp.BulkString pattern
          ]
    in
    let* result_opt = Codecrafters_redis.Database.handle_command db cmd
      ~current_time
      ~original_resp
      ~ic
      ~oc
      ~address
    in
    match result_opt with
    | Some resp -> Lwt.return resp
    | None ->
        (* For commands that write directly to the channel (like PSYNC),
           read the response from the channel *)
        (match cmd with
         | Codecrafters_redis.Command.Psync _ ->
             (* Read the FULLRESYNC response from the channel *)
             let* data = Lwt_io.read ~count:4096 ic in
             (match Codecrafters_redis.Resp.parse data with
              | Some (resp, _) -> Lwt.return resp
              | None -> Lwt.return Codecrafters_redis.Resp.Null)
         | _ -> Lwt.return Codecrafters_redis.Resp.Null)
  )

(* Check if database is in replica role by examining INFO output *)
let is_replica db =
  let result = exec_command Codecrafters_redis.Command.InfoReplication db ~current_time:0.0 in
  match result with
  | Codecrafters_redis.Resp.BulkString info ->
      String.contains info 's' && (
        try
          let _ = Str.search_forward (Str.regexp "role:slave") info 0 in
          true
        with Not_found -> false
      )
  | _ -> false

(* Check if database is in master role by examining INFO output *)
let is_master db =
  let result = exec_command Codecrafters_redis.Command.InfoReplication db ~current_time:0.0 in
  match result with
  | Codecrafters_redis.Resp.BulkString info ->
      (try
        let _ = Str.search_forward (Str.regexp "role:master") info 0 in
        true
      with Not_found -> false)
  | _ -> false

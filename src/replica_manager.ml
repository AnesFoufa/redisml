open Lwt.Syntax

(* Replica connection with both input and output channels *)
type replica_connection = {
  ic: Lwt_io.input_channel;
  oc: Lwt_io.output_channel;
  address: string; [@warning "-69"]  (* Used for logging but not read *)
}

(* Replica manager state *)
type t = {
  mutable replicas : replica_connection list;
  mutable commands_propagated : int;  (* Count of commands propagated to replicas *)
}

let create () = {
  replicas = [];
  commands_propagated = 0;
}

(* Empty RDB file (hex encoded) - minimal valid RDB *)
let empty_rdb = "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2"

(* Propagate command to all connected replicas *)
let propagate_to_replicas manager ~command =
  let cmd_str = Resp.serialize command in
  let rec send_to_all remaining successful =
    match remaining with
    | [] ->
        manager.replicas <- successful;
        manager.commands_propagated <- manager.commands_propagated + 1;
        Lwt.return_unit
    | replica :: rest ->
        Lwt.catch
          (fun () ->
            let* () = Lwt_io.write replica.oc cmd_str in
            let* () = Lwt_io.flush replica.oc in
            send_to_all rest (replica :: successful))
          (fun _exn ->
            (* Replica disconnected, skip it *)
            send_to_all rest successful)
  in
  send_to_all manager.replicas []

(* Register a new replica connection *)
let register_replica manager ~ic ~oc ~address =
  let replica = { ic; oc; address } in
  manager.replicas <- replica :: manager.replicas;
  Lwt.return_unit

(* Remove a replica from tracking *)
let remove_replica manager ~oc =
  manager.replicas <- List.filter (fun r -> r.oc != oc) manager.replicas

(* Count connected replicas *)
let count_replicas manager =
  List.length manager.replicas

(* Wait for replicas to acknowledge - returns number of replicas that responded within timeout *)
let wait_for_replicas manager ~num_replicas:_ ~timeout_ms =
  (* If no commands have been propagated, all replicas are synchronized *)
  if manager.commands_propagated = 0 then
    Lwt.return (List.length manager.replicas)
  else (
    (* Small delay to ensure all replica connections are fully established *)
    let* () = Lwt_unix.sleep 0.05 in

    let getack_cmd = Resp.Array [
      Resp.BulkString "REPLCONF";
      Resp.BulkString "GETACK";
      Resp.BulkString "*"
    ] in
    let getack_str = Resp.serialize getack_cmd in

    (* Send GETACK to all replicas and try to read responses *)
    let check_replica replica =
      Lwt.catch
        (fun () ->
          let* () = Lwt_io.write replica.oc getack_str in
          let* () = Lwt_io.flush replica.oc in

          (* Try to read response with timeout *)
          let read_response () =
            (* Small delay to ensure data arrives *)
            let* () = Lwt_unix.sleep 0.01 in
            let* data = Lwt_io.read ~count:1024 replica.ic in
            match Resp.parse data with
            | Some (Resp.Array [Resp.BulkString "REPLCONF"; Resp.BulkString "ACK"; _], _) ->
                Lwt.return true
            | _ ->
                Lwt.return false
          in
          Lwt_unix.with_timeout (float_of_int timeout_ms /. 1000.0) read_response)
        (fun _exn ->
          (* Timeout or error - replica didn't respond *)
          Lwt.return false)
  in

    (* Check all replicas in parallel *)
    let* results = Lwt_list.map_p check_replica manager.replicas in
    let num_acked = List.fold_left (fun acc responded -> if responded then acc + 1 else acc) 0 results in
    Lwt.return num_acked
  )

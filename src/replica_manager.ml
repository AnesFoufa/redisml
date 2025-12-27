open Lwt.Syntax

(* Replica manager state *)
type t = {
  mutable replicas : Lwt_io.output_channel list;
}

let create () = { replicas = [] }

(* Empty RDB file (hex encoded) - minimal valid RDB *)
let empty_rdb = "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2"

(* Propagate command to all connected replicas *)
let propagate_to_replicas manager ~command =
  let cmd_str = Resp.serialize command in
  let rec send_to_all remaining successful =
    match remaining with
    | [] ->
        manager.replicas <- successful;
        Lwt.return_unit
    | replica_oc :: rest ->
        Lwt.catch
          (fun () ->
            let* () = Lwt_io.write replica_oc cmd_str in
            let* () = Lwt_io.flush replica_oc in
            send_to_all rest (replica_oc :: successful))
          (fun _exn ->
            (* Replica disconnected, skip it *)
            send_to_all rest successful)
  in
  send_to_all manager.replicas []

(* Register a new replica connection *)
let register_replica manager ~channel ~address =
  manager.replicas <- channel :: manager.replicas;
  Lwt_io.eprintlf "Registered replica from %s" address

(* Remove a replica from tracking *)
let remove_replica manager ~channel =
  manager.replicas <- List.filter (fun r -> r != channel) manager.replicas

(* Count connected replicas *)
let count_replicas manager =
  List.length manager.replicas

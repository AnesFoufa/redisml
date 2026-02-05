(* Redis command types and execution *)

type replconf_command =
  | ReplconfListeningPort of int
  | ReplconfCapa of string
  | ReplconfGetAck

type set_params = { key : string; value : Resp.t; expiry_ms : int option }
type psync_params = { replication_id : string; offset : int }
type wait_params = { num_replicas : int; timeout_ms : int }
type config_param = Dir | Dbfilename
type read
type write

type _ t = private
  | Ping : read t
  | Echo : Resp.t -> read t
  | Get : string -> read t
  | Set : set_params -> write t
  | InfoReplication : read t
  | Replconf : replconf_command -> read t
  | Psync : psync_params -> write t
  | Wait : wait_params -> write t
  | ConfigGet : config_param -> read t
  | Keys : string -> read t

(* Command parsing/validation errors *)
type error =
  [ `InvalidExpiry of int
  | `InvalidPort of int
  | `InvalidTimeout of int
  | `InvalidNumReplicas of int
  | `UnknownCommand
  | `MalformedCommand of string ]

type parsed_t = Read of read t | Write of write t

(* Parse a RESP value into a command *)
val parse : Resp.t -> (parsed_t, [> error ]) result

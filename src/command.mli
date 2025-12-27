(* Redis command types and execution *)

type replconf_command =
  | ReplconfListeningPort of int
  | ReplconfCapa of string
  | ReplconfGetAck

type t =
  | Ping
  | Echo of Resp.t
  | Get of string
  | Set of string * Resp.t * int option
  | InfoReplication
  | Replconf of replconf_command
  | Psync of string * int
  | Wait of int * int

(* Parse a RESP value into a command *)
val parse : Resp.t -> t option

(* Redis command types and execution *)

type expiry =
  | ExpireMilliseconds of int
  | ExpireSeconds of int
  | NoExpiry

type t =
  | Ping
  | Echo of Resp.t
  | Get of Resp.t
  | Set of Resp.t * Resp.t * expiry
  | Info of Resp.t
  | Replconf of Resp.t list

(* Parse a RESP value into a command *)
val parse : Resp.t -> t option

(* Execute a command against storage and return response *)
val execute : t -> Storage.t -> Config.t -> Resp.t

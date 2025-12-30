(* Server configuration *)

type t = {
  port: int;
  dir: string option;
  dbfilename: string option;
  replicaof: (string * int) option;  (* (host, port) of master *)
}

(* Configuration errors as polymorphic variants *)
type error = [
  | `InvalidPort of int
  | `InvalidReplicaofFormat of string
  | `InvalidArgument of string
]

(* Default configuration *)
val default : t

(* Parse command-line arguments and return config or error *)
val parse_args : string array -> (t, [> error]) result

(* Parse args with fallback to default on error (logs error to stderr) *)
val parse_args_or_default : string array -> t

(* Check if running as replica *)
val is_replica : t -> bool

(* Error to string conversion *)
val error_to_string : [< error] -> string

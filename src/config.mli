(* Server configuration *)

type t = {
  port: int;
  dir: string option;
  dbfilename: string option;
}

(* Default configuration *)
val default : t

(* Parse command-line arguments and return config *)
val parse_args : string array -> t

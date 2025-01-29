type t

type replication_role =
  | Master of { replid : string; repl_offset : int }
  | Slave of { master_host : string; master_port : int }

val master : replication_role

val init :
  replication_role:replication_role ->
  dbfilename:string ->
  dir:string ->
  unit ->
  t

val get_metadata : t -> string -> string option

module Command : sig
  type t =
    | Ping
    | Echo of string
    | Get of { index : int; key : string; now : Int64.t }
    | Set of {
        index : int;
        key : string;
        value : Resp.t;
        duration : Int64.t option;
        now : Int64.t;
      }
    | Keys of { index : int; now : Int64.t }
    | Config_get_dir
    | Config_get_dbfilename
    | Info_replication
    | Repl_conf
    | Psync
    | Select of int
end

val handle_command : t -> Command.t -> Resp.t * string option
val to_string : t -> string

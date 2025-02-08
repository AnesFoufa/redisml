type t

type replication_role =
  | Master of { replid : string; repl_offset : int }
  | Slave of { port : int; master_host : string; master_port : int }

val master : replication_role

val init :
  replication_role:replication_role ->
  dbfilename:string ->
  dir:string ->
  unit ->
  t

val get_metadata : t -> string -> string option
val get_all_metadata : t -> (string * string) Seq.t

type client_id

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
    | Wait

  type command_error = NotConnected | NotHandeledBySlave
end

val handle_command :
  client_id -> t -> Command.t -> (Resp.t, Command.command_error) Result.t

val connect : t -> client_id * string Event.channel
val disconnect : client_id -> t -> unit
val to_string : t -> string
val of_string : string -> dbfilename:string -> dir:string -> (t, string) result

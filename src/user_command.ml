type read

type write

type _ t =
  | Ping : read t
  | Echo : Resp.t -> read t
  | Get : string -> read t
  | InfoReplication : read t
  | ConfigGet : Command.config_param -> read t
  | Keys : string -> read t
  | Set : Command.set_params -> write t

type parse_error =
  [ Command.error
  | `ReadOnlyReplica
  ]

let to_command : type a. a t -> Command.t = function
  | Ping -> Command.Ping
  | Echo msg -> Command.Echo msg
  | Get key -> Command.Get key
  | InfoReplication -> Command.InfoReplication
  | ConfigGet p -> Command.ConfigGet p
  | Keys pat -> Command.Keys pat
  | Set params -> Command.Set params

let of_command_for_user (cmd : Command.t) :
    [ `Read of read t | `Write of write t | `Disallowed ] =
  match cmd with
  | Command.Ping -> `Read Ping
  | Command.Echo msg -> `Read (Echo msg)
  | Command.Get key -> `Read (Get key)
  | Command.InfoReplication -> `Read InfoReplication
  | Command.ConfigGet p -> `Read (ConfigGet p)
  | Command.Keys pat -> `Read (Keys pat)
  | Command.Set params -> `Write (Set params)
  | Command.Wait _
  | Command.Psync _
  | Command.Replconf _ -> `Disallowed

type parsed_for_master = [ `Read of read t | `Write of write t | `Disallowed ]

let parse_for_master (resp : Resp.t) : (parsed_for_master, parse_error) result =
  match Command.parse resp with
  | Error e -> Error e
  | Ok cmd -> (
      match of_command_for_user cmd with
      | `Read r -> Ok (`Read r)
      | `Write w -> Ok (`Write w)
      | `Disallowed -> Ok `Disallowed)

let parse_for_replica (resp : Resp.t) : (read t, parse_error) result =
  match Command.parse resp with
  | Error e -> Error e
  | Ok cmd -> (
      match of_command_for_user cmd with
      | `Read r -> Ok r
      | `Write _w -> Error `ReadOnlyReplica
      | `Disallowed -> Error `ReadOnlyReplica)

type user_read
type user_write
type protocol_psync
type protocol_wait
type protocol_replconf

type _ t =
  | Read : User_command.read User_command.t -> user_read t
  | Write : User_command.write User_command.t -> user_write t
  | Psync : Command.psync_params -> protocol_psync t
  | Wait : Command.wait_params -> protocol_wait t
  | Replconf : Command.replconf_command -> protocol_replconf t

type packed = Packed : _ t -> packed

type parse_error = Command.error

let parse (resp : Resp.t) : (packed, parse_error) result =
  match Command.parse resp with
  | Error e -> Error e
  | Ok cmd -> (
      match cmd with
      | Command.Psync params -> Ok (Packed (Psync params))
      | Command.Wait params -> Ok (Packed (Wait params))
      | Command.Replconf replconf_cmd -> Ok (Packed (Replconf replconf_cmd))
      | Command.Ping -> Ok (Packed (Read User_command.Ping))
      | Command.Echo msg -> Ok (Packed (Read (User_command.Echo msg)))
      | Command.Get key -> Ok (Packed (Read (User_command.Get key)))
      | Command.InfoReplication -> Ok (Packed (Read User_command.InfoReplication))
      | Command.ConfigGet param -> Ok (Packed (Read (User_command.ConfigGet param)))
      | Command.Keys pattern -> Ok (Packed (Read (User_command.Keys pattern)))
      | Command.Set params -> Ok (Packed (Write (User_command.Set params))))

let as_read (Read cmd) = cmd
let as_write (Write cmd) = cmd

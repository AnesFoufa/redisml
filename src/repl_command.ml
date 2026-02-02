type t =
  | Apply_set of Command.set_params
  | Replconf_getack
  | Ignore

type parse_error =
  [ Command.error
  | `DisallowedFromMaster
  ]

let parse (resp : Resp.t) : (t, parse_error) result =
  match Command.parse resp with
  | Error e -> Error e
  | Ok cmd -> (
      match cmd with
      | Command.Set params -> Ok (Apply_set params)
      | Command.Replconf Command.ReplconfGetAck -> Ok Replconf_getack
      (* Masters sometimes send PING during handshake; treat as ignorable once
         we're in the streaming loop. *)
      | Command.Ping -> Ok Ignore
      | _ -> Error `DisallowedFromMaster)

let to_command = function
  | Apply_set params -> Some (Command.Set params)
  | Replconf_getack -> Some (Command.Replconf Command.ReplconfGetAck)
  | Ignore -> None

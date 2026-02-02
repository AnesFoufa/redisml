type response_required

type no_response

type _ t =
  | Apply_set : Command.set_params -> no_response t
  | Replconf_getack : response_required t
  | Ignore : no_response t

type packed = Packed : _ t -> packed

type parse_error =
  [ Command.error
  | `DisallowedFromMaster
  ]

let parse (resp : Resp.t) : (packed, parse_error) result =
  match Command.parse resp with
  | Error e -> Error e
  | Ok cmd -> (
      match cmd with
      | Command.Set params -> Ok (Packed (Apply_set params))
      | Command.Replconf Command.ReplconfGetAck -> Ok (Packed Replconf_getack)
      | Command.Ping -> Ok (Packed Ignore)
      | _ -> Error `DisallowedFromMaster)

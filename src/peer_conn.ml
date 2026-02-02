type user

type upstream_master

type 'peer t = {
  ic : Lwt_io.input_channel;
  oc : Lwt_io.output_channel;
  addr : string option;
}

let user ~ic ~oc ~addr = { ic; oc; addr = Some addr }
let upstream_master ~ic ~oc = { ic; oc; addr = None }

let ic t = t.ic
let oc t = t.oc

let addr t =
  match t.addr with
  | Some a -> a
  | None -> invalid_arg "Peer_conn.addr called on non-user conn"

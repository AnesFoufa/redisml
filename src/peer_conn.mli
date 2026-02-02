(** Peer_conn

    Typed wrapper around Lwt IO channels that tags the *peer kind*.

    This prevents accidentally wiring user connections into replication logic,
    and vice-versa, at compile time.
*)

type user

type upstream_master

type 'peer t

val user : ic:Lwt_io.input_channel -> oc:Lwt_io.output_channel -> addr:string -> user t
val upstream_master : ic:Lwt_io.input_channel -> oc:Lwt_io.output_channel -> upstream_master t

val ic : 'peer t -> Lwt_io.input_channel
val oc : 'peer t -> Lwt_io.output_channel
val addr : user t -> string

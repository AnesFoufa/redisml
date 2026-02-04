(* MasterConnection - Slave-side logic for connecting to master *)

(* Connect to master server, perform handshake, and start processing commands *)
val connect_to_master :
  database:Database.replica Database.t ->
  host:string ->
  master_port:int ->
  replica_port:int ->
  unit Lwt.t

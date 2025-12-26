(* MasterConnection - Slave-side logic for connecting to master *)

(* Connect to master server and perform handshake (for replica mode) *)
val connect_to_master :
  host:string ->
  master_port:int ->
  replica_port:int ->
  unit Lwt.t

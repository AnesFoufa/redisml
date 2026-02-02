type master

type replica

type initial

type ready

type replicating

type closed

type incomplete

type complete

type _ role_state =
  | Master_state : { replicas : Replica_manager.t } -> master role_state
  | Replica_state : { mutable offset : int } -> replica role_state

type 'r database = {
  storage : Storage.t;
  config : Config.t;
  role : 'r role_state;
}

type any_database =
  | AnyMaster : master database -> any_database
  | AnyReplica : replica database -> any_database

let make_master ~storage ~config : master database =
  { storage; config; role = Master_state { replicas = Replica_manager.create () } }

let make_replica ~storage ~config : replica database =
  { storage; config; role = Replica_state { offset = 0 } }

let any_of_master (db : master database) : any_database = AnyMaster db
let any_of_replica (db : replica database) : any_database = AnyReplica db

let fold_any (t : any_database) ~(master : master database -> 'a)
    ~(replica : replica database -> 'a) : 'a =
  match t with
  | AnyMaster db -> master db
  | AnyReplica db -> replica db

let storage (db : 'r database) = db.storage
let config (db : 'r database) = db.config

let is_master (type r) (db : r database) : bool =
  match db.role with
  | Master_state _ -> true
  | Replica_state _ -> false

let replicas (db : master database) =
  let (Master_state { replicas }) = db.role in
  replicas

let replica_offset (db : replica database) =
  let (Replica_state { offset }) = db.role in
  offset

let set_replica_offset (db : replica database) v =
  let (Replica_state state) = db.role in
  state.offset <- v

type (_, _) conn =
  | Conn_initial : {
      ic : Lwt_io.input_channel;
      oc : Lwt_io.output_channel;
      addr : string;
    }
      -> ('r, initial) conn
  | Conn_ready : {
      ic : Lwt_io.input_channel;
      oc : Lwt_io.output_channel;
      addr : string;
      buffer : Buffer_reader.t;
    }
      -> ('r, ready) conn
  | Conn_replicating : {
      ic : Lwt_io.input_channel;
      oc : Lwt_io.output_channel;
      buffer : Buffer_reader.t;
    }
      -> (replica, replicating) conn
  | Conn_closed : ('r, closed) conn

type hs_start

type hs_ping_sent

type hs_port_sent

type hs_capa_sent

type hs_psync_sent

type hs_complete

type _ handshake =
  | Hs_start : {
      ic : Lwt_io.input_channel;
      oc : Lwt_io.output_channel;
      replica_port : int;
    }
      -> hs_start handshake
  | Hs_ping_sent : {
      ic : Lwt_io.input_channel;
      oc : Lwt_io.output_channel;
      replica_port : int;
    }
      -> hs_ping_sent handshake
  | Hs_port_sent : { ic : Lwt_io.input_channel; oc : Lwt_io.output_channel }
      -> hs_port_sent handshake
  | Hs_capa_sent : { ic : Lwt_io.input_channel; oc : Lwt_io.output_channel }
      -> hs_capa_sent handshake
  | Hs_psync_sent : { ic : Lwt_io.input_channel; oc : Lwt_io.output_channel }
      -> hs_psync_sent handshake
  | Hs_complete : {
      ic : Lwt_io.input_channel;
      oc : Lwt_io.output_channel;
      repl_id : string;
      repl_offset : int;
    }
      -> hs_complete handshake

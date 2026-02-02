(** Typed_state

    Foundations for making illegal states unrepresentable.

    This module is intentionally "foundational": it introduces phantom types and
    GADTs but does not yet change runtime behaviour.
*)

(** Role phantom types *)
type master

type replica

(** Connection lifecycle phantom types *)
type initial

type ready

type replicating

type closed

(** Parse state phantom types (for incremental RESP parsing) *)
type incomplete

type complete

(** Role-indexed state carried by the database. *)
type _ role_state =
  | Master_state : { replicas : Replica_manager.t } -> master role_state
  | Replica_state : { mutable offset : int } -> replica role_state

(** Role-indexed database wrapper.

    Note: this will progressively replace [Database.t]. For now we keep the same
    underlying data sources (Storage/Config/Replica_manager).
*)
type 'r database = {
  storage : Storage.t;
  config : Config.t;
  role : 'r role_state;
}

(** Existential wrapper for runtime role selection (Config decides role). *)
type any_database = AnyDb : 'r database -> any_database

(** A connection value, indexed by role and lifecycle state. *)
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

(** Handshake stage phantom types (replica <-> master). *)
type hs_start

type hs_ping_sent

type hs_port_sent

type hs_capa_sent

type hs_psync_sent

type hs_complete

(** Handshake state machine (staged) *)
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

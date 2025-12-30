(** Incremental buffer reader for RESP protocol parsing *)

(** Buffer reader type that accumulates data and supports incremental parsing *)
type t

(** Create a new buffer reader
    @param max_size Optional maximum buffer size (for DOS protection)
    @param initial_capacity Initial buffer capacity (default 4096)
*)
val create : ?max_size:int option -> ?initial_capacity:int -> unit -> t

(** Get current buffer size in bytes *)
val size : t -> int

(** Check if adding data of given length would exceed max size *)
val would_exceed_max : t -> int -> bool

(** Add data to buffer
    @return Ok () if successful, Error `BufferOverflow if max size exceeded
*)
val add_data : t -> string -> (unit, [> `BufferOverflow]) result

(** Get current buffer contents *)
val contents : t -> string

(** Reset buffer and optionally add remaining data *)
val reset : t -> string option -> unit

(** Try to parse next value from buffer using provided parser
    @param parser Function that parses string into (value, remaining) option
    @return Some value if parse succeeds, None if incomplete
*)
val parse_next : t -> parser:(string -> ('a * string) option) -> 'a option

(** Read data from channel and add to buffer
    @return `Ok if data read successfully, `Eof if channel closed, `BufferOverflow if max size exceeded
*)
val read_from_channel : t -> Lwt_io.input_channel -> count:int -> [`Ok | `Eof | `BufferOverflow] Lwt.t

(** Read from channel until a complete value can be parsed
    @param parser Function that parses string into (value, remaining) option
    @return Ok value if successfully parsed, Error `Eof if channel closed, Error `BufferOverflow if buffer limit exceeded
*)
val read_until_parsed :
  t ->
  Lwt_io.input_channel ->
  count:int ->
  parser:(string -> ('a * string) option) ->
  ('a, [> `Eof | `BufferOverflow]) result Lwt.t

(** Process all values currently available in buffer
    @param parser Function that parses string into (value, remaining) option
    @param f Processing function called for each parsed value
*)
val process_all_buffered :
  t ->
  parser:(string -> ('a * string) option) ->
  f:('a -> unit Lwt.t) ->
  unit Lwt.t

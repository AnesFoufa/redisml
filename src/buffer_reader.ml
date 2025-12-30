open Lwt.Syntax

(* Buffer reader for incremental RESP parsing *)

type t = {
  buffer : Buffer.t;
  max_size : int option;
}

(* Create a new buffer reader *)
let create ?(max_size = None) ?(initial_capacity = 4096) () =
  { buffer = Buffer.create initial_capacity; max_size }

(* Get current buffer size *)
let size t = Buffer.length t.buffer

(* Check if adding data would exceed max size *)
let would_exceed_max t data_len =
  match t.max_size with
  | None -> false
  | Some max -> size t + data_len > max

(* Add data to buffer - returns Error if max size exceeded *)
let add_data t data =
  if would_exceed_max t (String.length data) then
    Error `BufferOverflow
  else (
    Buffer.add_string t.buffer data;
    Ok ()
  )

(* Get buffer contents *)
let contents t = Buffer.contents t.buffer

(* Clear buffer and optionally add remaining data *)
let reset t remaining =
  Buffer.clear t.buffer;
  match remaining with
  | Some s -> Buffer.add_string t.buffer s
  | None -> ()

(* Try to parse next value using provided parser *)
let parse_next t ~parser =
  let contents = Buffer.contents t.buffer in
  match parser contents with
  | Some (value, rest) ->
      (* Successfully parsed - update buffer with remaining data *)
      Buffer.clear t.buffer;
      Buffer.add_string t.buffer rest;
      Some value
  | None ->
      (* Incomplete data - wait for more *)
      None

(* Read data from channel and add to buffer *)
let read_from_channel t ic ~count =
  let* data = Lwt_io.read ~count ic in
  if String.length data = 0 then
    Lwt.return `Eof
  else
    match add_data t data with
    | Ok () -> Lwt.return `Ok
    | Error `BufferOverflow -> Lwt.return `BufferOverflow

(* Read and parse until one complete value is available *)
let read_until_parsed t ic ~count ~parser =
  let rec loop () =
    match parse_next t ~parser with
    | Some value -> Lwt.return (Ok value)
    | None ->
        let* result = read_from_channel t ic ~count in
        match result with
        | `Ok -> loop ()
        | `Eof -> Lwt.return (Error `Eof)
        | `BufferOverflow -> Lwt.return (Error `BufferOverflow)
  in
  loop ()

(* Process all available parsed values from buffer *)
let rec process_all_buffered t ~parser ~f =
  match parse_next t ~parser with
  | Some value ->
      let* () = f value in
      process_all_buffered t ~parser ~f
  | None ->
      Lwt.return_unit

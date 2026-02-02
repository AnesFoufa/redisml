open Lwt.Syntax

open Typed_state

(* Connect to master and perform handshake *)
let connect_to_master ~database ~host ~master_port ~replica_port =
  let database_t : Database.t = AnyDb database in
  let* () = Lwt_io.eprintlf "Connecting to master at %s:%d" host master_port in

  (* Resolve hostname to IP address *)
  let inet_addr =
    try
      Unix.inet_addr_of_string host
    with Failure _ ->
      let host_entry = Unix.gethostbyname host in
      host_entry.Unix.h_addr_list.(0)
  in

  let master_addr = Unix.ADDR_INET (inet_addr, master_port) in
  let* (ic, oc) = Lwt_io.open_connection master_addr in

  (* Step 1: Send PING *)
  let ping_cmd = Resp.Array [Resp.BulkString "PING"] in
  let* () = Lwt_io.write oc (Resp.serialize ping_cmd) in
  let* () = Lwt_io.flush oc in

  (* Read PONG response *)
  let* _pong = Lwt_io.read ~count:1024 ic in
  let* () = Lwt_io.eprintlf "Received PONG from master" in

  (* Step 2: Send REPLCONF listening-port *)
  let replconf_port_cmd = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "listening-port";
    Resp.BulkString (string_of_int replica_port)
  ] in
  let* () = Lwt_io.write oc (Resp.serialize replconf_port_cmd) in
  let* () = Lwt_io.flush oc in

  (* Read OK response *)
  let* _ok1 = Lwt_io.read ~count:1024 ic in
  let* () = Lwt_io.eprintlf "Received OK from REPLCONF listening-port" in

  (* Step 3: Send REPLCONF capa psync2 *)
  let replconf_capa_cmd = Resp.Array [
    Resp.BulkString "REPLCONF";
    Resp.BulkString "capa";
    Resp.BulkString "psync2"
  ] in
  let* () = Lwt_io.write oc (Resp.serialize replconf_capa_cmd) in
  let* () = Lwt_io.flush oc in

  (* Read OK response *)
  let* _ok2 = Lwt_io.read ~count:1024 ic in
  let* () = Lwt_io.eprintlf "Received OK from REPLCONF capa" in

  (* Step 4: Send PSYNC ? -1 (request full sync) *)
  let psync_cmd = Resp.Array [
    Resp.BulkString "PSYNC";
    Resp.BulkString "?";
    Resp.BulkString "-1"
  ] in
  let* () = Lwt_io.write oc (Resp.serialize psync_cmd) in
  let* () = Lwt_io.flush oc in

  (* Helper: Read and parse FULLRESYNC response *)
  let read_fullresync_response reader ic =
    let* result = Buffer_reader.read_until_parsed reader ic ~count:4096 ~parser:Resp.parse in
    match result with
    | Ok _resp ->
        Lwt_io.eprintlf "Received FULLRESYNC from master"
    | Error `Eof ->
        Lwt.fail_with "Master connection closed before FULLRESYNC"
    | Error `BufferOverflow ->
        Lwt.fail_with "FULLRESYNC response too large"
  in

  (* Helper: Read RDB file from buffer *)
  let read_rdb_file ic buffer =
    let rec read_header () =
      let contents = Buffer.contents buffer in
      if String.length contents > 0 && contents.[0] = '$' then (
        try
          let newline_pos = String.index contents '\n' in
          let length_str = String.sub contents 1 (newline_pos - 2) in
          let rdb_length = int_of_string length_str in
          let remaining = String.sub contents (newline_pos + 1) (String.length contents - newline_pos - 1) in
          Buffer.clear buffer;
          Buffer.add_string buffer remaining;
          Lwt.return rdb_length
        with Not_found ->
          let* data = Lwt_io.read ~count:4096 ic in
          Buffer.add_string buffer data;
          read_header ()
      ) else (
        let* data = Lwt_io.read ~count:4096 ic in
        Buffer.add_string buffer data;
        read_header ()
      )
    in
    let rec read_data needed =
      let contents = Buffer.contents buffer in
      if String.length contents >= needed then (
        let remaining = String.sub contents needed (String.length contents - needed) in
        Buffer.clear buffer;
        Buffer.add_string buffer remaining;
        let* () = Lwt_io.eprintlf "Received RDB file (%d bytes)" needed in
        Lwt.return ()
      ) else (
        let* data = Lwt_io.read ~count:4096 ic in
        Buffer.add_string buffer data;
        read_data needed
      )
    in
    let* rdb_length = read_header () in
    read_data rdb_length
  in

  (* Helper: Process commands already in buffer *)
  let process_buffered_commands reader ic oc =
    Buffer_reader.process_all_buffered reader ~parser:Resp.parse ~f:(fun cmd ->
      match Command.parse cmd with
      | Ok parsed_cmd ->
          let current_time = Unix.gettimeofday () in
          let* response_opt =
            Database.handle_command database_t parsed_cmd
              ~current_time
              ~original_resp:cmd
              ~ic
              ~oc
              ~address:"master"
          in
          let cmd_bytes = String.length (Resp.serialize cmd) in
          Database.increment_offset database cmd_bytes;

          (match parsed_cmd with
           | Command.Replconf _ ->
               (match response_opt with
                | Some resp ->
                    let* () = Lwt_io.write oc (Resp.serialize resp) in
                    Lwt_io.flush oc
                | None -> Lwt.return_unit)
           | _ -> Lwt.return_unit)
      | Error _ -> Lwt.return_unit
    )
  in

  (* Spawn background task to handle replication stream *)
  let _ = Lwt.async (fun () ->
    let reader = Buffer_reader.create () in
    let buffer = Buffer.create 4096 in  (* For RDB reading which has custom format *)

    (* Read FULLRESYNC response *)
    let* () = read_fullresync_response reader ic in

    (* Read RDB file - copy data from reader to buffer for custom RDB parsing *)
    let () =
      let contents = Buffer_reader.contents reader in
      Buffer.add_string buffer contents;
      Buffer_reader.reset reader None
    in
    let* () = read_rdb_file ic buffer in

    (* Copy any remaining data from RDB buffer back to reader *)
    let () =
      let remaining = Buffer.contents buffer in
      Buffer.clear buffer;
      Buffer_reader.reset reader (Some remaining)
    in

    (* Continuously read and process commands from master *)
    let rec replication_loop () =
      Lwt.catch
        (fun () ->
          (* Process any commands already in buffer *)
          let* () = process_buffered_commands reader ic oc in

          (* Read more data from master *)
          let* result = Buffer_reader.read_from_channel reader ic ~count:4096 in
          match result with
          | `Eof ->
              Lwt_io.eprintlf "Master connection closed"
          | `BufferOverflow ->
              Lwt_io.eprintlf "Master sent too much data"
          | `Ok ->
              replication_loop ()
        )
        (fun exn ->
          Lwt_io.eprintlf "Error processing master commands: %s" (Printexc.to_string exn))
    in
    replication_loop ()
  ) in

  Lwt.return_unit

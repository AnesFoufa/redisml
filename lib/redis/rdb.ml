open Angstrom

type metadata = (string, string) Hashtbl.t
type databases = (int, Database.t) Hashtbl.t

let header = string "REDIS0011"
let first_two_bits c = (Char.code c lsr 6) land 0b11

type size =
  | Size of int
  | BytesOneIntegerLE
  | BytesTwoIntegerLE
  | BytesFourIntegerLE

let (rdb_size : size Angstrom.t) =
  let* first_char = any_char in
  let remaining_six_bits = Char.code first_char land 0b00111111 in
  match first_two_bits first_char with
  | 0 -> return (Size remaining_six_bits)
  | 1 ->
      let* second_char = any_char in
      return (Size ((remaining_six_bits lsl 8) lor Char.code second_char))
  | 2 ->
      let* next_four = take 4 in
      let buf = Bytes.of_string next_four in
      return (Size (Bytes.get_int32_be buf 0 |> Int32.to_int))
  | 3 -> (
      match Char.code first_char with
      | 0xC0 -> return BytesOneIntegerLE
      | 0xC1 -> return BytesTwoIntegerLE
      | 0xC2 -> return BytesFourIntegerLE
      | i -> failwith (Printf.sprintf "%s %d" "Not implemented " i))
  | i -> failwith (Printf.sprintf "%s %d" "Not implemented (2) " i)

let (rdb_string : String.t Angstrom.t) =
  let* size = rdb_size in
  match size with
  | Size i -> take i
  | BytesOneIntegerLE ->
      let* next_chars = take 1 in
      let buf = Bytes.of_string next_chars in
      Bytes.get_int8 buf 0 |> string_of_int |> return
  | BytesTwoIntegerLE ->
      let* next_chars = take 2 in
      let buf = Bytes.of_string next_chars in
      Bytes.get_int16_le buf 0 |> string_of_int |> return
  | BytesFourIntegerLE ->
      let* next_chars = take 4 in
      let buf = Bytes.of_string next_chars in
      Bytes.get_int32_le buf 0 |> Int32.to_int |> string_of_int |> return

let metadata_subsection =
  let* _ = char '\250' in
  let* key = rdb_string in
  let* value = rdb_string in
  return (key, value)

let metadata_section =
  let* subsections = many metadata_subsection in
  subsections |> List.to_seq |> Hashtbl.of_seq |> return

let milliseconds_expire =
  let* _milliseconds_indicator = char '\252' in
  let* expire_timestamp_little_endian = take 8 in
  let buffer = Bytes.of_string expire_timestamp_little_endian in
  let timestamp = Bytes.get_int64_le buffer 0 in
  return timestamp

let seconds_expire =
  let* _seconds_indicator = char '\xFD' in
  let* expire_timestamp_little_endian = take 4 in
  let buffer = Bytes.of_string expire_timestamp_little_endian in
  let timestamp =
    Bytes.get_int32_le buffer 0
    |> Int64.of_int32
    |> Int64.mul (Int64.of_int 1000)
  in
  return timestamp

let (expire : Int64.t Angstrom.t) = milliseconds_expire <|> seconds_expire

let key_and_record =
  let* expire_option = expire >>| Option.some <|> return None in
  let* _value_type = Char.chr 0 |> char in
  let* key = rdb_string in
  let* value = rdb_string in
  let open Database in
  return (key, { value = Resp.RBulkString value; expire = expire_option })

let database_subsction =
  let* _start_indicator = char '\xFE' in
  let* index = rdb_size in
  match index with
  | Size i ->
      let* _table_size_indicator = char '\xFB' in
      let* _database_size = rdb_size in
      let* _nb_expiring_keys = rdb_size in
      let* keys_and_records = many key_and_record in
      return (i, keys_and_records |> List.to_seq |> Hashtbl.of_seq)
  | _ -> failwith "Not implemented"

let end_of_file_section =
  let* _end_of_file_indicator = char '\255' in
  let* _checksum = take 8 in
  return ()

let rdb =
  let* _ = header in
  let* metatadata = metadata_section in
  let* databases_list = many database_subsction in
  let* _ = end_of_file_section in
  let databases = databases_list |> List.to_seq |> Hashtbl.of_seq in
  return (metatadata, databases)

let of_string = parse_string ~consume:Consume.Prefix rdb

let size_encode n =
  if n < 0 then invalid_arg "Number must be non-negative"
  else if n < 1 lsl 6 then String.make 1 (Char.chr n)
  else if n < 1 lsl 14 then
    let b1 = (n lsr 8) lor 0b11000000 in
    let b2 = n land 0xFF in
    String.init 2 (fun i -> Char.chr (if i = 0 then b1 else b2))
  else if n < 1 lsl 32 then (
    let bytes = Bytes.create 5 in
    Bytes.set bytes 0 (Char.chr 0b11111110);
    Bytes.set_int32_be bytes 1 (Int32.of_int n);
    Bytes.to_string bytes)
  else
    let bytes = Bytes.create 9 in
    Bytes.set bytes 0 (Char.chr 0b11111111);
    Bytes.set_int64_be bytes 1 (Int64.of_int n);
    Bytes.to_string bytes

(* Encode an integer in Redis RDB integer format *)
let encode_redis_integer value =
  let bytes =
    if value >= -128 && value <= 127 then (
      let buf = Bytes.create 2 in
      Bytes.set buf 0 (Char.chr (0xC0 lor 0));
      Bytes.set buf 1 (Char.chr (value land 0xFF));
      buf)
    else if value >= -32768 && value <= 32767 then (
      let buf = Bytes.create 3 in
      Bytes.set buf 0 (Char.chr (0xC0 lor 1));
      Bytes.set_int16_le buf 1 value;
      buf)
    else
      let buf = Bytes.create 5 in
      Bytes.set buf 0 (Char.chr (0xC0 lor 2));
      Bytes.set_int32_le buf 1 (Int32.of_int value);
      buf
  in
  Bytes.to_string bytes

let string_encode str =
  match int_of_string_opt str with
  | Some i -> encode_redis_integer i
  | None ->
      let length = String.length str |> size_encode in
      length ^ str

let metadata_subsection_to_string metadata =
  Hashtbl.to_seq metadata
  |> Seq.map (fun (key, value) ->
         Printf.sprintf "\xFA%s%s" (string_encode key) (string_encode value))
  |> List.of_seq |> String.concat ""

let encode_value = function
  | Resp.RBulkString s -> string_encode s
  | Resp.RString s -> string_encode s
  | _ -> failwith "To do (encode value)"

let key_value_to_string =
  let open Database in
  function
  | key, { value; expire = None } ->
      Printf.sprintf "\000%s%s" (string_encode key) (encode_value value)
  | key, { value; expire = Some duration }
    when Int64.rem duration (Int64.of_int 1000) |> Int64.equal (Int64.of_int 0)
    ->
      let duration_in_seconds =
        duration |> Int64.div (Int64.of_int 1000) |> Int64.to_int32
      in
      let buffer = Bytes.create 4 in
      let () = Bytes.set_int32_le buffer 0 duration_in_seconds in
      Printf.sprintf "\xFD%s\000%s%s" (Bytes.to_string buffer)
        (string_encode key) (encode_value value)
  | key, { value; expire = Some duration } ->
      let buffer = Bytes.create 8 in
      let () = Bytes.set_int64_le buffer 0 duration in
      Printf.sprintf "\xFC%s\000%s%s" (Bytes.to_string buffer)
        (string_encode key) (encode_value value)

let database_subsection_to_string db =
  let open Database in
  let hashtable_size = Hashtbl.length db in
  let expiry_hashtable_size =
    Hashtbl.to_seq_values db
    |> Seq.filter (fun { expire; _ } -> Option.is_some expire)
    |> Seq.length
  in
  let resizdb_field =
    Printf.sprintf "\xFB%s%s"
      (size_encode hashtable_size)
      (size_encode expiry_hashtable_size)
  in
  let keys_and_values =
    Hashtbl.to_seq db
    |> Seq.map key_value_to_string
    |> List.of_seq |> String.concat ""
  in
  String.cat resizdb_field keys_and_values

let databases_subsection_to_string databases =
  Hashtbl.to_seq databases
  |> Seq.map (fun (key, value) ->
         Printf.sprintf "\xFE%s%s" (size_encode key)
           (database_subsection_to_string value))
  |> List.of_seq |> String.concat ""

module CRC64 = struct
  let crc64_polynomial = 0x42F0E1EBA9EA3693L

  (* Precompute the CRC-64 table *)
  let crc64_table =
    let table = Array.make 256 0L in
    for i = 0 to 255 do
      let compute_crc value =
        let value = ref value in
        for _ = 0 to 7 do
          if Int64.logand !value 1L <> 0L then
            value :=
              Int64.logxor (Int64.shift_right_logical !value 1) crc64_polynomial
          else value := Int64.shift_right_logical !value 1
        done;
        !value
      in
      table.(i) <- compute_crc (Int64.of_int i)
    done;
    table

  (* Function to compute CRC-64 for a string *)
  let crc64_of_string s =
    let crc = ref 0xFFFFFFFFFFFFFFFFL in
    String.iter
      (fun c ->
        let byte = Char.code c in
        let idx =
          Int64.to_int (Int64.logxor !crc (Int64.of_int byte)) land 0xFF
        in
        crc := Int64.logxor (Int64.shift_right_logical !crc 8) crc64_table.(idx))
      s;
    !crc

  (* Convert the CRC-64 value to an 8-byte string *)
  let crc64_to_8byte_string s =
    let crc64 = crc64_of_string s in
    String.init 8 (fun i ->
        Char.chr
          (Int64.to_int (Int64.shift_right_logical crc64 (8 * (7 - i)))
          land 0xFF))
end

let to_string metadata databases =
  let header = "REDIS0011" in
  let metadata_subsection_str = metadata_subsection_to_string metadata in
  let databases_subsection_str = databases_subsection_to_string databases in
  let end_of_file_subsection = "" in
  let file =
    Printf.sprintf "%s%s%s%s\xFF" header metadata_subsection_str
      databases_subsection_str end_of_file_subsection
  in
  let checksum = CRC64.crc64_to_8byte_string file in
  let res = file ^ checksum in
  res

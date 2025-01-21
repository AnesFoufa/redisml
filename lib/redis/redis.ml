[@@@warning "-27-69-32"]

type config = { dir : string; dbfilename : string }
type record = { value : Resp.t; expire : Int64.t option }
type database = (string, record) Hashtbl.t
type databases = (int, database) Hashtbl.t

type t = {
  metadata : (string, string) Hashtbl.t;
  databases : databases;
  config : config;
}

module RDB = struct
  open Angstrom

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
    let* milliseconds_indicator = char '\252' in
    let* expire_timestamp_little_endian = take 8 in
    let buffer = Bytes.of_string expire_timestamp_little_endian in
    let timestamp = Bytes.get_int64_le buffer 0 in
    return timestamp

  let seconds_expire =
    let* seconds_indicator = char '\xFD' in
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
    return (key, { value = Resp.RBulkString value; expire = expire_option })

  let database_subsction =
    let* _start_indicator = char '\254' in
    let* index = rdb_size in
    match index with
    | Size i ->
        let* _table_size_indicator = char '\251' in
        let* database_size = rdb_size in
        let* nb_expiring_keys = rdb_size in
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
end

exception UnparsedRDB

let init ~dbfilename ~dir =
  let file_path = Filename.concat dir dbfilename in
  let config = { dir; dbfilename } in
  try
    let ic = open_in file_path in
    let buf = Bytes.create 2048 in
    let nb_read = input ic buf 0 2048 in
    let content = Bytes.sub_string buf 0 nb_read in
    match RDB.of_string content with
    | Ok (metadata, databases) -> { metadata; databases; config }
    | Error err -> raise UnparsedRDB
  with Sys_error _ | UnparsedRDB ->
    let database = Hashtbl.create 256 in
    let databases = Hashtbl.create 16 in
    Hashtbl.add databases 0 database;
    let metadata = Hashtbl.create 32 in
    { metadata; databases; config }

let get_database databases i = Hashtbl.find databases.databases i
let get_dir databases = databases.config.dir
let get_dbfilename databases = databases.config.dbfilename

let set_database databases i =
  if Hashtbl.mem databases.databases i then ()
  else Hashtbl.add databases.databases i (Hashtbl.create 256)

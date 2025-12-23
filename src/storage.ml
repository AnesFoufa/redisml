(* In-memory key-value storage *)

type t = {
  data : (string, string) Hashtbl.t;
}

let create () = {
  data = Hashtbl.create 1024;
}

let set storage key value =
  Hashtbl.replace storage.data key value

let get storage key =
  Hashtbl.find_opt storage.data key

let delete storage key =
  Hashtbl.remove storage.data key

let keys storage =
  Hashtbl.to_seq_keys storage.data
  |> List.of_seq

(* In-memory key-value storage with expiry support *)

type item = {
  value : Resp.t;
  expires_at : float option; (* Unix timestamp in seconds *)
}

type t = {
  data : (string, item) Hashtbl.t;
}

let create () = {
  data = Hashtbl.create Constants.initial_storage_capacity;
}

let set storage key value expires_at =
  let item = { value; expires_at } in
  Hashtbl.replace storage.data key item

let is_expired item =
  match item.expires_at with
  | None -> false
  | Some expires_at -> Unix.gettimeofday () > expires_at

let get storage key =
  match Hashtbl.find_opt storage.data key with
  | None -> None
  | Some item ->
      if is_expired item then (
        (* Remove expired key *)
        Hashtbl.remove storage.data key;
        None
      ) else
        Some item.value

let delete storage key =
  Hashtbl.remove storage.data key

let keys storage =
  (* Filter out expired keys *)
  Hashtbl.to_seq storage.data
  |> Seq.filter (fun (_key, item) -> not (is_expired item))
  |> Seq.map fst
  |> List.of_seq

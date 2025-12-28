(* In-memory key-value storage with expiry support *)

type item = {
  value : Resp.t;
  expires_at : float option; (* Unix timestamp in seconds *)
}

type t = {
  data : (string, item) Hashtbl.t;
  get_time : unit -> float;  (* Injectable time source for testing *)
}

let create ?(capacity=Constants.initial_storage_capacity) ?(get_time=Unix.gettimeofday) () = {
  data = Hashtbl.create capacity;
  get_time;
}

let set storage key value expires_at =
  let item = { value; expires_at } in
  Hashtbl.replace storage.data key item

let is_expired storage item =
  match item.expires_at with
  | None -> false
  | Some expires_at -> storage.get_time () > expires_at

let get storage key =
  match Hashtbl.find_opt storage.data key with
  | None -> None
  | Some item ->
      if is_expired storage item then (
        (* Remove expired key *)
        Hashtbl.remove storage.data key;
        None
      ) else
        Some item.value

let delete storage key =
  Hashtbl.remove storage.data key

let keys storage =
  (* Filter out expired keys - use fold to avoid intermediate allocations *)
  Hashtbl.fold (fun key item acc ->
    if is_expired storage item then acc
    else key :: acc
  ) storage.data []

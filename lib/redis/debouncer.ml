type 'a t = {
  mutable last_op_data : (Int64.t * 'a) Option.t;
  op : unit -> 'a;
  duration : Int64.t;
}

let init ~op ?(miliseconds = 0) ?(seconds = 0) ?(minutes = 0) () =
  let minutes = Int64.of_int minutes in
  let seconds = Int64.of_int seconds in
  let miliseconds = Int64.of_int miliseconds in
  let duration =
    Int64.add
      (Int64.mul minutes (Int64.of_int 60000))
      (Int64.add (Int64.mul seconds (Int64.of_int 1000)) miliseconds)
  in
  { last_op_data = None; op; duration }

let debounce self =
  let now = Unix.gettimeofday () *. 1000. |> Int64.of_float in
  match self.last_op_data with
  | Some (ts, res) when Int64.compare (Int64.add ts self.duration) now >= 0 ->
      res
  | _ ->
      let res = self.op () in
      self.last_op_data <- Some (now, res);
      res

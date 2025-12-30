(* Result monad syntax for composable error handling *)

(** Basic Result monad operators *)

let ( let* ) = Result.bind
(** Monadic bind for Result. Use with: [let* x = result in ...]
    Chains computations that may fail, short-circuiting on first error. *)

let ( let+ ) x f = Result.map f x
(** Monadic map for Result. Use with: [let+ x = result in ...]
    Transforms the success value without changing error channel. *)

(** Utility functions *)

let lift f x =
  try Ok (f x)
  with e -> Error (`Exception (Printexc.to_string e))
(** Lift a potentially-throwing function into Result context.
    Catches exceptions and converts them to [`Exception err]. *)

let map_error f = function
  | Ok x -> Ok x
  | Error e -> Error (f e)
(** Transform the error type of a Result. Useful for composing
    errors from different modules. *)

(** Lwt + Result integration *)

module Lwt_syntax = struct
  let ( let*! ) = Lwt.bind
  (** Lwt bind - for pure Lwt operations without Result. *)

  let ( let* ) = Lwt_result.bind
  (** Lwt_result bind - chains async operations that may fail. *)

  let ( let+ ) x f = Lwt_result.map f x
  (** Lwt_result map - transforms success value in async context. *)

  let return_ok x = Lwt.return (Ok x)
  (** Lift a value into Lwt+Result success. Equivalent to [Lwt.return (Ok x)]. *)

  let return_error e = Lwt.return (Error e)
  (** Lift an error into Lwt+Result failure. Equivalent to [Lwt.return (Error e)]. *)

  let lift_result = Lwt.return
  (** Lift a Result into Lwt context. Equivalent to [Lwt.return result]. *)
end

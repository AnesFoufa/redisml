(* Result monad syntax for composable error handling *)

(** Basic Result monad operators *)

val ( let* ) : ('a, 'e) result -> ('a -> ('b, 'e) result) -> ('b, 'e) result
(** Monadic bind for Result. Use with: [let* x = result in ...]
    Chains computations that may fail, short-circuiting on first error. *)

val ( let+ ) : ('a, 'e) result -> ('a -> 'b) -> ('b, 'e) result
(** Monadic map for Result. Use with: [let+ x = result in ...]
    Transforms the success value without changing error channel. *)

(** Utility functions *)

val lift : ('a -> 'b) -> 'a -> ('b, [> `Exception of string]) result
(** Lift a potentially-throwing function into Result context.
    Catches exceptions and converts them to [`Exception err]. *)

val map_error : ('e1 -> 'e2) -> ('a, 'e1) result -> ('a, 'e2) result
(** Transform the error type of a Result. Useful for composing
    errors from different modules. *)

(** Lwt + Result integration *)

module Lwt_syntax : sig
  val ( let*! ) : 'a Lwt.t -> ('a -> 'b Lwt.t) -> 'b Lwt.t
  (** Lwt bind - for pure Lwt operations without Result. *)

  val ( let* ) : ('a, 'e) result Lwt.t -> ('a -> ('b, 'e) result Lwt.t) -> ('b, 'e) result Lwt.t
  (** Lwt_result bind - chains async operations that may fail. *)

  val ( let+ ) : ('a, 'e) result Lwt.t -> ('a -> 'b) -> ('b, 'e) result Lwt.t
  (** Lwt_result map - transforms success value in async context. *)

  val return_ok : 'a -> ('a, 'e) result Lwt.t
  (** Lift a value into Lwt+Result success. Equivalent to [Lwt.return (Ok x)]. *)

  val return_error : 'e -> ('a, 'e) result Lwt.t
  (** Lift an error into Lwt+Result failure. Equivalent to [Lwt.return (Error e)]. *)

  val lift_result : ('a, 'e) result -> ('a, 'e) result Lwt.t
  (** Lift a Result into Lwt context. Equivalent to [Lwt.return result]. *)
end

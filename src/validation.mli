(** Validation framework for command parameters

    This module provides composable validators for command parameter validation.
    Validators are functions that take a value and return Result types.
*)

(** A validator is a function that validates a value of type 'a and returns
    either Ok value or Error of type 'e *)
type ('a, 'e) validator = 'a -> ('a, 'e) result

(** {1 Core Combinators} *)

(** Compose two validators - runs both and returns first error

    Example:
    {[
      let validate_port =
        int_range ~min:1 ~max:65535 ~error:(fun n -> `InvalidPort n)
    ]}
*)
val ( >>> ) : ('a, 'e) validator -> ('a, 'e) validator -> ('a, 'e) validator

(** Always succeed with the given value *)
val always_ok : ('a, 'e) validator

(** Always fail with the given error *)
val always_fail : 'e -> ('a, 'e) validator

(** {1 Integer Validators} *)

(** Validate integer is in range [min, max] inclusive

    @param min Minimum value (inclusive)
    @param max Maximum value (inclusive)
    @param error Function to create error from invalid value
*)
val int_range : min:int -> max:int -> error:(int -> 'e) -> (int, 'e) validator

(** Validate integer is non-negative (>= 0) *)
val non_negative : error:(int -> 'e) -> (int, 'e) validator

(** Validate integer is positive (> 0) *)
val positive : error:(int -> 'e) -> (int, 'e) validator

(** {1 String Validators} *)

(** Validate string is non-empty *)
val non_empty_string : error:'e -> (string, 'e) validator

(** Validate string satisfies a predicate *)
val string_satisfies : error:'e -> (string -> bool) -> (string, 'e) validator

(** {1 Parsing Validators} *)

(** Parse integer from string

    @param error Function to create error from invalid string
*)
val parse_int : error:(string -> 'e) -> string -> (int, 'e) result

(** Parse and validate integer in one step

    @param parse_error Function to create error from invalid string
    @param validator Validator to apply to parsed integer
*)
val parse_and_validate_int :
  parse_error:(string -> 'e) ->
  validator:(int, 'e) validator ->
  string -> (int, 'e) result

(** {1 Common Domain Validators} *)

(** Validate port number (1-65535) *)
val port : error:(int -> 'e) -> (int, 'e) validator

(** Validate timeout in milliseconds (>= 0) *)
val timeout_ms : error:(int -> 'e) -> (int, 'e) validator

(** Validate expiry in milliseconds (>= 0) *)
val expiry_ms : error:(int -> 'e) -> (int, 'e) validator

(** Validate replica count (>= 0) *)
val replica_count : error:(int -> 'e) -> (int, 'e) validator

(** {1 Composite Validators} *)

(** Validate a 2-tuple using two validators *)
val validate_pair :
  ('a, 'e) validator -> ('b, 'e) validator -> ('a * 'b, 'e) validator

(** Validate optional value *)
val validate_option : ('a, 'e) validator -> ('a option, 'e) validator

(** Validate all fields of a record

    Example:
    {[
      validate_all [
        (fun () -> Validation.timeout_ms ~error params.timeout_ms);
        (fun () -> Validation.replica_count ~error params.num_replicas);
      ]
    ]}
*)
val validate_all : (unit -> (unit, 'e) result) list -> (unit, 'e) result

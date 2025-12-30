(* Validation framework for command parameters *)

(* Validation result type - parameterized by error *)
type ('a, 'e) validator = 'a -> ('a, 'e) result

(* Core combinators *)

(** Compose two validators - runs both and returns first error *)
let ( >>> ) v1 v2 x =
  match v1 x with
  | Ok _ as ok -> (
      match v2 x with
      | Ok _ -> ok
      | Error _ as err -> err)
  | Error _ as err -> err

(** Always succeed *)
let always_ok x = Ok x

(** Always fail with given error *)
let always_fail error _x = Error error

(* Integer validators *)

(** Validate integer is in range [min, max] inclusive *)
let int_range ~min ~max ~error n =
  if n >= min && n <= max then
    Ok n
  else
    Error (error n)

(** Validate integer is non-negative (>= 0) *)
let non_negative ~error n =
  if n >= 0 then
    Ok n
  else
    Error (error n)

(** Validate integer is positive (> 0) *)
let positive ~error n =
  if n > 0 then
    Ok n
  else
    Error (error n)

(* String validators *)

(** Validate string is non-empty *)
let non_empty_string ~error s =
  if String.length s > 0 then
    Ok s
  else
    Error error

(** Validate string matches predicate *)
let string_satisfies ~error pred s =
  if pred s then
    Ok s
  else
    Error error

(* Parsing validators *)

(** Parse integer from string *)
let parse_int ~error s =
  match int_of_string_opt s with
  | Some n -> Ok n
  | None -> Error (error s)

(** Parse and validate integer *)
let parse_and_validate_int ~parse_error ~validator s =
  match parse_int ~error:parse_error s with
  | Ok n -> validator n
  | Error _ as e -> e

(* Common domain validators *)

(** Validate port number (1-65535) *)
let port ~error n =
  int_range ~min:1 ~max:65535 ~error n

(** Validate timeout in milliseconds (>= 0) *)
let timeout_ms ~error n =
  non_negative ~error n

(** Validate expiry in milliseconds (>= 0) *)
let expiry_ms ~error n =
  non_negative ~error n

(** Validate replica count (>= 0) *)
let replica_count ~error n =
  non_negative ~error n

(* Tuple validators *)

(** Validate a 2-tuple using two validators *)
let validate_pair v1 v2 (a, b) =
  match v1 a with
  | Ok a' -> (
      match v2 b with
      | Ok b' -> Ok (a', b')
      | Error e -> Error e)
  | Error e -> Error e

(** Validate optional value *)
let validate_option validator = function
  | None -> Ok None
  | Some x -> (
      match validator x with
      | Ok x' -> Ok (Some x')
      | Error e -> Error e)

(* Field validators for records *)

(** Validate all fields of a record - accumulates errors or returns Ok () *)
let validate_all validators =
  let rec loop = function
    | [] -> Ok ()
    | v :: rest -> (
        match v () with
        | Ok () -> loop rest
        | Error _ as e -> e)
  in
  loop validators

type t =
  | RString of string
  | RArray of t list
  | RError of string
  | RInteger of int
  | RBulkString of string
  | NullBulk
[@@deriving show, eq]

val to_string : t -> string
val of_string : string -> (t, string) Stdlib.result

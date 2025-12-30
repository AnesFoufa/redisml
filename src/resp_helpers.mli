(** High-level RESP parsing helpers for command processing

    This module provides convenient functions for extracting command names,
    arguments, and parameters from RESP Array values representing Redis commands.
*)

(** {1 Command Identification} *)

(** Extract command name from RESP array (case-insensitive)

    Example: [command_name (Array [BulkString "GET"; BulkString "key"])] returns [Some "get"]
*)
val command_name : Resp.t -> string option

(** Check if RESP value is a command with given name (case-insensitive)

    Example: [is_command "get" (Array [BulkString "GET"; BulkString "key"])] returns [true]
*)
val is_command : string -> Resp.t -> bool

(** {1 Argument Extraction} *)

(** Extract bulk string argument at position (0-indexed after command)

    Example: [bulk_string_arg 0 (Array [BulkString "GET"; BulkString "mykey"])] returns [Some "mykey"]
*)
val bulk_string_arg : int -> Resp.t -> string option

(** Extract all arguments after command as bulk strings

    Returns [None] if any argument is not a BulkString.
    Example: [bulk_string_args (Array [BulkString "SET"; BulkString "k"; BulkString "v"])]
    returns [Some ["k"; "v"]]
*)
val bulk_string_args : Resp.t -> string list option

(** Extract integer argument at position (parses bulk string to int)

    Example: [int_arg 0 (Array [BulkString "WAIT"; BulkString "2"; BulkString "100"])]
    returns [Some 2]
*)
val int_arg : int -> Resp.t -> int option

(** Extract two integer arguments

    Example: [int_args_2 (Array [BulkString "WAIT"; BulkString "2"; BulkString "100"])]
    returns [Some (2, 100)]
*)
val int_args_2 : Resp.t -> (int * int) option

(** Extract three integer arguments *)
val int_args_3 : Resp.t -> (int * int * int) option

(** Extract argument at position (any RESP type)

    Example: [arg 1 (Array [BulkString "SET"; BulkString "k"; Integer 42])]
    returns [Some (Integer 42)]
*)
val arg : int -> Resp.t -> Resp.t option

(** Get number of arguments (excludes command)

    Example: [arg_count (Array [BulkString "GET"; BulkString "key"])] returns [Some 1]
*)
val arg_count : Resp.t -> int option

(** {1 Command Matching} *)

(** Match command with specific arity (number of arguments)

    Returns [Some args] if command matches name and has exact number of arguments.
    Example: [command_with_arity "GET" 1 (Array [BulkString "GET"; BulkString "key"])]
    returns [Some [BulkString "key"]]
*)
val command_with_arity : string -> int -> Resp.t -> Resp.t list option

(** Match command with minimum arity

    Returns [Some args] if command matches name and has at least min_arity arguments.
*)
val command_with_min_arity : string -> int -> Resp.t -> Resp.t list option

(** {1 Parameter Parsing} *)

(** Extract key-value pairs from arguments (alternating pattern)

    Example: [key_value_pairs [BulkString "name"; BulkString "Alice"; BulkString "age"; Integer 30]]
    returns [Some [("name", BulkString "Alice"); ("age", Integer 30)]]
*)
val key_value_pairs : Resp.t list -> (string * Resp.t) list option

(** Parse optional parameters from argument list

    Applies parser repeatedly until it returns None, collecting results.
    Useful for parsing optional flags like PX/EX in SET command.

    @param parser Function that tries to parse option from args, returns (option, remaining_args)
*)
val parse_options :
  (Resp.t list -> ('a * Resp.t list) option) ->
  Resp.t list ->
  'a list option

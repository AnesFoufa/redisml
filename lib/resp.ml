open Angstrom

type t =
  | RString of string
  | RArray of t list
  | RError of string
  | RInteger of int
  | RBulkString of string
  | NullBulk
[@@deriving show, eq]

let not_separator c = c <> '\r' && c <> '\n'
let string_without_separator = take_while1 not_separator

let simple_string =
  char '+' *> string_without_separator <* string "\r\n" >>| fun s -> RString s

let simple_error =
  char '-' *> string_without_separator <* string "\r\n" >>| fun s -> RError s

let sign = choice [ char '+' *> return 1; char '-' *> return (-1); return 1 ]

let positive_integer =
  take_while1 (function '0' .. '9' -> true | _ -> false) >>| int_of_string

let signed_integer =
  sign >>= fun sign ->
  positive_integer >>| fun num -> sign * num

let integer =
  char ':' *> signed_integer <* string "\r\n" >>| fun i -> RInteger i

let bulk_string =
  char '$'
  *> ( signed_integer <* string "\r\n" >>= fun n ->
       count n (satisfy not_separator) )
  <* string "\r\n"
  >>| fun chars -> RBulkString (List.to_seq chars |> String.of_seq)

let resp =
  fix (fun resp ->
      let arr =
        char '*' *> (positive_integer <* string "\r\n" >>= fun n -> count n resp)
        >>| fun x -> RArray x
      in
      choice [ simple_string; simple_error; integer; bulk_string; arr ])

let rec to_string = function
  | RString s -> Printf.sprintf "+%s\r\n" s
  | RError s -> Printf.sprintf "-%s\r\n" s
  | RInteger i -> Printf.sprintf ":%d\r\n" i
  | RBulkString s -> Printf.sprintf "$%d\r\n%s\r\n" (String.length s) s
  | RArray elems ->
      Printf.sprintf "*%d\r\n%s" (List.length elems)
        (String.concat "" (List.map to_string elems))
  | NullBulk -> Printf.sprintf "$-1\r\n"

let of_string = parse_string ~consume:Consume.All resp

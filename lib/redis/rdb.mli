type metadata = (string, string) Hashtbl.t
type databases = (int, Database.t) Hashtbl.t

val of_string : string -> (metadata * databases, string) result

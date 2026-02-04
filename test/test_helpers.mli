(* Test helpers for database testing *)

(* Execute a command synchronously for testing using mock channels *)
val exec_command :
  Codecrafters_redis.Command.t ->
  Codecrafters_redis.Database.db ->
  current_time:float ->
  Codecrafters_redis.Resp.t

(* Check if database is in replica role by examining INFO output *)
val is_replica : Codecrafters_redis.Database.db -> bool

(* Check if database is in master role by examining INFO output *)
val is_master : Codecrafters_redis.Database.db -> bool

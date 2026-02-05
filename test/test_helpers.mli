(* Test helpers for database testing *)

type running_db =
  | Master of Codecrafters_redis.Database.master Codecrafters_redis.Database.t
  | Replica of
      Codecrafters_redis.Database.connected_replica
      Codecrafters_redis.Database.t

(* Execute a command synchronously for testing using mock channels *)
val exec_command :
  Codecrafters_redis.Resp.t ->
  running_db ->
  current_time:float ->
  Codecrafters_redis.Resp.t

(* Create and initialize a database synchronously for tests *)
val create_db : Codecrafters_redis.Config.t -> running_db

(* Check if database is in replica role by examining INFO output *)
val is_replica : running_db -> bool

(* Check if database is in master role by examining INFO output *)
val is_master : running_db -> bool

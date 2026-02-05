# Phantom Types for Database Role Safety

## Overview

The Database module now uses **phantom types** to enforce role-specific operations at compile time, preventing master-only or replica-only functions from being called on the wrong role.

## Type System

### Core Types

```ocaml
type 'a t                    (* Phantom typed database *)
type master                  (* Phantom marker for master role *)
type replica                 (* Phantom marker for replica role *)
type db = Master of master t | Replica of replica t  (* Dynamic wrapper *)
```

### Internal Implementation (GADT)

```ocaml
type _ role =
  | Master : Replica_manager.t -> master role
  | Replica : { mutable replication_offset : int } -> replica role

type 'a t = {
  storage : Storage.t;
  config : Config.t;
  role : 'a role;
}
```

The GADT links the phantom type parameter with concrete role data, ensuring type safety.

## API Changes

### Before (Runtime Checks)

```ocaml
(* Old API - runtime role checks *)
type t  (* Non-phantom typed *)

val handle_command : t -> Command.t -> ... -> Resp.t option Lwt.t
val increment_offset : t -> int -> unit
```

Usage required runtime checks:
```ocaml
let increment_offset db bytes =
  match db.role with
  | Replica state -> state.replication_offset <- state.replication_offset + bytes
  | Master _ -> failwith "increment_offset called on master"  (* Runtime error! *)
```

### After (Compile-Time Safety)

```ocaml
(* New API - compile-time role safety *)
type 'a t                     (* Phantom typed *)
type master
type replica
type db = Master of master t | Replica of replica t

val handle_master_command : master t -> Command.t -> ... -> Resp.t option Lwt.t
val handle_replica_command : replica t -> Command.t -> ... -> Resp.t option Lwt.t
val increment_offset : replica t -> int -> unit  (* Only accepts replicas! *)
```

Usage is type-safe:
```ocaml
(* This won't compile - type error! *)
let master_db : master t = ... in
Database.increment_offset master_db 100  (* Compile error: expected replica t *)

(* This works fine *)
let replica_db : replica t = ... in
Database.increment_offset replica_db 100  (* OK! *)
```

## Benefits

### 1. **Compile-Time Role Enforcement**

Role violations are caught by the type checker:

```ocaml
(* Master can't call increment_offset *)
Database.increment_offset master_db 100  (* TYPE ERROR *)

(* Replica can't handle PSYNC *)
let handle_psync (replica_db : replica t) =
  Database.handle_master_command replica_db psync_cmd  (* TYPE ERROR *)
```

### 2. **No Runtime Checks in Typed Functions**

Functions with phantom types don't need runtime role checks:

```ocaml
(* Old: needed runtime check *)
let increment_offset db bytes =
  match db.role with
  | Replica state -> ...
  | Master _ -> failwith "..."  (* Can happen at runtime *)

(* New: type system guarantees it's a replica *)
let increment_offset db bytes =
  let (Replica state) = db.role in  (* Always succeeds *)
  state.replication_offset <- state.replication_offset + bytes
```

### 3. **Self-Documenting API**

Type signatures clearly indicate which operations work on which roles:

```ocaml
val handle_master_command : master t -> ...   (* Masters only *)
val handle_replica_command : replica t -> ... (* Replicas only *)
val get_config : 'a t -> Config.t            (* Works for any role *)
```

### 4. **Eliminates Invalid States**

The type system prevents representing invalid states:
- Can't have a replica with a `Replica_manager.t`
- Can't have a master tracking `replication_offset`
- Can't accidentally propagate commands from a replica

## Usage Patterns

### Dynamic Role Dispatch

When the role is determined at runtime (e.g., `create`), use the `db` variant:

```ocaml
let database : db ref = ref (Database.create config)

(* Pattern match to dispatch to appropriate handler *)
match !database with
| Master master_db -> Database.handle_master_command master_db cmd ...
| Replica replica_db -> Database.handle_replica_command replica_db cmd ...
```

### Static Role Knowledge

When the role is statically known, use phantom types directly:

```ocaml
(* Master connection module - knows it's always a replica *)
val connect_to_master :
  database:replica t ->  (* Type guarantees it's a replica *)
  host:string ->
  ...

(* Inside implementation *)
let process_command replica_db cmd =
  Database.increment_offset replica_db bytes  (* Type-safe *)
```

## Implementation Details

### Role-Specific Functions

**Master-only operations:**
- `handle_master_command` - Handles all commands on master, including PSYNC and WAIT
- Commands that require `Replica_manager.t` access (propagation, WAIT)

**Replica-only operations:**
- `handle_replica_command` - Handles commands on replica, rejects PSYNC/WAIT
- `increment_offset` - Updates replication offset (only replicas have this)

**Generic operations:**
- `get_config` - Works for any role (`'a t`)
- `execute_command` - Pure command execution (internal, role-generic)

### GADT Pattern Matching

The GADT allows exhaustive pattern matching with type refinement:

```ocaml
(* When matching on db variant, types are refined *)
match !database with
| Master master_db ->
    (* master_db : master t *)
    Database.handle_master_command master_db cmd  (* OK *)
| Replica replica_db ->
    (* replica_db : replica t *)
    Database.increment_offset replica_db bytes  (* OK *)
```

## Migration Impact

### Files Changed

1. **src/database.mli** - Added phantom types and role-specific functions
2. **src/database.ml** - Implemented GADTs and separated handlers
3. **src/main.ml** - Pattern match on `db` variant for dispatch
4. **src/master_connection.mli** - Changed to accept `replica t`
5. **src/master_connection.ml** - Use `handle_replica_command`
6. **test/test_helpers.ml** - Pattern match on `db` for testing
7. **test/database_test.ml** - Extract replica from variant for offset tests

### Backward Compatibility

This is a **breaking change** requiring updates to all call sites:
- `Database.t` → `Database.db` (or specific `master t`/`replica t`)
- `handle_command` → `handle_master_command` or `handle_replica_command`
- Extract typed database from variant where needed

## Testing

All 143 tests pass with the phantom type implementation:
- **Storage**: 21 tests - Storage operations unchanged
- **RESP**: 47 tests - Protocol parsing unchanged
- **Command**: 50 tests - Command parsing unchanged
- **RDB**: 6 tests - RDB loading unchanged
- **Database**: 19 tests - Adapted to use phantom types

## Comparison with Alternatives

### Alternative 1: Runtime Checks (Previous Approach)

```ocaml
type t = { role: role; ... }
val handle_command : t -> ...
val increment_offset : t -> ...  (* Fails at runtime for master *)
```

**Pros:** Simple, flexible
**Cons:** Role errors only caught at runtime, requires defensive checks

### Alternative 2: Separate Master/Replica Types (No Shared Code)

```ocaml
module Master : sig
  type t
  val handle_command : t -> ...
end

module Replica : sig
  type t
  val handle_command : t -> ...
  val increment_offset : t -> ...
end
```

**Pros:** Complete separation
**Cons:** Code duplication, no shared polymorphic functions

### Current Approach: Phantom Types (Best of Both)

```ocaml
type 'a t
type master
type replica
val handle_master_command : master t -> ...
val handle_replica_command : replica t -> ...
val get_config : 'a t -> ...  (* Polymorphic! *)
```

**Pros:** Compile-time safety, code reuse, polymorphic functions
**Cons:** Slightly more complex types, GADT understanding needed

## Future Enhancements

Potential improvements:
1. Add more role-specific operations as phantom-typed functions
2. Use phantom types for command routing (master vs replica commands)
3. Extend pattern to other modules (e.g., `Protocol`, `Connection`)
4. Add type aliases for common patterns: `type master_db = master t`

## References

- [OCaml Manual: Phantom Types](https://v2.ocaml.org/manual/gadts-tutorial.html)
- [Real World OCaml: GADTs](https://dev.realworldocaml.org/gadts.html)
- [Phantom Types in Practice](https://blog.janestreet.com/howto-static-access-control-using-phantom-types/)

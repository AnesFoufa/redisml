# Documentation

This directory contains technical documentation for the Redis implementation.

## Available Documentation

### [Phantom Types](./PHANTOM_TYPES.md)
Comprehensive guide to the phantom type system used for compile-time role safety in the Database module. Explains:
- How phantom types enforce master/replica separation
- GADT implementation details
- Migration guide and API changes
- Benefits over runtime checks
- Usage patterns and examples

## Project Documentation

For general project information, see the root-level files:
- **[CLAUDE.md](../CLAUDE.md)** - Development guidance and architectural principles
- **[README.md](../README.md)** - Project overview and getting started

## Architecture Overview

The codebase follows a layered architecture:

```
┌─────────────────────────────────────┐
│         Application (main.ml)       │
├─────────────────────────────────────┤
│      Database (database.ml)         │ ← Phantom types for role safety
│  - Master operations                │
│  - Replica operations                │
├─────────────────────────────────────┤
│      Protocol (protocol.ml)         │
│  - RESP parsing (resp.ml)           │
│  - Command parsing (command.ml)     │
├─────────────────────────────────────┤
│      Storage (storage.ml)           │
│  - Key-value operations             │
│  - Expiry management                │
└─────────────────────────────────────┘
```

## Testing

Tests are maintained in the `with-tests` branch. See test files in `test/` directory:
- `resp_test.ml` - RESP protocol parsing (47 tests)
- `storage_test.ml` - Storage operations (21 tests)
- `command_test.ml` - Command parsing (50 tests)
- `database_test.ml` - Database integration (19 tests)
- `rdb_test.ml` - RDB file parsing (6 tests)

Total: **143 tests**

## Contributing Documentation

When adding new documentation:
1. Create markdown files in this `docs/` directory
2. Add entry to this README with brief description
3. Follow existing documentation structure and style
4. Include code examples where helpful
5. Keep documentation in sync with code changes

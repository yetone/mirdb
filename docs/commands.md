# Memcached Command Documentation

This page documents all supported Memcached commands in MirDB, including standard Memcached commands and MirDB-specific extensions.

## Standard Memcached Commands

MirDB supports the following standard Memcached text protocol commands:

### Storage Commands

#### SET
Store a key-value pair.

**Syntax:**
```
SET <key> <flags> <exptime> <bytes> [noreply]\r\n
<data>\r\n
```

**Parameters:**
- `<key>`: The key name (string, no spaces)
- `<flags>`: 16-bit integer stored with data (returned on retrieval)
- `<exptime>`: Expiration time in seconds (0 = never expire)
- `<bytes>`: Number of bytes in the data block
- `<data>`: The data to store
- `[noreply]`: Optional parameter to suppress response

**Example:**
```bash
# Store a value
SET mykey 0 3600 11\r\nHello World\r\n
```

**Response:**
- `STORED`: Data stored successfully
- `ERROR`: Invalid command or error

---

#### ADD
Store a value only if the key doesn't already exist.

**Syntax:**
```
ADD <key> <flags> <exptime> <bytes>\r\n
<data>\r\n
```

**Parameters:**
Same as SET command

**Example:**
```bash
# Add a value only if key doesn't exist
ADD newkey 0 0 5\r\nValue\r\n
```

**Response:**
- `STORED`: Data stored successfully
- `NOT_STORED`: Key already exists

---

#### REPLACE
Store a value only if the key already exists.

**Syntax:**
```
REPLACE <key> <flags> <exptime> <bytes>\r\n
<data>\r\n
```

**Parameters:**
Same as SET command

**Example:**
```bash
# Replace a value if key exists
REPLACE mykey 0 0 10\r\nNew Value\r\n
```

**Response:**
- `STORED`: Data replaced successfully
- `NOT_FOUND`: Key does not exist

---

#### APPEND
Append data to an existing value.

**Syntax:**
```
APPEND <key> <flags> <exptime> <bytes>\r\n
<data>\r\n
```

**Parameters:**
Same as SET command

**Example:**
```bash
# Append to existing value
APPEND mykey 0 0 9\r\n Appended\r\n
```

**Response:**
- `STORED`: Data appended successfully
- `NOT_FOUND`: Key does not exist

---

#### PREPEND
Prepend data to an existing value.

**Syntax:**
```
PREPEND <key> <flags> <exptime> <bytes>\r\n
<data>\r\n
```

**Parameters:**
Same as SET command

**Example:**
```bash
# Prepend to existing value
PREPEND mykey 0 0 6\r\nFirst \r\n
```

**Response:**
- `STORED`: Data prepended successfully
- `NOT_FOUND`: Key does not exist

---

### Retrieval Commands

#### GET
Retrieve one or more keys.

**Syntax:**
```
GET <key1> [<key2> ...]\r\n
```

**Parameters:**
- `<key>`: One or more key names (space-separated)

**Example:**
```bash
# Get a single key
GET mykey\r\n

# Get multiple keys
GET key1 key2 key3\r\n
```

**Response:**
```
VALUE <key> <flags> <bytes>\r\n
<data>\r\n
END\r\n
```

---

#### GETS
Retrieve keys with CAS token (same as GET in this implementation).

**Syntax:**
```
GETS <key1> [<key2> ...]\r\n
```

**Parameters:**
Same as GET command

**Example:**
```bash
# Get with CAS token
GETS mykey\r\n
```

**Response:**
```
VALUE <key> <flags> <bytes> <cas>\r\n
<data>\r\n
END\r\n
```

---

### Deletion Commands

#### DELETE
Delete a key.

**Syntax:**
```
DELETE <key> [noreply]\r\n
```

**Parameters:**
- `<key>`: Key name to delete
- `[noreply]`: Optional parameter to suppress response

**Example:**
```bash
# Delete a key
DELETE mykey\r\n
```

**Response:**
- `DELETED`: Key deleted successfully
- `NOT_FOUND`: Key does not exist

---

## MirDB-Specific Commands

These commands are specific to MirDB and provide additional functionality beyond standard Memcached.

### INFO
Display database status and level information.

**Syntax:**
```
INFO\r\n
```

**Description:**
Returns detailed information about the database state, including:
- Number of keys in each LSM level
- Memory usage statistics
- Compaction status
- Storage information

**Example:**
```bash
INFO\r\n
```

**Response:**
```
MirDB Database Status\r\nLSM Level 0: 3 SSTables, 1024 keys\r\nLSM Level 1: 5 SSTables, 2048 keys\r\n...
```

---

### MAJOR_COMPACTION
Trigger manual major compaction across LSM levels.

**Syntax:**
```
MAJOR_COMPACTION\r\n
```

**Description:**
Manually triggers compaction across all LSM levels to optimize storage and improve read performance. This is useful when you want to force compaction rather than waiting for automatic compaction to occur based on size thresholds.

**Example:**
```bash
MAJOR_COMPACTION\r\n
```

**Response:**
```
COMPACTION_STARTED\r\n
```

---

## Response Codes

All commands return one of the following response codes:

| Response Code | Meaning |
|---------------|---------|
| `STORED` | Data stored successfully |
| `NOT_STORED` | Data not stored (condition not met for add/replace) |
| `EXISTS` | CAS check failed (key exists with different CAS value) |
| `NOT_FOUND` | Key not found (for delete/replace/append/prepend) |
| `DELETED` | Key deleted successfully |
| `ERROR` | Unknown command or parsing error |
| `CLIENT_ERROR <message>` | Client input error (e.g., malformed command) |
| `SERVER_ERROR <message>` | Server error (internal failure) |
| `COMPACTION_STARTED` | Major compaction initiated successfully |

---

## Example Session

Here's a complete example of using Memcached commands with MirDB:

```bash
# Connect to MirDB (default port: 12333)
telnet localhost 12333

# Store a value
SET user:1 0 3600 15\r\n{"name":"Alice"}\r\n
STORED\r\n

# Retrieve the value
GET user:1\r\n
VALUE user:1 0 15\r\n
{"name":"Alice"}\r\n
END\r\n

# Add a new key
ADD user:2 0 3600 13\r\n{"name":"Bob"}\r\n
STORED\r\n

# Try to add the same key again
ADD user:2 0 3600 15\r\n{"name":"Charlie"}\r\n
NOT_STORED\r\n

# Append data
APPEND user:1 0 0 14\r\n, "age": 30\r\n
STORED\r\n

# Delete a key
DELETE user:2\r\n
DELETED\r\n

# Check database status
INFO\r\n
MirDB Database Status\r\nLSM Level 0: 2 SSTables\r\n...

# Trigger major compaction
MAJOR_COMPACTION\r\n
COMPACTION_STARTED\r\n
```

---

## Important Notes

- Commands are case-insensitive (SET, set, Set all work)
- Keys can be up to 250 bytes
- Values can be up to 1MB (configurable)
- TTL is supports seconds (up to 30 days) or Unix timestamp
- All data is persistent across server restarts (unlike standard Memcached)
- MirDB uses LSM tree storage for efficient write performance

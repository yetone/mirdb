/**
 * Getting Started steps data.
 * Owner: Scenario 4 - Getting Started Section
 *
 * This file contains the getting started steps to enable
 * easy content updates without code changes (NFR-5).
 */

import { Step } from '../types'

export const steps: Step[] = [
  {
    number: 1,
    title: 'Install and Run MirDB',
    content: 'Clone the repository and build the project using Cargo, then start the server.',
    code: `# Clone and build
git clone https://github.com/example/mirdb.git
cd mirdb

# Build and run
cargo build --release
cargo run --release

# Server starts on 0.0.0.0:12333`,
  },
  {
    number: 2,
    title: 'Connect with a Memcached Client',
    content: 'Use any memcached-compatible client to connect to MirDB on localhost:12333.',
    code: `# Using telnet
telnet localhost 12333

# Or using netcat
nc localhost 12333

# Python client example
import memcache
mc = memcache.Client(['localhost:12333'])`,
  },
  {
    number: 3,
    title: 'Set a Value',
    content: 'Store a key-value pair using the SET command. Format: set <key> <flags> <ttl> <bytes>',
    code: `set mykey 0 0 5
hello
STORED`,
  },
  {
    number: 4,
    title: 'Get a Value',
    content: 'Retrieve stored values using the GET command.',
    code: `get mykey
VALUE mykey 0 5
hello
END`,
  },
  {
    number: 5,
    title: 'Delete a Value',
    content: 'Remove a key from the store using the DELETE command.',
    code: `delete mykey
DELETED

# Verify deletion
get mykey
END`,
  },
]

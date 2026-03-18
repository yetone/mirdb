/**
 * Code Examples Data.
 * Owner: Scenario 3 - Usage Examples and Code Blocks
 *
 * Contains Memcached protocol examples:
 * - SET command
 * - GET command
 * - DELETE command
 */

import type { CodeExample } from '../types';

export const codeExamples: CodeExample[] = [
  {
    language: 'bash',
    code: `# Connect to MirDB using telnet
telnet localhost 11211

# SET a key-value pair
SET mykey 0 0 5
hello
STORED

# GET the value
GET mykey
VALUE mykey 0 5
hello
END

# DELETE the key
DELETE mykey
DELETED`,
    filename: 'memcached-commands.txt',
  },
];

export const memcachedExample = `# Connect to MirDB using telnet
telnet localhost 11211

# SET a key-value pair
SET mykey 0 0 5
hello
STORED

# GET the value
GET mykey
VALUE mykey 0 5
hello
END

# DELETE the key
DELETE mykey
DELETED`;

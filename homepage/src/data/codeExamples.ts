/**
 * Code examples for Quick Start section.
 * Owner: Scenario 4 - Quick Start Section with Code Examples
 */
import { CodeExample } from '@/types';

export const installationExample: CodeExample = {
  language: 'bash',
  title: 'Installation',
  code: `# Clone and build MirDB
git clone https://github.com/yetone/mirdb.git
cd mirdb
cargo build --release

# Run MirDB server
./target/release/mirdb`,
};

export const setExample: CodeExample = {
  language: 'bash',
  title: 'SET Command',
  code: `# Store a value with SET command
# Using telnet or netcat to connect
echo "set mykey 0 0 5\\r\\nhello\\r" | nc localhost 11211

# Response: STORED`,
};

export const getExample: CodeExample = {
  language: 'bash',
  title: 'GET Command',
  code: `# Retrieve a value with GET command
echo "get mykey\\r" | nc localhost 11211

# Response:
# VALUE mykey 0 5
# hello
# END`,
};

export const pythonExample: CodeExample = {
  language: 'python',
  title: 'Python Client',
  code: `import memcache

# Connect to MirDB (memcached protocol compatible)
mc = memcache.Client(['localhost:11211'])

# SET a value
mc.set('user:1', 'John Doe')

# GET the value
user = mc.get('user:1')
print(user)  # Output: John Doe`,
};

export const codeExamples: CodeExample[] = [
  installationExample,
  setExample,
  getExample,
  pythonExample,
];

export default codeExamples;

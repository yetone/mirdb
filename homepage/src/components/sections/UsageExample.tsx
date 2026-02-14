/**
 * Usage example section component.
 * Owner: Scenario 6 - Code Examples and Quick Start
 *
 * Features:
 * - Interactive code block with memcached commands
 * - Syntax highlighting for shell/terminal
 * - Copy-to-clipboard functionality
 * - Commands: set, get, delete examples
 */
import React from 'react';
import { CodeBlock } from '../common/CodeBlock';

const USAGE_EXAMPLE_CODE = `# Connect to MirDB using telnet
telnet localhost 12333

# Set a value (format: set <key> <flags> <exptime> <bytes>)
set mykey 0 0 5
hello
STORED

# Get the value
get mykey
VALUE mykey 0 5
hello
END

# Delete the key
delete mykey
DELETED`;

export const UsageExample: React.FC = () => {
  return (
    <section
      id="usage"
      className="py-16 md:py-24 bg-gray-50 dark:bg-gray-800/50"
    >
      <div className="container mx-auto px-4 max-w-4xl">
        <h2 className="text-3xl md:text-4xl font-bold text-center mb-4 text-gray-900 dark:text-white">
          Usage Example
        </h2>
        <p className="text-gray-600 dark:text-gray-400 text-center mb-8 max-w-2xl mx-auto">
          MirDB uses the standard Memcached text protocol. Connect using any
          Memcached client or telnet to start storing and retrieving data.
        </p>
        <div className="max-w-3xl mx-auto">
          <CodeBlock
            code={USAGE_EXAMPLE_CODE}
            language="bash"
            title="Memcached Commands"
            showLineNumbers
          />
        </div>
        <div className="mt-8 text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">
            The default port is <code className="px-1.5 py-0.5 bg-gray-200 dark:bg-gray-700 rounded text-gray-800 dark:text-gray-200">12333</code>.
            All standard Memcached commands are supported.
          </p>
        </div>
      </div>
    </section>
  );
};

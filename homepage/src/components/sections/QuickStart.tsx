/**
 * Quick Start Section Component.
 * Owner: Scenario 3 - Quick Start Section with Code Examples
 *
 * Responsibilities:
 * - Display code examples for set/get operations
 * - Integrate syntax highlighting
 * - Provide copy-to-clipboard functionality
 *
 * Requirements:
 * - REQ-4: Quick-start code examples
 * - US-2: Working code examples
 */

import { CodeBlock } from '@/components/ui/CodeBlock';

const CODE_EXAMPLES = {
  connect: {
    title: 'Connect to MirDB',
    language: 'bash',
    code: `# Connect using telnet (default port: 12333)
telnet localhost 12333`,
  },
  setGet: {
    title: 'Set and Get Operations',
    language: 'bash',
    code: `# Set a key-value pair
set mykey 0 0 5
hello
STORED

# Get the value
get mykey
VALUE mykey 0 5
hello
END`,
  },
  operations: {
    title: 'Common Operations',
    language: 'bash',
    code: `# Add (only if key doesn't exist)
add newkey 0 0 5
world
STORED

# Replace (only if key exists)
replace mykey 0 0 6
hello!
STORED

# Delete a key
delete mykey
DELETED`,
  },
};

export function QuickStart() {
  return (
    <section
      id="quick-start"
      aria-labelledby="quick-start-heading"
      role="region"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-gray-50 dark:bg-gray-800"
    >
      <div className="max-w-4xl mx-auto">
        <h2
          id="quick-start-heading"
          className="text-3xl font-bold text-gray-900 dark:text-white mb-4"
        >
          Quick Start
        </h2>
        <p className="text-lg text-gray-600 dark:text-gray-300 mb-8">
          Get started with MirDB in seconds. Connect using any Memcached client or telnet to interact with the database.
        </p>

        <div className="space-y-8">
          {/* Step 1: Connect */}
          <div>
            <h3 className="text-xl font-semibold text-gray-900 dark:text-white mb-3">
              1. Start the Server
            </h3>
            <p className="text-gray-600 dark:text-gray-300 mb-4">
              Run MirDB and connect using telnet or any Memcached-compatible client:
            </p>
            <CodeBlock
              code={CODE_EXAMPLES.connect.code}
              language={CODE_EXAMPLES.connect.language}
              title={CODE_EXAMPLES.connect.title}
            />
          </div>

          {/* Step 2: Set and Get */}
          <div>
            <h3 className="text-xl font-semibold text-gray-900 dark:text-white mb-3">
              2. Store and Retrieve Data
            </h3>
            <p className="text-gray-600 dark:text-gray-300 mb-4">
              Use the standard Memcached protocol commands to set and get values:
            </p>
            <CodeBlock
              code={CODE_EXAMPLES.setGet.code}
              language={CODE_EXAMPLES.setGet.language}
              title={CODE_EXAMPLES.setGet.title}
            />
          </div>

          {/* Step 3: More Operations */}
          <div>
            <h3 className="text-xl font-semibold text-gray-900 dark:text-white mb-3">
              3. Additional Commands
            </h3>
            <p className="text-gray-600 dark:text-gray-300 mb-4">
              MirDB supports common Memcached operations including add, replace, and delete:
            </p>
            <CodeBlock
              code={CODE_EXAMPLES.operations.code}
              language={CODE_EXAMPLES.operations.language}
              title={CODE_EXAMPLES.operations.title}
            />
          </div>
        </div>

        <div className="mt-12 p-6 bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
            Why MirDB?
          </h3>
          <p className="text-gray-600 dark:text-gray-300">
            Unlike standard Memcached, MirDB persists your data to disk using an LSM tree architecture.
            Your data survives restarts while maintaining the familiar Memcached protocol interface.
          </p>
        </div>
      </div>
    </section>
  );
}

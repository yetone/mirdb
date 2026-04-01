/**
 * Usage Demo Section Component.
 * Owner: Scenario 2 - Usage Demo Section
 *
 * Displays code examples for memcached protocol commands:
 * - set command with STORED response
 * - get command with VALUE response
 * - delete command with DELETED response
 */

import { CodeBlock } from './CodeBlock'

const SET_EXAMPLE = `$ telnet localhost 11211
Trying 127.0.0.1...
Connected to localhost.

set mykey 0 0 5
hello
STORED`

const GET_EXAMPLE = `get mykey
VALUE mykey 0 5
hello
END`

const DELETE_EXAMPLE = `delete mykey
DELETED`

export function UsageDemo() {
  return (
    <section
      id="usage"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-white dark:bg-gray-800"
      aria-labelledby="usage-heading"
    >
      <div className="max-w-4xl mx-auto">
        <div className="text-center mb-12">
          <h2
            id="usage-heading"
            className="text-3xl font-bold text-gray-900 dark:text-white mb-4"
          >
            Usage Demo
          </h2>
          <p className="text-gray-600 dark:text-gray-300 max-w-2xl mx-auto">
            MirDB supports the standard memcached text protocol. Connect using telnet or any memcached client.
          </p>
        </div>

        <div className="space-y-8" data-testid="usage-demo-content">
          <div>
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-3">
              SET Command
            </h3>
            <p className="text-gray-600 dark:text-gray-400 mb-2 text-sm">
              Store a value with: <code className="bg-gray-100 dark:bg-gray-700 px-1 rounded">set &lt;key&gt; &lt;flags&gt; &lt;exptime&gt; &lt;bytes&gt;</code>
            </p>
            <CodeBlock code={SET_EXAMPLE} language="shell" />
          </div>

          <div>
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-3">
              GET Command
            </h3>
            <p className="text-gray-600 dark:text-gray-400 mb-2 text-sm">
              Retrieve a value with: <code className="bg-gray-100 dark:bg-gray-700 px-1 rounded">get &lt;key&gt;</code>
            </p>
            <CodeBlock code={GET_EXAMPLE} language="shell" />
          </div>

          <div>
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-3">
              DELETE Command
            </h3>
            <p className="text-gray-600 dark:text-gray-400 mb-2 text-sm">
              Remove a value with: <code className="bg-gray-100 dark:bg-gray-700 px-1 rounded">delete &lt;key&gt;</code>
            </p>
            <CodeBlock code={DELETE_EXAMPLE} language="shell" />
          </div>
        </div>
      </div>
    </section>
  )
}

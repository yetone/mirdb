/**
 * Quick Start Section Component
 * Owner: Scenario 5 - Quick Start Section
 *
 * Expected exports:
 * - QuickStart: Section with code example and copy functionality
 *
 * Requirements:
 * - Display MirDB command examples
 * - Syntax highlighting for code
 * - Copy to clipboard button
 * - Visual feedback on successful copy
 */

import { CodeBlock } from '../ui/CodeBlock';

const QUICK_START_CODE = `$ mirdb-server --port 11211
$ telnet localhost 11211
$ set mykey 0 0 5
hello
$ get mykey`;

export function QuickStart() {
  return (
    <section
      id="quickstart"
      data-testid="quickstart"
      className="py-20 bg-white dark:bg-gray-900"
      aria-labelledby="quickstart-heading"
    >
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <h2
          id="quickstart-heading"
          className="text-3xl font-bold text-center text-gray-900 dark:text-white mb-4"
        >
          Quick Start
        </h2>
        <p className="text-center text-gray-600 dark:text-gray-400 mb-8 max-w-2xl mx-auto">
          Get up and running with MirDB in seconds. Start the server and connect using any Memcached client.
        </p>
        <div className="max-w-2xl mx-auto">
          <CodeBlock
            code={QUICK_START_CODE}
            language="shell"
            showCopyButton={true}
          />
        </div>
      </div>
    </section>
  );
}

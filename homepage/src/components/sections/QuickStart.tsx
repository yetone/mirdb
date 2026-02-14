/**
 * Quick start section component.
 * Owner: Scenario 6 - Code Examples and Quick Start
 *
 * Features:
 * - Installation commands for common platforms
 * - Minimum system requirements
 * - First connection steps
 * - Copy-to-clipboard for commands
 */
import React from 'react';
import { CodeBlock } from '../common/CodeBlock';

const INSTALL_COMMAND = `# Clone the repository
git clone https://github.com/yetone/mirdb.git
cd mirdb

# Build with Cargo
cargo build --release

# Run MirDB
./target/release/mirdb-server`;

const FIRST_CONNECTION = `# In a new terminal, connect using telnet
telnet localhost 12333

# Or use any Memcached client library`;

export const QuickStart: React.FC = () => {
  return (
    <section
      id="quickstart"
      className="py-16 md:py-24 bg-white dark:bg-gray-900"
    >
      <div className="container mx-auto px-4 max-w-4xl">
        <h2 className="text-3xl md:text-4xl font-bold text-center mb-4 text-gray-900 dark:text-white">
          Quick Start
        </h2>
        <p className="text-gray-600 dark:text-gray-400 text-center mb-12 max-w-2xl mx-auto">
          Get MirDB running in minutes. Just clone, build, and connect.
        </p>

        <div className="space-y-8">
          {/* System Requirements */}
          <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-6">
            <h3 className="text-xl font-semibold mb-4 text-gray-900 dark:text-white">
              System Requirements
            </h3>
            <ul className="space-y-2 text-gray-600 dark:text-gray-400">
              <li className="flex items-start">
                <span className="text-green-500 mr-2">✓</span>
                <span>Rust 1.70 or later (with Cargo)</span>
              </li>
              <li className="flex items-start">
                <span className="text-green-500 mr-2">✓</span>
                <span>Linux, macOS, or Windows</span>
              </li>
              <li className="flex items-start">
                <span className="text-green-500 mr-2">✓</span>
                <span>512 MB RAM minimum (1 GB recommended)</span>
              </li>
            </ul>
          </div>

          {/* Installation */}
          <div>
            <h3 className="text-xl font-semibold mb-4 text-gray-900 dark:text-white">
              Step 1: Installation
            </h3>
            <CodeBlock
              code={INSTALL_COMMAND}
              language="bash"
              title="Install MirDB"
            />
          </div>

          {/* First Connection */}
          <div>
            <h3 className="text-xl font-semibold mb-4 text-gray-900 dark:text-white">
              Step 2: Connect
            </h3>
            <CodeBlock
              code={FIRST_CONNECTION}
              language="bash"
              title="Connect to MirDB"
            />
          </div>

          {/* Next Steps */}
          <div className="bg-blue-50 dark:bg-blue-900/20 rounded-lg p-6 border border-blue-200 dark:border-blue-800">
            <h3 className="text-xl font-semibold mb-2 text-blue-900 dark:text-blue-100">
              What&apos;s Next?
            </h3>
            <p className="text-blue-800 dark:text-blue-200 mb-4">
              Once connected, you can use standard Memcached commands like{' '}
              <code className="px-1.5 py-0.5 bg-blue-100 dark:bg-blue-800 rounded">
                set
              </code>
              ,{' '}
              <code className="px-1.5 py-0.5 bg-blue-100 dark:bg-blue-800 rounded">
                get
              </code>
              , and{' '}
              <code className="px-1.5 py-0.5 bg-blue-100 dark:bg-blue-800 rounded">
                delete
              </code>{' '}
              to store and retrieve data.
            </p>
            <a
              href="https://github.com/yetone/mirdb"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center text-blue-600 dark:text-blue-400 hover:underline font-medium"
            >
              View Full Documentation
              <svg
                className="w-4 h-4 ml-1"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
                aria-hidden="true"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M14 5l7 7m0 0l-7 7m7-7H3"
                />
              </svg>
            </a>
          </div>
        </div>
      </div>
    </section>
  );
};

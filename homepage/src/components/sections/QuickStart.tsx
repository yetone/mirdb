/**
 * Quick Start Section component.
 * Owner: Scenario 5 - Quick Start Section with Code Examples
 *
 * Features:
 * - Prerequisites list (Rust toolchain)
 * - Step-by-step installation commands
 * - CodeBlock components with copy functionality
 * - Memcached client connection example
 *
 * Requirements: REQ-6, REQ-7
 */

'use client';

import React from 'react';
import { Terminal, CheckCircle2 } from 'lucide-react';
import { CodeBlock } from '@/components/ui/CodeBlock';
import { QUICK_START_COMMANDS } from '@/lib/constants';

interface Prerequisite {
  name: string;
  description: string;
  link?: string;
}

const PREREQUISITES: Prerequisite[] = [
  {
    name: 'Rust toolchain',
    description: 'Install via rustup.rs',
    link: 'https://rustup.rs/',
  },
  {
    name: 'Git',
    description: 'For cloning the repository',
    link: 'https://git-scm.com/',
  },
];

const MEMCACHED_EXAMPLE = `# Connect using telnet (memcached protocol)
telnet localhost 11211

# Set a key-value pair
set mykey 0 0 5
hello

# Get the value back
get mykey`;

export function QuickStart() {
  return (
    <section
      id="quick-start"
      className="py-20 px-4 bg-white dark:bg-slate-800"
      aria-labelledby="quick-start-heading"
    >
      <div className="max-w-4xl mx-auto">
        {/* Section Header */}
        <div className="text-center mb-12">
          <div className="flex justify-center mb-4">
            <div className="p-3 rounded-full bg-accent-500/10">
              <Terminal className="w-8 h-8 text-accent-500" aria-hidden="true" />
            </div>
          </div>
          <h2
            id="quick-start-heading"
            className="text-3xl md:text-4xl font-bold text-primary-900 dark:text-white mb-4"
          >
            Quick Start
          </h2>
          <p className="text-lg text-gray-600 dark:text-gray-300 max-w-2xl mx-auto">
            Get MirDB up and running in minutes with these simple steps.
          </p>
        </div>

        {/* Prerequisites */}
        <div className="mb-10" data-testid="prerequisites-section">
          <h3 className="text-xl font-semibold text-primary-900 dark:text-white mb-4">
            Prerequisites
          </h3>
          <ul className="space-y-3" role="list" aria-label="Prerequisites list">
            {PREREQUISITES.map((prereq) => (
              <li
                key={prereq.name}
                className="flex items-start gap-3 text-gray-700 dark:text-gray-300"
                data-testid="prerequisite-item"
              >
                <CheckCircle2
                  className="w-5 h-5 text-green-500 flex-shrink-0 mt-0.5"
                  aria-hidden="true"
                />
                <span>
                  <strong className="font-medium text-primary-900 dark:text-white">
                    {prereq.name}
                  </strong>
                  {prereq.description && (
                    <span className="text-gray-600 dark:text-gray-400">
                      {' - '}
                      {prereq.link ? (
                        <a
                          href={prereq.link}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-accent-500 hover:underline"
                        >
                          {prereq.description}
                        </a>
                      ) : (
                        prereq.description
                      )}
                    </span>
                  )}
                </span>
              </li>
            ))}
          </ul>
        </div>

        {/* Installation Steps */}
        <div className="mb-10" data-testid="installation-section">
          <h3 className="text-xl font-semibold text-primary-900 dark:text-white mb-4">
            Installation
          </h3>
          <div className="space-y-4">
            {/* Step 1: Clone the repository */}
            <div data-testid="step-clone">
              <p className="text-sm text-gray-600 dark:text-gray-400 mb-2">
                1. Clone the repository
              </p>
              <CodeBlock
                code={QUICK_START_COMMANDS[0].code}
                language={QUICK_START_COMMANDS[0].lang}
              />
            </div>

            {/* Step 2: Navigate to directory */}
            <div data-testid="step-cd">
              <p className="text-sm text-gray-600 dark:text-gray-400 mb-2">
                2. Navigate to the project directory
              </p>
              <CodeBlock
                code={QUICK_START_COMMANDS[1].code}
                language={QUICK_START_COMMANDS[1].lang}
              />
            </div>

            {/* Step 3: Build and run */}
            <div data-testid="step-run">
              <p className="text-sm text-gray-600 dark:text-gray-400 mb-2">
                3. Build and run MirDB
              </p>
              <CodeBlock
                code={QUICK_START_COMMANDS[2].code}
                language={QUICK_START_COMMANDS[2].lang}
              />
            </div>
          </div>
        </div>

        {/* Usage Example */}
        <div data-testid="usage-section">
          <h3 className="text-xl font-semibold text-primary-900 dark:text-white mb-4">
            Connect Using Memcached Protocol
          </h3>
          <p className="text-gray-600 dark:text-gray-400 mb-4">
            MirDB is compatible with standard memcached clients. Here&apos;s how to connect
            and perform basic operations:
          </p>
          <CodeBlock
            code={MEMCACHED_EXAMPLE}
            language="bash"
            data-testid="memcached-example"
          />
        </div>
      </div>
    </section>
  );
}

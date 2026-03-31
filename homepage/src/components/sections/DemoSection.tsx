/**
 * Demo Section Component
 * Owner: Scenario 6 - Demo Section
 *
 * Displays usage demonstration:
 * - Embedded usage.gif
 * - Input/output interaction display
 *
 * Requirements: REQ-7, Story 4 (Usage Demo)
 */

import { Play } from 'lucide-react';

export interface DemoSectionProps {
  className?: string;
}

export function DemoSection({ className = '' }: DemoSectionProps) {
  return (
    <section
      id="demo"
      className={`py-16 px-4 sm:px-6 lg:px-8 bg-gray-50 dark:bg-gray-800 ${className}`}
      aria-labelledby="demo-heading"
      data-testid="demo-section"
    >
      <div className="max-w-4xl mx-auto">
        <div className="text-center mb-12">
          <div className="flex items-center justify-center gap-3 mb-4">
            <Play
              className="w-8 h-8 text-primary-600 dark:text-primary-400"
              aria-hidden="true"
            />
            <h2
              id="demo-heading"
              className="text-3xl font-bold text-gray-900 dark:text-white sm:text-4xl"
              data-testid="demo-heading"
            >
              See MirDB in Action
            </h2>
          </div>
          <p
            className="text-lg text-gray-600 dark:text-gray-300 max-w-2xl mx-auto"
            data-testid="demo-description"
          >
            Watch how MirDB handles Memcached protocol commands. The demo below shows
            basic get and set operations, demonstrating the familiar command-line interface
            that makes MirDB a drop-in replacement for Memcached with persistent storage.
          </p>
        </div>

        <div
          className="bg-white dark:bg-gray-900 rounded-xl shadow-lg overflow-hidden"
          data-testid="demo-container"
        >
          <div className="bg-gray-800 dark:bg-gray-950 px-4 py-2 flex items-center gap-2">
            <div className="flex gap-1.5">
              <span className="w-3 h-3 rounded-full bg-red-500" aria-hidden="true" />
              <span className="w-3 h-3 rounded-full bg-yellow-500" aria-hidden="true" />
              <span className="w-3 h-3 rounded-full bg-green-500" aria-hidden="true" />
            </div>
            <span className="text-gray-400 text-sm ml-2">Terminal - MirDB Demo</span>
          </div>
          <div className="p-4 bg-gray-900">
            <img
              src="/assets/usage.gif"
              alt="MirDB usage demonstration showing Memcached protocol commands including get and set operations with input and output examples"
              className="w-full h-auto rounded"
              data-testid="demo-gif"
              loading="lazy"
            />
          </div>
        </div>

        <div className="mt-8 text-center">
          <p
            className="text-sm text-gray-500 dark:text-gray-400"
            data-testid="demo-caption"
          >
            The demonstration shows standard Memcached commands: connect, set key-value pairs,
            and retrieve stored data. All data is persisted to disk for durability.
          </p>
        </div>
      </div>
    </section>
  );
}

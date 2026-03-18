/**
 * Usage Examples Section.
 * Owner: Scenario 3 - Usage Examples and Code Blocks
 *
 * Displays:
 * - Memcached protocol code examples
 * - Syntax-highlighted code blocks
 * - Usage animation GIF
 */

import { CodeBlock } from '../ui/CodeBlock';
import { memcachedExample } from '../../data/codeExamples';

export function UsageExamples() {
  return (
    <section id="usage" className="py-16 px-4 sm:px-6 lg:px-8" aria-labelledby="usage-heading">
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-12">
          <h2
            id="usage-heading"
            className="text-3xl sm:text-4xl font-bold text-white mb-4"
          >
            Usage Examples
          </h2>
          <p className="text-lg text-gray-400 max-w-2xl mx-auto">
            MirDB speaks the Memcached protocol. Use your favorite tools and libraries
            to interact with the database.
          </p>
        </div>

        <div className="grid lg:grid-cols-2 gap-8 items-start">
          {/* Code Example */}
          <div className="order-2 lg:order-1">
            <h3 className="text-xl font-semibold text-white mb-4">
              Memcached Protocol Commands
            </h3>
            <CodeBlock
              code={memcachedExample}
              language="bash"
              filename="memcached-commands.txt"
              showCopyButton={true}
            />
          </div>

          {/* Usage GIF */}
          <div className="order-1 lg:order-2">
            <h3 className="text-xl font-semibold text-white mb-4">
              Live Demo
            </h3>
            <div className="rounded-lg overflow-hidden bg-gray-900 border border-gray-700">
              <img
                src="/assets/usage.gif"
                alt="MirDB usage demonstration showing Memcached protocol commands in action"
                className="w-full h-auto"
                loading="lazy"
                data-testid="usage-gif"
              />
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

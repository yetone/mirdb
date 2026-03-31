/**
 * Quick Start Section Component
 * Owner: Scenario 3 - Quick Start Section
 *
 * Displays installation and usage instructions:
 * - Code blocks with syntax highlighting
 * - Copy-to-clipboard functionality
 * - Links to full documentation
 *
 * Requirements: REQ-3, Story 3 (Quick Start)
 */

import { ExternalLink, Terminal, Package } from 'lucide-react';
import { CodeBlock } from '../ui/CodeBlock';

interface QuickStartSectionProps {
  onCopy?: (code: string) => void;
}

const installationCode = `# Clone the repository
git clone https://github.com/yetone/mirdb.git
cd mirdb

# Build with Cargo
cargo build --release`;

const usageCode = `# Start the MirDB server
./target/release/mirdb-server

# Connect using any Memcached client
telnet localhost 11211

# Basic Memcached commands
set mykey 0 0 5
hello
STORED

get mykey
VALUE mykey 0 5
hello
END`;

const rustClientCode = `// Add to Cargo.toml
[dependencies]
memcache = "0.17"

// Connect to MirDB
use memcache::Client;

fn main() {
    let client = Client::connect("memcache://localhost:11211").unwrap();

    // Set a value
    client.set("key", "value", 0).unwrap();

    // Get a value
    let value: Option<String> = client.get("key").unwrap();
    println!("Value: {:?}", value);
}`;

export function QuickStartSection({ onCopy }: QuickStartSectionProps) {
  return (
    <section
      id="quick-start"
      className="py-16 px-4 md:px-8 bg-gray-50 dark:bg-gray-900"
      data-testid="quick-start-section"
    >
      <div className="max-w-4xl mx-auto">
        <h2 className="text-3xl md:text-4xl font-bold text-center mb-4 text-gray-900 dark:text-white">
          Quick Start
        </h2>
        <p className="text-lg text-center text-gray-600 dark:text-gray-400 mb-12">
          Get MirDB running in minutes with these simple steps
        </p>

        <div className="space-y-8">
          {/* Installation Section */}
          <div>
            <div className="flex items-center gap-2 mb-4">
              <Package className="w-5 h-5 text-blue-500" />
              <h3 className="text-xl font-semibold text-gray-900 dark:text-white">
                Installation
              </h3>
            </div>
            <CodeBlock
              code={installationCode}
              language="bash"
              title="Terminal"
              showCopyButton
            />
          </div>

          {/* Usage Section */}
          <div>
            <div className="flex items-center gap-2 mb-4">
              <Terminal className="w-5 h-5 text-green-500" />
              <h3 className="text-xl font-semibold text-gray-900 dark:text-white">
                Basic Usage
              </h3>
            </div>
            <CodeBlock
              code={usageCode}
              language="bash"
              title="Memcached Protocol Commands"
              showCopyButton
            />
          </div>

          {/* Rust Client Example */}
          <div>
            <div className="flex items-center gap-2 mb-4">
              <Package className="w-5 h-5 text-orange-500" />
              <h3 className="text-xl font-semibold text-gray-900 dark:text-white">
                Rust Client Example
              </h3>
            </div>
            <CodeBlock
              code={rustClientCode}
              language="rust"
              title="main.rs"
              showCopyButton
            />
          </div>

          {/* Documentation Link */}
          <div className="text-center pt-4">
            <a
              href="https://github.com/yetone/mirdb#readme"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 text-blue-600 dark:text-blue-400 hover:underline font-medium"
              data-testid="docs-link"
            >
              View Full Documentation
              <ExternalLink className="w-4 h-4" />
            </a>
          </div>
        </div>
      </div>
    </section>
  );
}

/**
 * Installation Section Component (React Island).
 * Owner: Scenario 6 - Installation Instructions Section
 *
 * Features:
 * - Prerequisites section with Rust toolchain requirement
 * - Build from source instructions with copy button
 * - Run with config instructions with copy button
 * - All code blocks have copy-to-clipboard functionality
 */
import { CopyButton } from '../Terminal/CopyButton';

interface CodeBlockWithCopyProps {
  code: string;
  language: string;
  label?: string;
}

function CodeBlockWithCopy({ code, language, label }: CodeBlockWithCopyProps) {
  return (
    <div
      className="code-block-wrapper relative bg-background border border-border rounded-lg overflow-hidden"
      data-testid={`code-block-${label || 'default'}`}
    >
      <div className="code-header flex items-center justify-between px-4 py-2 bg-surface border-b border-border">
        <span className="text-text-secondary text-xs font-mono">{language}</span>
        <CopyButton text={code} className="ml-auto" />
      </div>
      <pre className="code-block font-mono text-sm leading-relaxed overflow-x-auto p-4" data-language={language}>
        <code className="text-text-primary">{code}</code>
      </pre>
    </div>
  );
}

// Installation commands
const CLONE_COMMAND = `git clone https://github.com/lispking/mirdb.git
cd mirdb`;

const BUILD_COMMAND = `cargo build --release`;

const RUN_COMMAND = `./target/release/mirdb --config mirdb.toml`;

const CONFIG_EXAMPLE = `# mirdb.toml
[server]
host = "127.0.0.1"
port = 11211

[storage]
data_dir = "./data"
memtable_size = "64M"`;

export function InstallationSection() {
  return (
    <div className="installation-section max-w-4xl mx-auto" data-testid="installation-section">
      {/* Prerequisites */}
      <div className="mb-12" data-testid="prerequisites-section">
        <h3 className="text-xl font-bold font-mono text-text-primary mb-4">Prerequisites</h3>
        <div className="bg-surface border border-border rounded-lg p-6">
          <p className="text-text-secondary mb-4">
            MirDB requires the Rust toolchain to build from source. Install Rust using rustup:
          </p>
          <a
            href="https://rustup.rs"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center text-accent hover:underline font-mono"
            data-testid="rustup-link"
          >
            <svg
              className="w-4 h-4 mr-2"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"
              />
            </svg>
            https://rustup.rs
          </a>
          <p className="text-text-secondary mt-4 text-sm">
            Rust version 1.70 or later is recommended for optimal performance.
          </p>
        </div>
      </div>

      {/* Build from Source */}
      <div className="mb-12" data-testid="build-section">
        <h3 className="text-xl font-bold font-mono text-text-primary mb-4">Build from Source</h3>
        <div className="space-y-4">
          <div>
            <p className="text-text-secondary mb-3">1. Clone the repository:</p>
            <CodeBlockWithCopy code={CLONE_COMMAND} language="bash" label="clone" />
          </div>
          <div>
            <p className="text-text-secondary mb-3">2. Build in release mode:</p>
            <CodeBlockWithCopy code={BUILD_COMMAND} language="bash" label="build" />
          </div>
        </div>
      </div>

      {/* Run with Configuration */}
      <div className="mb-12" data-testid="run-section">
        <h3 className="text-xl font-bold font-mono text-text-primary mb-4">Run with Configuration</h3>
        <div className="space-y-4">
          <div>
            <p className="text-text-secondary mb-3">
              Create a configuration file (optional, defaults are used if not provided):
            </p>
            <CodeBlockWithCopy code={CONFIG_EXAMPLE} language="toml" label="config" />
          </div>
          <div>
            <p className="text-text-secondary mb-3">Start MirDB with your configuration:</p>
            <CodeBlockWithCopy code={RUN_COMMAND} language="bash" label="run" />
          </div>
        </div>
      </div>

      {/* Quick Start Note */}
      <div className="bg-surface/50 border border-border rounded-lg p-6" data-testid="quick-start-note">
        <div className="flex items-start">
          <svg
            className="w-6 h-6 text-success mr-3 flex-shrink-0 mt-0.5"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
            aria-hidden="true"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
          <div>
            <p className="text-text-primary font-semibold mb-1">You're ready!</p>
            <p className="text-text-secondary text-sm">
              MirDB is now running on port 11211. Connect using any Memcached-compatible client.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

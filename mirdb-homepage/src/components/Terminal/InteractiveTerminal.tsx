/**
 * Interactive Terminal Component (React Island).
 * Owner: Scenario 3 - Interactive Terminal Component
 *
 * Expected features:
 * - Tabbed interface (Basic Usage, Configuration, Advanced)
 * - Code display with syntax highlighting
 * - Copy-to-clipboard button per code block
 * - Terminal styling (dark background, monospace font)
 */
import { useState } from 'react';
import { TabPanel } from './TabPanel';
import { CopyButton } from './CopyButton';
import type { TerminalTab } from '../../types';

const TERMINAL_TABS: TerminalTab[] = [
  {
    id: 'basic',
    label: 'Basic Usage',
    language: 'shell',
    code: `# Connect to MirDB using telnet or nc
$ telnet localhost 11211

# Set a key-value pair
set mykey 0 0 5
hello
STORED

# Get a value by key
get mykey
VALUE mykey 0 5
hello
END

# Delete a key
delete mykey
DELETED`,
  },
  {
    id: 'config',
    label: 'Configuration',
    language: 'toml',
    code: `# mirdb.toml - MirDB Configuration

[server]
host = "127.0.0.1"
port = 11211

[storage]
data_dir = "/var/lib/mirdb"
memtable_size = "64M"
sstable_block_size = "4K"

[compaction]
minor_threshold = 4
major_threshold = 10
level_ratio = 10

[logging]
level = "info"
file = "/var/log/mirdb/mirdb.log"`,
  },
  {
    id: 'advanced',
    label: 'Advanced',
    language: 'shell',
    code: `# Batch operations with gets (CAS support)
gets key1 key2 key3
VALUE key1 0 5 12345
hello
VALUE key2 0 5 12346
world
END

# Append data to existing key
append mykey 0 0 6
 world
STORED

# Prepend data to existing key
prepend mykey 0 0 3
Hi
STORED

# Add (only if key doesn't exist)
add newkey 0 0 4
test
STORED

# Replace (only if key exists)
replace mykey 0 0 7
updated
STORED`,
  },
];

// Language-specific syntax highlighting patterns
const SYNTAX_PATTERNS: Record<string, Array<{ pattern: RegExp; className: string }>> = {
  shell: [
    { pattern: /(#[^\n]*)/g, className: 'text-text-secondary' }, // Comments
    { pattern: /(\$\s*\w+)/g, className: 'text-warning' }, // Shell prompt/command
    { pattern: /\b(set|get|gets|delete|append|prepend|add|replace)\b/gi, className: 'text-accent' }, // Memcached commands
    { pattern: /\b(STORED|DELETED|END|VALUE|NOT_FOUND|EXISTS|NOT_STORED)\b/g, className: 'text-success' }, // Responses
    { pattern: /(\d+)/g, className: 'text-warning' }, // Numbers
  ],
  toml: [
    { pattern: /(#[^\n]*)/g, className: 'text-text-secondary' }, // Comments
    { pattern: /\[([^\]]+)\]/g, className: 'text-accent' }, // Section headers
    { pattern: /\b(\w+)\s*=/g, className: 'text-success' }, // Keys
    { pattern: /("[^"]*")/g, className: 'text-warning' }, // Strings
    { pattern: /\b(\d+[KMGT]?)\b/g, className: 'text-warning' }, // Numbers with units
  ],
};

function highlightCode(code: string, language: string): string {
  const patterns = SYNTAX_PATTERNS[language] || [];
  let highlightedCode = code;

  // Simple HTML escaping
  highlightedCode = highlightedCode
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

  // Apply syntax highlighting patterns
  patterns.forEach(({ pattern, className }) => {
    highlightedCode = highlightedCode.replace(pattern, (match) => {
      // Skip if already wrapped in a span
      if (match.includes('<span')) return match;
      return `<span class="${className}">${match}</span>`;
    });
  });

  return highlightedCode;
}

export function InteractiveTerminal() {
  const [activeTab, setActiveTab] = useState(TERMINAL_TABS[0].id);

  const currentTab = TERMINAL_TABS.find((tab) => tab.id === activeTab) || TERMINAL_TABS[0];

  const tabs = TERMINAL_TABS.map((tab) => ({
    id: tab.id,
    label: tab.label,
  }));

  return (
    <div
      className="interactive-terminal max-w-4xl mx-auto bg-background border border-border rounded-lg overflow-hidden"
      data-testid="interactive-terminal"
    >
      <div className="terminal-header flex items-center justify-between px-4 py-2 bg-surface border-b border-border">
        <div className="flex items-center gap-2">
          <span className="w-3 h-3 rounded-full bg-red-500" aria-hidden="true" />
          <span className="w-3 h-3 rounded-full bg-yellow-500" aria-hidden="true" />
          <span className="w-3 h-3 rounded-full bg-green-500" aria-hidden="true" />
        </div>
        <span className="text-text-secondary text-sm font-mono">mirdb terminal</span>
      </div>

      <TabPanel tabs={tabs} activeTab={activeTab} onTabChange={setActiveTab}>
        <div className="relative">
          <div className="absolute top-2 right-2 z-10">
            <CopyButton text={currentTab.code} />
          </div>
          <pre
            className="code-block font-mono text-sm leading-relaxed overflow-x-auto p-4 pr-24 bg-background rounded"
            data-testid="code-block"
            data-language={currentTab.language}
          >
            <code
              className="syntax-highlighted"
              data-testid="highlighted-code"
              dangerouslySetInnerHTML={{ __html: highlightCode(currentTab.code, currentTab.language) }}
            />
          </pre>
        </div>
      </TabPanel>
    </div>
  );
}

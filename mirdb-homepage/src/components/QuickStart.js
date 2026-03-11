/**
 * Quick Start Guide Component
 * Owner: Scenario 4 - Quick Start Guide Section
 *
 * Expected exports:
 * - QuickStart(): Renders quick start section
 * - InstallStep({ platform, command }): Platform-specific install step
 * - getInstallCommands(): Get array of installation commands
 * - getConfigExample(): Get configuration file example
 *
 * Requirements:
 * - Installation instructions (cargo install or build from source)
 * - Server start command
 * - Client connection example (telnet/nc localhost:12333)
 * - Example mirdb.toml configuration
 * - Link to full documentation
 * - Copy buttons for all code blocks
 */

import { CopyButton } from './CopyButton.js'

/**
 * Installation commands for different platforms/methods
 */
const installCommands = [
  {
    id: 'cargo',
    platform: 'cargo',
    label: 'Using Cargo (Recommended)',
    command: 'cargo install mirdb',
    description: 'Install from crates.io using Cargo package manager'
  },
  {
    id: 'source',
    platform: 'source',
    label: 'Build from Source',
    command: 'git clone https://github.com/transybao93/mirdb.git\ncd mirdb\ncargo build --release',
    description: 'Clone the repository and build locally'
  }
]

/**
 * Example configuration file content
 */
const configExample = `# mirdb.toml - MirDB Configuration File

# Server settings
[server]
port = 12333
host = "127.0.0.1"

# Storage settings
[storage]
data_dir = "./data"
memtable_max_size = 4194304  # 4MB
sstable_max_size = 104857600  # 100MB

# LSM Tree settings
[lsm]
max_levels = 7
block_size = 4096  # 4KB`

/**
 * Get installation commands data
 * @returns {Array} Array of installation command objects
 */
export function getInstallCommands() {
  return installCommands
}

/**
 * Get example configuration file content
 * @returns {string} Configuration file content
 */
export function getConfigExample() {
  return configExample
}

/**
 * Escape HTML special characters
 * @param {string} str - String to escape
 * @returns {string} Escaped string
 */
function escapeHtml(str) {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;')
}

/**
 * Renders a code block with syntax highlighting and copy button
 * @param {Object} props - Code block properties
 * @param {string} props.code - Code content
 * @param {string} props.id - Unique identifier
 * @param {string} [props.language='bash'] - Language for highlighting
 * @returns {string} HTML string for the code block
 */
function CodeBlock({ code, id, language = 'bash' }) {
  const escapedCode = escapeHtml(code)

  return `
    <div class="code-block relative group" data-testid="code-block-${id}" data-language="${language}">
      <pre class="bg-gray-900 text-gray-100 rounded-lg p-4 overflow-x-auto font-mono text-sm leading-relaxed"><code class="language-${language}">${escapedCode}</code></pre>
      <div class="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity">
        ${CopyButton({ text: code, id, label: `Copy ${id} command` })}
      </div>
    </div>
  `
}

/**
 * Renders a platform-specific installation step
 * @param {Object} props - Install step properties
 * @param {string} props.platform - Platform identifier
 * @param {string} props.command - Installation command
 * @param {string} [props.label] - Display label
 * @param {string} [props.description] - Step description
 * @returns {string} HTML string for the install step
 */
export function InstallStep({ platform, command, label = '', description = '' }) {
  return `
    <div class="install-step" data-testid="install-step-${platform}">
      ${label ? `<h4 class="text-lg font-semibold text-gray-900 dark:text-white mb-2">${escapeHtml(label)}</h4>` : ''}
      ${description ? `<p class="text-gray-600 dark:text-gray-400 text-sm mb-3">${escapeHtml(description)}</p>` : ''}
      ${CodeBlock({ code: command, id: `install-${platform}`, language: 'bash' })}
    </div>
  `
}

/**
 * Renders the complete quick start guide section
 * @returns {string} HTML string for the quick start section
 */
export function QuickStart() {
  const installSteps = installCommands.map(cmd => InstallStep({
    platform: cmd.platform,
    command: cmd.command,
    label: cmd.label,
    description: cmd.description
  })).join('')

  return `
    <section
      id="quickstart"
      class="quickstart-section py-16 bg-gray-50 dark:bg-gray-900"
      data-testid="quickstart-section"
      aria-labelledby="quickstart-heading"
    >
      <div class="container-custom max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <!-- Section Header -->
        <div class="text-center mb-12">
          <h2
            id="quickstart-heading"
            class="text-3xl md:text-4xl font-bold text-gray-900 dark:text-white mb-4"
          >
            Quick Start Guide
          </h2>
          <p class="text-lg text-gray-600 dark:text-gray-400 max-w-2xl mx-auto">
            Get MirDB up and running in minutes. Follow these simple steps to install, configure, and start using MirDB.
          </p>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
          <!-- Left Column: Installation & Start -->
          <div class="space-y-8">
            <!-- Installation Section -->
            <div data-testid="quickstart-install" class="bg-white dark:bg-gray-800 rounded-xl shadow-md p-6">
              <h3 class="text-xl font-bold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                <span class="flex items-center justify-center w-8 h-8 rounded-full bg-blue-100 dark:bg-blue-900 text-blue-600 dark:text-blue-400 text-sm font-bold">1</span>
                Installation
              </h3>

              <div class="space-y-6">
                ${CodeBlock({ code: 'cargo install mirdb', id: 'install', language: 'bash' })}

                <details class="mt-4">
                  <summary class="cursor-pointer text-sm text-blue-600 dark:text-blue-400 hover:underline">
                    Alternative installation methods
                  </summary>
                  <div class="mt-4 space-y-4 pl-4 border-l-2 border-gray-200 dark:border-gray-700">
                    ${InstallStep({
                      platform: 'source',
                      command: 'git clone https://github.com/transybao93/mirdb.git\ncd mirdb\ncargo build --release',
                      label: 'Build from Source',
                      description: 'Clone and build locally for development'
                    })}
                  </div>
                </details>
              </div>
            </div>

            <!-- Start Server Section -->
            <div data-testid="quickstart-usage" class="bg-white dark:bg-gray-800 rounded-xl shadow-md p-6">
              <h3 class="text-xl font-bold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                <span class="flex items-center justify-center w-8 h-8 rounded-full bg-blue-100 dark:bg-blue-900 text-blue-600 dark:text-blue-400 text-sm font-bold">2</span>
                Start the Server
              </h3>

              <p class="text-gray-600 dark:text-gray-400 mb-4">
                Start MirDB with default configuration:
              </p>
              ${CodeBlock({ code: 'mirdb', id: 'server-start', language: 'bash' })}

              <p class="text-gray-600 dark:text-gray-400 mt-4 mb-4">
                Or specify a custom configuration file:
              </p>
              ${CodeBlock({ code: 'mirdb --config mirdb.toml', id: 'server-config', language: 'bash' })}
            </div>

            <!-- Connect Section -->
            <div class="bg-white dark:bg-gray-800 rounded-xl shadow-md p-6">
              <h3 class="text-xl font-bold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                <span class="flex items-center justify-center w-8 h-8 rounded-full bg-blue-100 dark:bg-blue-900 text-blue-600 dark:text-blue-400 text-sm font-bold">3</span>
                Connect to MirDB
              </h3>

              <p class="text-gray-600 dark:text-gray-400 mb-4">
                Connect using telnet or netcat (nc) to localhost:12333:
              </p>
              ${CodeBlock({ code: 'telnet localhost 12333', id: 'connect', language: 'bash' })}

              <p class="text-gray-600 dark:text-gray-400 mt-4 mb-4">
                Or use netcat:
              </p>
              ${CodeBlock({ code: 'nc localhost 12333', id: 'connect-nc', language: 'bash' })}
            </div>
          </div>

          <!-- Right Column: Configuration -->
          <div class="space-y-8">
            <!-- Configuration Section -->
            <div class="bg-white dark:bg-gray-800 rounded-xl shadow-md p-6">
              <h3 class="text-xl font-bold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                <svg class="w-6 h-6 text-blue-600 dark:text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"></path>
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"></path>
                </svg>
                Configuration (mirdb.toml)
              </h3>

              <p class="text-gray-600 dark:text-gray-400 mb-4">
                Create a configuration file to customize MirDB settings:
              </p>
              ${CodeBlock({ code: configExample, id: 'config', language: 'toml' })}

              <div class="mt-4 p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                <h4 class="text-sm font-semibold text-blue-800 dark:text-blue-300 mb-2">Key Configuration Options</h4>
                <ul class="text-sm text-blue-700 dark:text-blue-400 space-y-1">
                  <li><code class="bg-blue-100 dark:bg-blue-900 px-1 rounded">port</code> - Server listening port (default: 12333)</li>
                  <li><code class="bg-blue-100 dark:bg-blue-900 px-1 rounded">data_dir</code> - Data storage directory</li>
                  <li><code class="bg-blue-100 dark:bg-blue-900 px-1 rounded">memtable_max_size</code> - In-memory buffer size before flush</li>
                  <li><code class="bg-blue-100 dark:bg-blue-900 px-1 rounded">max_levels</code> - LSM Tree compaction levels</li>
                </ul>
              </div>
            </div>

            <!-- Basic Usage Example -->
            <div class="bg-white dark:bg-gray-800 rounded-xl shadow-md p-6">
              <h3 class="text-xl font-bold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                <svg class="w-6 h-6 text-blue-600 dark:text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path>
                </svg>
                Basic Usage
              </h3>

              <p class="text-gray-600 dark:text-gray-400 mb-4">
                Once connected, use memcached protocol commands:
              </p>
              ${CodeBlock({ code: '# Store a value\nset mykey 0 0 5\nhello\n\n# Retrieve the value\nget mykey\n\n# Delete the key\ndelete mykey', id: 'basic-usage', language: 'bash' })}
            </div>

            <!-- Documentation Link -->
            <div class="bg-gradient-to-r from-blue-600 to-blue-700 rounded-xl shadow-md p-6 text-white">
              <h3 class="text-xl font-bold mb-4 flex items-center gap-2">
                <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253"></path>
                </svg>
                Full Documentation
              </h3>

              <p class="text-blue-100 mb-4">
                Explore the complete documentation for advanced features, API reference, and best practices.
              </p>

              <a
                href="https://github.com/transybao93/mirdb#readme"
                target="_blank"
                rel="noopener noreferrer"
                class="inline-flex items-center gap-2 bg-white text-blue-600 font-semibold px-6 py-3 rounded-lg hover:bg-blue-50 transition-colors duration-200"
                data-testid="quickstart-docs-link"
              >
                <span>Read the Documentation</span>
                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14 5l7 7m0 0l-7 7m7-7H3"></path>
                </svg>
              </a>
            </div>
          </div>
        </div>
      </div>
    </section>
  `
}

export default QuickStart

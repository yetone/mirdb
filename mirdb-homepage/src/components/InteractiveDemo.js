/**
 * Interactive Demo Component
 * Owner: Scenario 3 - Interactive Demo Section
 *
 * Displays interactive memcached protocol command examples with copy functionality.
 */

import { CopyButton } from './CopyButton.js'

/**
 * Command examples data for the interactive demo
 */
const commandExamples = [
  {
    id: 'set',
    name: 'SET',
    description: 'Store a key-value pair',
    command: 'set mykey 0 0 5\r\nmyval\r\n',
    response: 'STORED',
    syntax: 'set <key> <flags> <exptime> <bytes>\\r\\n<data>\\r\\n'
  },
  {
    id: 'get',
    name: 'GET',
    description: 'Retrieve a value by key',
    command: 'get mykey\r\n',
    response: 'VALUE mykey 0 5\r\nmyval\r\nEND',
    syntax: 'get <key>\\r\\n'
  },
  {
    id: 'delete',
    name: 'DELETE',
    description: 'Remove a key-value pair',
    command: 'delete mykey\r\n',
    response: 'DELETED',
    syntax: 'delete <key>\\r\\n'
  }
]

/**
 * Tokenize memcached protocol text for syntax highlighting
 * @param {string} text - Text to highlight
 * @param {boolean} isCommand - Whether this is a command (vs response)
 * @returns {string} HTML with syntax highlighting spans
 */
function highlightSyntax(text, isCommand = true) {
  // Escape HTML first
  let escaped = text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')

  if (isCommand) {
    // Process the command line by line to avoid issues with regex in HTML
    const lines = escaped.split(/(\r\n|\\r\\n)/)
    const processedLines = lines.map(line => {
      if (line === '\r\n' || line === '\\r\\n') {
        return '<span class="syntax-newline">\\r\\n</span>'
      }

      // First, highlight numbers (before adding spans to avoid matching class attrs)
      let processed = line.replace(/\b(\d+)\b/g, '<<NUM>>$1<</NUM>>')

      // Highlight command keywords
      processed = processed.replace(
        /^(set|get|delete|add|replace|append|prepend|cas|incr|decr|stats|version|quit)\b/i,
        '<<KW>>$1<</KW>>'
      )

      // Highlight key names (first word after command)
      processed = processed.replace(
        /(<<KW>>[^<]+<<\/KW>>)\s+(\w+)/,
        '$1 <<KEY>>$2<</KEY>>'
      )

      // Convert markers to actual HTML spans
      processed = processed
        .replace(/<<KW>>/g, '<span class="syntax-keyword">')
        .replace(/<<\/KW>>/g, '</span>')
        .replace(/<<KEY>>/g, '<span class="syntax-key">')
        .replace(/<<\/KEY>>/g, '</span>')
        .replace(/<<NUM>>/g, '<span class="syntax-number">')
        .replace(/<<\/NUM>>/g, '</span>')

      return processed
    })
    return processedLines.join('')
  } else {
    // Response highlighting
    const lines = escaped.split(/(\r\n|\\r\\n)/)
    const processedLines = lines.map(line => {
      if (line === '\r\n' || line === '\\r\\n') {
        return '<span class="syntax-newline">\\r\\n</span>'
      }

      // Highlight success responses
      if (/^(STORED|DELETED|OK)$/.test(line)) {
        return `<span class="syntax-success">${line}</span>`
      }

      // Highlight VALUE response header
      const valueMatch = line.match(/^(VALUE)\s+(\w+)\s+(\d+)\s+(\d+)/)
      if (valueMatch) {
        return `<span class="syntax-response">${valueMatch[1]}</span> <span class="syntax-key">${valueMatch[2]}</span> <span class="syntax-number">${valueMatch[3]}</span> <span class="syntax-number">${valueMatch[4]}</span>`
      }

      // Highlight END
      if (line === 'END') {
        return '<span class="syntax-response">END</span>'
      }

      return line
    })
    return processedLines.join('')
  }
}

/**
 * Renders a code block with syntax highlighting and copy button
 * @param {Object} props - Code block properties
 * @param {string} props.code - Code content
 * @param {string} props.id - Unique identifier
 * @param {string} [props.language='memcached'] - Language for highlighting
 * @param {boolean} [props.copyable=true] - Whether to show copy button
 * @param {boolean} [props.isCommand=true] - Whether this is a command (vs response)
 * @returns {string} HTML string for the code block
 */
export function CodeBlock({ code, id, language = 'memcached', copyable = true, isCommand = true }) {
  const highlightedCode = highlightSyntax(code, isCommand)
  const displayCode = code.replace(/\r\n/g, '\\r\\n')

  return `
    <div class="code-block relative group" data-testid="code-block-${id}" data-language="${language}">
      <pre class="syntax-highlighted bg-gray-900 text-gray-100 rounded-lg p-4 overflow-x-auto font-mono text-sm leading-relaxed"><code class="language-${language}">${highlightedCode}</code></pre>
      ${copyable ? `
        <div class="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity">
          ${CopyButton({ text: displayCode, id, label: `Copy ${id.toUpperCase()} command` })}
        </div>
      ` : ''}
    </div>
  `
}

/**
 * Renders a command example card with command and response
 * @param {Object} example - Command example data
 * @returns {string} HTML string for the command example card
 */
function CommandExample(example) {
  return `
    <article
      class="command-example bg-white dark:bg-gray-800 rounded-xl shadow-md p-6"
      data-testid="command-example-${example.id}"
      data-command-id="${example.id}"
    >
      <div class="flex items-center justify-between mb-4">
        <h3 class="command-name text-xl font-bold text-gray-900 dark:text-white">
          ${example.name}
        </h3>
        <span class="command-badge inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200">
          ${example.id.toUpperCase()}
        </span>
      </div>

      <p class="command-description text-gray-600 dark:text-gray-400 mb-4">
        ${example.description}
      </p>

      <div class="space-y-4">
        <div>
          <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
            Command
          </label>
          ${CodeBlock({ code: example.command, id: example.id, isCommand: true })}
        </div>

        <div>
          <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
            Response
          </label>
          ${CodeBlock({ code: example.response, id: `${example.id}-response`, copyable: false, isCommand: false })}
        </div>
      </div>
    </article>
  `
}

/**
 * Renders the complete interactive demo section
 * @returns {string} HTML string for the interactive demo section
 */
export function InteractiveDemo() {
  const examples = commandExamples.map(CommandExample).join('')

  return `
    <section
      id="interactive-demo"
      class="interactive-demo-section py-16 bg-gray-50 dark:bg-gray-900"
      data-testid="interactive-demo"
      aria-labelledby="demo-heading"
    >
      <div class="container-custom max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="text-center mb-12">
          <h2
            id="demo-heading"
            class="demo-title text-3xl md:text-4xl font-bold text-gray-900 dark:text-white mb-4"
          >
            Try It Out
          </h2>
          <p class="demo-subtitle text-lg text-gray-600 dark:text-gray-400 max-w-2xl mx-auto">
            MirDB uses the memcached text protocol. Here are examples of common key-value operations
            you can perform. Click the copy button to try them yourself.
          </p>
        </div>

        <div
          class="command-grid grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
          data-testid="command-grid"
          role="list"
          aria-label="Command examples"
        >
          ${examples}
        </div>

        <div class="mt-8 text-center">
          <p class="text-sm text-gray-500 dark:text-gray-400">
            Connect to MirDB using <code class="px-2 py-1 bg-gray-200 dark:bg-gray-700 rounded">telnet localhost 12333</code>
            or <code class="px-2 py-1 bg-gray-200 dark:bg-gray-700 rounded">nc localhost 12333</code>
          </p>
        </div>
      </div>
    </section>
  `
}

/**
 * Get the command examples data (useful for testing)
 * @returns {Array} Array of command example objects
 */
export function getCommandExamples() {
  return commandExamples
}

export default InteractiveDemo

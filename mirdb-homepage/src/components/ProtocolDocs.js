/**
 * Protocol Documentation Preview Component
 * Owner: Scenario 5 - Protocol Documentation Preview
 *
 * Displays memcached protocol commands and response codes reference.
 */

import commandsData from '../data/commands.json'

/**
 * Renders a single command row in the commands table
 * @param {Object} command - Command data
 * @param {string} command.name - Command name
 * @param {string} command.syntax - Command syntax
 * @param {string} command.description - Command description
 * @param {string} [command.response] - Optional response format
 * @returns {string} HTML string for the command row
 */
export function CommandRow({ name, syntax, description, response }) {
  return `
    <tr class="command-row border-b border-gray-200 dark:border-gray-700" data-testid="command-row-${name.toLowerCase()}">
      <td class="command-name py-3 px-4 font-mono font-semibold text-primary-600 dark:text-primary-400">
        ${name}
      </td>
      <td class="command-syntax py-3 px-4">
        <code class="bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded text-sm font-mono text-gray-800 dark:text-gray-200 whitespace-pre-wrap break-all">
          ${syntax}
        </code>
        ${response ? `
          <div class="command-response mt-2">
            <span class="text-xs text-gray-500 dark:text-gray-400">Response:</span>
            <code class="bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded text-xs font-mono text-gray-800 dark:text-gray-200 block mt-1 whitespace-pre-wrap break-all">
              ${response}
            </code>
          </div>
        ` : ''}
      </td>
      <td class="command-description py-3 px-4 text-gray-600 dark:text-gray-300 text-sm">
        ${description}
      </td>
    </tr>
  `
}

/**
 * Renders the command reference table with all supported commands
 * @param {Array} commands - Array of command objects
 * @returns {string} HTML string for the command reference table
 */
export function CommandReference({ commands }) {
  const commandRows = commands.map(cmd => CommandRow(cmd)).join('')

  return `
    <div class="command-reference mb-12" data-testid="command-reference">
      <h3 class="text-xl font-semibold text-gray-900 dark:text-white mb-4">
        Supported Commands
      </h3>
      <div class="overflow-x-auto">
        <table class="command-table w-full border-collapse" data-testid="command-table">
          <thead>
            <tr class="bg-gray-100 dark:bg-gray-700 text-left">
              <th class="py-3 px-4 font-semibold text-gray-700 dark:text-gray-200 w-24">Command</th>
              <th class="py-3 px-4 font-semibold text-gray-700 dark:text-gray-200">Syntax</th>
              <th class="py-3 px-4 font-semibold text-gray-700 dark:text-gray-200 w-1/3">Description</th>
            </tr>
          </thead>
          <tbody data-testid="command-list">
            ${commandRows}
          </tbody>
        </table>
      </div>
    </div>
  `
}

/**
 * Renders a single response code row
 * @param {Object} responseCode - Response code data
 * @param {string} responseCode.code - Response code
 * @param {string} responseCode.description - Response code description
 * @returns {string} HTML string for the response code row
 */
export function ResponseCodeRow({ code, description }) {
  return `
    <tr class="response-code-row border-b border-gray-200 dark:border-gray-700" data-testid="response-code-${code.toLowerCase()}">
      <td class="response-code py-2 px-4 font-mono font-semibold text-green-600 dark:text-green-400">
        ${code}
      </td>
      <td class="response-description py-2 px-4 text-gray-600 dark:text-gray-300 text-sm">
        ${description}
      </td>
    </tr>
  `
}

/**
 * Renders the response codes reference table
 * @param {Array} responseCodes - Array of response code objects
 * @returns {string} HTML string for the response codes table
 */
export function ResponseCodes({ responseCodes }) {
  const responseCodeRows = responseCodes.map(rc => ResponseCodeRow(rc)).join('')

  return `
    <div class="response-codes" data-testid="response-codes">
      <h3 class="text-xl font-semibold text-gray-900 dark:text-white mb-4">
        Response Codes
      </h3>
      <div class="overflow-x-auto">
        <table class="response-codes-table w-full border-collapse" data-testid="response-codes-table">
          <thead>
            <tr class="bg-gray-100 dark:bg-gray-700 text-left">
              <th class="py-2 px-4 font-semibold text-gray-700 dark:text-gray-200 w-32">Code</th>
              <th class="py-2 px-4 font-semibold text-gray-700 dark:text-gray-200">Description</th>
            </tr>
          </thead>
          <tbody data-testid="response-codes-list">
            ${responseCodeRows}
          </tbody>
        </table>
      </div>
    </div>
  `
}

/**
 * Renders the complete protocol documentation preview section
 * @returns {string} HTML string for the protocol documentation section
 */
export function ProtocolDocs() {
  const { commands, responseCodes } = commandsData

  return `
    <div class="container-custom max-w-7xl mx-auto px-4 sm:px-6 lg:px-8" data-testid="protocol-docs">
      <div class="text-center mb-12">
        <h2 class="section-title text-3xl md:text-4xl font-bold text-gray-900 dark:text-white mb-4">
          Protocol Documentation
        </h2>
        <p class="section-subtitle text-lg text-gray-600 dark:text-gray-400 max-w-2xl mx-auto">
          MirDB implements the memcached text protocol, providing full compatibility with existing clients and tools.
        </p>
      </div>

      ${CommandReference({ commands })}
      ${ResponseCodes({ responseCodes })}

      <div class="mt-8 text-center">
        <a
          href="https://github.com/memcached/memcached/blob/master/doc/protocol.txt"
          target="_blank"
          rel="noopener noreferrer"
          class="inline-flex items-center text-primary-600 dark:text-primary-400 hover:underline"
        >
          View full memcached protocol specification
          <svg class="w-4 h-4 ml-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"></path>
          </svg>
        </a>
      </div>
    </div>
  `
}

export default ProtocolDocs

/**
 * Footer Component
 * Owner: Scenario 9 - Footer Component
 *
 * Displays the page footer with links to GitHub, documentation,
 * license information, and copyright notice.
 */

/**
 * GitHub icon SVG
 * @returns {string} SVG string for GitHub icon
 */
const githubIcon = `
  <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true">
    <path fill-rule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clip-rule="evenodd"></path>
  </svg>
`

/**
 * Documentation icon SVG
 * @returns {string} SVG string for documentation icon
 */
const docsIcon = `
  <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253"></path>
  </svg>
`

/**
 * Community icon SVG
 * @returns {string} SVG string for community icon
 */
const communityIcon = `
  <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z"></path>
  </svg>
`

/**
 * Get the current year for copyright notice
 * @returns {number} Current year
 */
export function getCurrentYear() {
  return new Date().getFullYear()
}

/**
 * Renders the complete footer with links, license, and copyright
 * @returns {string} HTML string for the footer
 */
export function Footer() {
  const currentYear = getCurrentYear()

  return `
    <div
      class="footer-content bg-gray-100 dark:bg-gray-800 border-t border-gray-200 dark:border-gray-700"
      data-testid="footer-section"
    >
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <!-- Footer Links -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-8 mb-8">
          <!-- Resources -->
          <div>
            <h3 class="text-sm font-semibold text-gray-900 dark:text-white uppercase tracking-wider mb-4">
              Resources
            </h3>
            <ul class="space-y-3">
              <li>
                <a
                  href="https://github.com/transybao93/mirdb"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="flex items-center gap-2 text-gray-600 dark:text-gray-400 hover:text-blue-600 dark:hover:text-blue-400 transition-colors"
                  data-testid="footer-github-link"
                >
                  ${githubIcon}
                  <span>GitHub Repository</span>
                </a>
              </li>
              <li>
                <a
                  href="https://github.com/transybao93/mirdb#readme"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="flex items-center gap-2 text-gray-600 dark:text-gray-400 hover:text-blue-600 dark:hover:text-blue-400 transition-colors"
                  data-testid="footer-documentation-link"
                >
                  ${docsIcon}
                  <span>Documentation</span>
                </a>
              </li>
              <li>
                <a
                  href="https://github.com/transybao93/mirdb/discussions"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="flex items-center gap-2 text-gray-600 dark:text-gray-400 hover:text-blue-600 dark:hover:text-blue-400 transition-colors"
                  data-testid="footer-community-link"
                >
                  ${communityIcon}
                  <span>Community</span>
                </a>
              </li>
            </ul>
          </div>

          <!-- About MirDB -->
          <div>
            <h3 class="text-sm font-semibold text-gray-900 dark:text-white uppercase tracking-wider mb-4">
              About MirDB
            </h3>
            <p class="text-gray-600 dark:text-gray-400 text-sm leading-relaxed">
              MirDB is a persistent key-value store with Memcached protocol compatibility.
              Built with Rust for performance and reliability, featuring LSM Tree architecture
              and SSTables for durable storage.
            </p>
          </div>

          <!-- Legal -->
          <div>
            <h3 class="text-sm font-semibold text-gray-900 dark:text-white uppercase tracking-wider mb-4">
              Legal
            </h3>
            <p
              class="text-gray-600 dark:text-gray-400 text-sm mb-2"
              data-testid="footer-license"
            >
              Released under the <strong class="font-medium">MIT License</strong>
            </p>
            <a
              href="https://github.com/transybao93/mirdb/blob/main/LICENSE"
              target="_blank"
              rel="noopener noreferrer"
              class="text-blue-600 dark:text-blue-400 hover:underline text-sm"
              data-testid="footer-license-link"
            >
              View License
            </a>
          </div>
        </div>

        <!-- Divider -->
        <div class="border-t border-gray-200 dark:border-gray-700 pt-8">
          <!-- Copyright -->
          <div class="flex flex-col md:flex-row justify-between items-center gap-4">
            <p
              class="text-gray-500 dark:text-gray-400 text-sm"
              data-testid="footer-copyright"
            >
              &copy; ${currentYear} MirDB. All rights reserved.
            </p>
            <div class="flex items-center gap-4">
              <span class="text-gray-400 dark:text-gray-500 text-xs">
                Built with ❤️ using Rust
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  `
}

export default Footer

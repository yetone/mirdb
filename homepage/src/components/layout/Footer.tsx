/**
 * Footer Component
 * Owner: Scenario 7 - Footer Display
 *
 * Expected exports:
 * - Footer: Page footer with copyright and links
 *
 * Requirements:
 * - Copyright text with current year
 * - MirDB name/logo
 * - GitHub repository link
 * - Additional navigation links
 * - Proper security attributes on external links
 */

export function Footer() {
  const currentYear = new Date().getFullYear()

  return (
    <footer data-testid="footer" className="bg-gray-900 text-white py-8">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex flex-col md:flex-row justify-between items-center">
          <div className="text-gray-400">
            © {currentYear} MirDB. All rights reserved.
          </div>
          <div className="flex space-x-6 mt-4 md:mt-0">
            <a
              href="https://github.com/yetone/mirdb"
              target="_blank"
              rel="noopener noreferrer"
              className="text-gray-400 hover:text-white"
            >
              GitHub
            </a>
          </div>
        </div>
      </div>
    </footer>
  )
}

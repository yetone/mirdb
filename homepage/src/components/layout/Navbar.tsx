/**
 * Navbar Component
 * Owner: Scenario 2 - Navbar & Logo Display
 *
 * Expected exports:
 * - Navbar: Navigation bar with logo, links, and theme toggle
 *
 * Requirements:
 * - Display MirDB logo (from assets/logo.gif)
 * - Show product name "MirDB"
 * - Navigation links: GitHub, Documentation, API Reference
 * - Theme toggle button
 * - Responsive mobile menu (hamburger)
 */

export function Navbar() {
  return (
    <nav data-testid="navbar" className="bg-white dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between h-16 items-center">
          <div className="flex items-center">
            <span className="text-xl font-bold text-primary-600">MirDB</span>
          </div>
          <div className="flex items-center space-x-4">
            <a href="https://github.com/yetone/mirdb" className="text-gray-600 dark:text-gray-300 hover:text-primary-600">GitHub</a>
          </div>
        </div>
      </div>
    </nav>
  )
}

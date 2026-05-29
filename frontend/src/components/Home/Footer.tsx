/**
 * Footer Component
 * Owner: Scenario 12 - Footer and Additional Content
 *
 * Simple footer with copyright information and placeholder links.
 *
 * Expected props:
 * - none
 *
 * Expected exports:
 * - Footer: React.FC component
 */

export default function Footer() {
  return (
    <footer
      data-testid="footer"
      className="footer mt-auto px-4 sm:px-6 lg:px-8 py-6 md:py-8 border-t border-gray-200 dark:border-gray-700"
    >
      <div className="max-w-7xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-4">
        <p className="text-sm opacity-70 text-center sm:text-left">
          &copy; {new Date().getFullYear()} URL Shortener. All rights reserved.
        </p>
        <nav aria-label="Footer navigation" className="flex flex-wrap items-center justify-center gap-4 sm:gap-6">
          <a
            href="#"
            className="text-sm opacity-70 hover:opacity-100 transition-opacity min-h-[44px] flex items-center px-2"
          >
            Terms
          </a>
          <a
            href="#"
            className="text-sm opacity-70 hover:opacity-100 transition-opacity min-h-[44px] flex items-center px-2"
          >
            Privacy
          </a>
          <a
            href="#"
            className="text-sm opacity-70 hover:opacity-100 transition-opacity min-h-[44px] flex items-center px-2"
          >
            Contact
          </a>
        </nav>
      </div>
    </footer>
  );
}

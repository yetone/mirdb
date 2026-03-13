/**
 * Hero Section Component
 * Owner: Scenario 3 - Hero Section Display
 *
 * Displays the main hero section with headline, subtitle, and CTA buttons.
 *
 * Requirements:
 * - H1: "Persistent Key-Value Store"
 * - Subtitle: "with Memcached Protocol"
 * - CTA buttons: "Get Started", "GitHub"
 * - Fully above-the-fold on 1366x768 screens
 */

import { GITHUB_URL, DOCS_URL } from '../../utils/constants';

export function Hero() {
  return (
    <section
      data-testid="hero"
      className="flex flex-col items-center justify-center min-h-[70vh] px-4 py-16 text-center bg-gradient-to-b from-purple-50 to-white dark:from-gray-800 dark:to-gray-900"
      aria-labelledby="hero-heading"
    >
      <h1
        id="hero-heading"
        className="text-4xl md:text-5xl lg:text-6xl font-bold text-gray-900 dark:text-white mb-4"
      >
        Persistent Key-Value Store
      </h1>
      <p className="text-xl md:text-2xl text-gray-600 dark:text-gray-300 mb-8 max-w-2xl">
        with Memcached Protocol
      </p>
      <div className="flex flex-col sm:flex-row gap-4">
        <a
          href={DOCS_URL}
          className="inline-flex items-center justify-center px-8 py-3 text-lg font-semibold text-white bg-purple-600 rounded-lg hover:bg-purple-700 focus:outline-none focus:ring-2 focus:ring-purple-500 focus:ring-offset-2 transition-colors"
        >
          Get Started
        </a>
        <a
          href={GITHUB_URL}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center justify-center px-8 py-3 text-lg font-semibold text-gray-700 bg-white border-2 border-gray-300 rounded-lg hover:bg-gray-50 hover:border-gray-400 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-offset-2 transition-colors dark:bg-gray-800 dark:text-gray-200 dark:border-gray-600 dark:hover:bg-gray-700"
        >
          <svg
            className="w-5 h-5 mr-2"
            fill="currentColor"
            viewBox="0 0 24 24"
            aria-hidden="true"
          >
            <path
              fillRule="evenodd"
              d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z"
              clipRule="evenodd"
            />
          </svg>
          GitHub
        </a>
      </div>
    </section>
  );
}

export default Hero;

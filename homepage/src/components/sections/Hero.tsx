/**
 * Hero Section Component
 * Owner: Scenario 3 - Hero Section Display
 *
 * Expected exports:
 * - Hero: Hero section with headline, subtitle, and CTA buttons
 *
 * Requirements:
 * - H1: "Persistent Key-Value Store"
 * - Subtitle: "with Memcached Protocol"
 * - CTA buttons: "Get Started", "GitHub"
 * - Fully above-the-fold on 1366x768 screens
 */

export function Hero() {
  return (
    <section data-testid="hero" className="bg-gradient-to-br from-primary-50 to-white dark:from-gray-900 dark:to-gray-800 py-20">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
        <h1 className="text-4xl md:text-6xl font-bold text-gray-900 dark:text-white mb-4">
          Persistent Key-Value Store
        </h1>
        <p className="text-xl text-gray-600 dark:text-gray-300 mb-8">
          with Memcached Protocol
        </p>
        <div className="flex justify-center space-x-4">
          <button className="bg-primary-600 text-white px-6 py-3 rounded-lg hover:bg-primary-700">
            Get Started
          </button>
          <a href="https://github.com/yetone/mirdb" className="border border-primary-600 text-primary-600 px-6 py-3 rounded-lg hover:bg-primary-50">
            GitHub
          </a>
        </div>
      </div>
    </section>
  )
}

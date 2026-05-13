import { GITHUB_REPO_URL } from '../../utils/constants';

export default function Hero() {
  return (
    <section
      id="hero"
      aria-label="Hero section"
      className="relative min-h-screen w-full flex items-center justify-center bg-gradient-to-br from-blue-50 via-white to-amber-50 dark:from-gray-900 dark:via-gray-950 dark:to-gray-900"
    >
      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-20 text-center">
        <img
          src="/assets/logo.gif"
          alt="MirDB Logo"
          className="mx-auto mb-8 w-24 h-24 sm:w-28 sm:h-28"
          width="112"
          height="112"
        />

        <h1 className="text-4xl sm:text-5xl lg:text-6xl font-bold tracking-tight text-gray-900 dark:text-white mb-4">
          MirDB
        </h1>

        <p className="text-lg sm:text-xl lg:text-2xl text-gray-600 dark:text-gray-300 mb-10 max-w-2xl mx-auto">
          A Persistent Key-Value Store with Memcached Protocol
        </p>

        <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
          <a
            href="#quickstart"
            className="inline-flex items-center justify-center px-8 py-3 text-base font-medium rounded-lg text-white bg-brand-600 hover:bg-brand-700 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-brand-500 transition-colors"
            aria-label="Get Started - scroll to Quick Start section"
          >
            Get Started
          </a>

          <a
            href={GITHUB_REPO_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center justify-center px-8 py-3 text-base font-medium rounded-lg text-gray-700 dark:text-gray-200 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-brand-500 transition-colors"
            aria-label="View MirDB on GitHub - opens in new tab"
          >
            View on GitHub
          </a>
        </div>
      </div>
    </section>
  );
}

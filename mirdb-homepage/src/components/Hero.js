/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Displays the main hero section with product name, tagline, value proposition,
 * CTA buttons, and branding elements.
 */

/**
 * MirDB Logo SVG inline for the hero section
 * Uses the same gradient as the logo.svg file
 */
const logoSvg = `
  <svg
    class="w-24 h-24 md:w-32 md:h-32"
    viewBox="0 0 200 200"
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
    aria-label="MirDB Logo"
    role="img"
  >
    <title>MirDB Logo</title>
    <defs>
      <linearGradient id="heroLogoGradient" x1="0%" y1="0%" x2="100%" y2="100%">
        <stop offset="0%" style="stop-color:#3b82f6;stop-opacity:1" />
        <stop offset="100%" style="stop-color:#1d4ed8;stop-opacity:1" />
      </linearGradient>
    </defs>
    <circle cx="100" cy="100" r="90" fill="url(#heroLogoGradient)"/>
    <path d="M60 70 L100 50 L140 70 L140 130 L100 150 L60 130 Z" fill="white" opacity="0.9"/>
    <path d="M60 70 L100 90 L140 70" stroke="white" stroke-width="2" fill="none"/>
    <path d="M100 90 L100 150" stroke="white" stroke-width="2" fill="none"/>
    <text x="100" y="115" font-family="Arial, sans-serif" font-size="24" font-weight="bold" fill="#1d4ed8" text-anchor="middle">DB</text>
  </svg>
`

/**
 * Renders the CTA buttons for the hero section
 * @returns {string} HTML string for the CTA buttons container
 */
export function renderCTAButtons() {
  return `
    <div
      class="flex flex-col sm:flex-row gap-4 justify-center items-center mt-8"
      data-testid="hero-cta-container"
    >
      <a
        href="#quickstart"
        class="btn-primary bg-blue-600 hover:bg-blue-700 text-white font-semibold px-8 py-3 rounded-lg transition-colors duration-200 shadow-lg hover:shadow-xl flex items-center gap-2"
        data-testid="hero-cta-get-started"
      >
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path>
        </svg>
        Get Started
      </a>
      <a
        href="https://github.com/transybao93/mirdb"
        target="_blank"
        rel="noopener noreferrer"
        class="btn-secondary bg-gray-100 dark:bg-gray-800 hover:bg-gray-200 dark:hover:bg-gray-700 text-gray-900 dark:text-white font-semibold px-8 py-3 rounded-lg transition-colors duration-200 border border-gray-300 dark:border-gray-600 flex items-center gap-2"
        data-testid="hero-cta-github"
      >
        <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24">
          <path fill-rule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clip-rule="evenodd"></path>
        </svg>
        View on GitHub
      </a>
    </div>
  `
}

/**
 * Renders the complete hero section with product branding, value proposition, and CTAs
 * @returns {string} HTML string for the hero section
 */
export function Hero() {
  return `
    <div
      class="hero-section container-custom max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-20 text-center"
      data-testid="hero-section"
      aria-label="MirDB Hero Section"
    >
      <!-- Logo/Branding Element -->
      <div
        class="flex justify-center mb-8"
        data-testid="hero-logo"
      >
        ${logoSvg}
      </div>

      <!-- Product Name -->
      <h1
        class="text-5xl md:text-6xl lg:text-7xl font-bold text-gray-900 dark:text-white mb-6 tracking-tight"
        data-testid="hero-product-name"
      >
        MirDB
      </h1>

      <!-- Tagline -->
      <p
        class="text-xl md:text-2xl text-gray-600 dark:text-gray-300 mb-4 max-w-3xl mx-auto"
        data-testid="hero-tagline"
      >
        A persistent key-value store with Memcached protocol compatibility
      </p>

      <!-- Value Proposition -->
      <p
        class="text-lg md:text-xl text-gray-500 dark:text-gray-400 mb-8 max-w-2xl mx-auto leading-relaxed"
        data-testid="hero-value-proposition"
      >
        Get the simplicity of Memcached with the durability you need. MirDB persists your data to disk
        while maintaining full compatibility with the Memcached text protocol. Built with Rust for
        performance and reliability.
      </p>

      <!-- CTA Buttons -->
      ${renderCTAButtons()}

      <!-- Additional Info -->
      <div class="mt-12 flex flex-wrap justify-center gap-6 text-sm text-gray-500 dark:text-gray-400">
        <div class="flex items-center gap-2">
          <svg class="w-5 h-5 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
          </svg>
          <span>LSM Tree Architecture</span>
        </div>
        <div class="flex items-center gap-2">
          <svg class="w-5 h-5 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
          </svg>
          <span>SSTables for Persistence</span>
        </div>
        <div class="flex items-center gap-2">
          <svg class="w-5 h-5 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
          </svg>
          <span>Open Source</span>
        </div>
      </div>
    </div>
  `
}

export default Hero

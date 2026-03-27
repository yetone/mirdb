/**
 * PostCSS Configuration
 * Owner: Scenario 17 - Performance - Page Load
 *
 * Plugins:
 * - tailwindcss: CSS framework with utility classes
 * - autoprefixer: Vendor prefix management
 * - cssnano: Advanced CSS minification (production only)
 *
 * Note: Additional minification via cssnano for production builds
 * provides extra optimizations beyond Tailwind's --minify flag
 */
module.exports = {
  plugins: {
    tailwindcss: {},
    autoprefixer: {},
    ...(process.env.NODE_ENV === 'production' ? {
      cssnano: {
        preset: ['default', {
          // Disable calc minification to preserve browser compatibility
          calc: false,
          // Preserve license comments
          discardComments: { removeAll: false }
        }]
      }
    } : {})
  },
}

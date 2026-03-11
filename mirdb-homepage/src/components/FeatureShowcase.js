/**
 * Feature Showcase Component
 * Owner: Scenario 2 - Feature Showcase Section
 *
 * Displays MirDB's key features in an appealing grid layout with icons and descriptions.
 */

import features from '../data/features.json'

/**
 * SVG icons for feature cards
 */
const icons = {
  protocol: `<svg class="feature-icon w-12 h-12 text-primary-600 dark:text-primary-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"></path>
  </svg>`,
  storage: `<svg class="feature-icon w-12 h-12 text-primary-600 dark:text-primary-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"></path>
  </svg>`,
  tree: `<svg class="feature-icon w-12 h-12 text-primary-600 dark:text-primary-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"></path>
  </svg>`,
  list: `<svg class="feature-icon w-12 h-12 text-primary-600 dark:text-primary-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 10h16M4 14h16M4 18h16"></path>
  </svg>`,
  layers: `<svg class="feature-icon w-12 h-12 text-primary-600 dark:text-primary-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"></path>
  </svg>`
}

/**
 * Renders an individual feature card with icon, title, and description
 * @param {Object} props - Feature card properties
 * @param {string} props.id - Unique identifier for the feature
 * @param {string} props.icon - Icon key from the icons object
 * @param {string} props.title - Feature title
 * @param {string} props.description - Feature description
 * @returns {string} HTML string for the feature card
 */
export function FeatureCard({ id, icon, title, description }) {
  const iconSvg = icons[icon] || icons.protocol

  return `
    <article
      class="feature-card bg-white dark:bg-gray-800 rounded-xl shadow-md hover:shadow-lg transition-all duration-300 p-6 flex flex-col"
      data-feature-id="${id}"
      data-testid="feature-card-${id}"
    >
      <div class="feature-icon-wrapper mb-4">
        ${iconSvg}
      </div>
      <h3 class="feature-title text-xl font-semibold text-gray-900 dark:text-white mb-2">
        ${title}
      </h3>
      <p class="feature-description text-gray-600 dark:text-gray-300 text-sm leading-relaxed flex-grow">
        ${description}
      </p>
    </article>
  `
}

/**
 * Renders the complete feature showcase section with all features in a responsive grid
 * @returns {string} HTML string for the feature showcase section
 */
export function FeatureShowcase() {
  const featureCards = features.map(feature => FeatureCard(feature)).join('')

  return `
    <div class="container-custom max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      <div class="text-center mb-12">
        <h2 class="section-title text-3xl md:text-4xl font-bold text-gray-900 dark:text-white mb-4">
          Key Features
        </h2>
        <p class="section-subtitle text-lg text-gray-600 dark:text-gray-400 max-w-2xl mx-auto">
          MirDB combines the simplicity of memcached with the durability of persistent storage, powered by a modern LSM tree architecture.
        </p>
      </div>

      <div
        class="feature-grid grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
        data-testid="feature-grid"
        role="list"
        aria-label="MirDB Features"
      >
        ${featureCards}
      </div>
    </div>
  `
}

export default FeatureShowcase

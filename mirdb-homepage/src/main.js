// MirDB Homepage - Main Entry Point

// Import components
import { Hero } from './components/Hero.js'
import { FeatureShowcase } from './components/FeatureShowcase.js'

// Initialize components when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  // Render Hero Section
  const heroSection = document.getElementById('hero')
  if (heroSection) {
    heroSection.innerHTML = Hero()
  }

  // Render Feature Showcase
  const featuresSection = document.getElementById('features')
  if (featuresSection) {
    featuresSection.innerHTML = FeatureShowcase()
  }
})

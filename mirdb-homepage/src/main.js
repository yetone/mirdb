// MirDB Homepage - Main Entry Point

// Import components
import { FeatureShowcase } from './components/FeatureShowcase.js'

// Initialize components when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  // Render Feature Showcase
  const featuresSection = document.getElementById('features')
  if (featuresSection) {
    featuresSection.innerHTML = FeatureShowcase()
  }
})

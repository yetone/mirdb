// MirDB Homepage - Main Entry Point

// Import components
import { Hero } from './components/Hero.js'
import { FeatureShowcase } from './components/FeatureShowcase.js'
import { InteractiveDemo } from './components/InteractiveDemo.js'
import { initCopyButtons } from './components/CopyButton.js'
import { ArchitectureOverview } from './components/ArchitectureOverview.js'

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

  // Render Interactive Demo
  const demoSection = document.getElementById('demo')
  if (demoSection) {
    demoSection.innerHTML = InteractiveDemo()
  }

  // Render Architecture Overview
  const architectureSection = document.getElementById('architecture')
  if (architectureSection) {
    architectureSection.innerHTML = ArchitectureOverview()
  }

  // Initialize copy button event handlers
  initCopyButtons()
})

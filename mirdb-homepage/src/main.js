// MirDB Homepage - Main Entry Point

// Import components
import { Hero } from './components/Hero.js'
import { FeatureShowcase } from './components/FeatureShowcase.js'
import { InteractiveDemo } from './components/InteractiveDemo.js'
import { QuickStart } from './components/QuickStart.js'
import { ProtocolDocs } from './components/ProtocolDocs.js'
import { ArchitectureOverview } from './components/ArchitectureOverview.js'
import { PerformanceInfo } from './components/PerformanceInfo.js'
import { Footer } from './components/Footer.js'
import { initCopyButtons } from './components/CopyButton.js'

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

  // Render Quick Start Guide
  const quickstartSection = document.getElementById('quickstart')
  if (quickstartSection) {
    quickstartSection.innerHTML = QuickStart()
  }

  // Render Protocol Documentation
  const protocolSection = document.getElementById('protocol')
  if (protocolSection) {
    protocolSection.innerHTML = ProtocolDocs()
  }

  // Render Architecture Overview
  const architectureSection = document.getElementById('architecture')
  if (architectureSection) {
    architectureSection.innerHTML = ArchitectureOverview()
  }

  // Render Performance Information
  const performanceSection = document.getElementById('performance')
  if (performanceSection) {
    performanceSection.innerHTML = PerformanceInfo()
  }

  // Render Footer
  const footerSection = document.getElementById('footer')
  if (footerSection) {
    footerSection.innerHTML = Footer()
  }

  // Initialize copy button event handlers
  initCopyButtons()
})

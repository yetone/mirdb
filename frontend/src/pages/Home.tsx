/**
 * Homepage Component
 * Owner: Scenario 1 - Hero Section Rendering
 * Modified by: Scenario 5 - Theme Adaptation
 * Modified by: Scenario 6 - Responsive Design
 * Modified by: Scenario 11 - Component Integration
 *
 * Main landing page for the URL Shortener service.
 * Integrates all homepage sections and existing components.
 *
 * Requirements:
 * - REQ-1: Clear value proposition in first viewport
 * - REQ-7: Theme adaptation
 * - REQ-8: Responsive design
 * - NFR-1: WCAG 2.1 AA accessibility
 * - NFR-5: SEO optimization
 *
 * Integrated Components:
 * - Navbar (with ThemeToggle)
 * - BackgroundEffect
 * - HeroSection (with FuturisticButton)
 * - FeaturesSection (with GlassMorphismCard)
 * - FAQSection
 * - CTAFooter
 */
import { motion, AnimatePresence } from 'framer-motion'
import { HeroSection, CTAFooter, FeaturesSection, FAQSection } from '../components/home'
import { Navbar } from '../components/Navbar'
import { BackgroundEffect } from '../components/BackgroundEffect'
import { useTheme } from '../contexts/ThemeContext'
import type { HomePageProps } from '../types/home'

// Theme transition animation variants
const themeTransitionVariants = {
  initial: { opacity: 0.8 },
  animate: {
    opacity: 1,
    transition: {
      duration: 0.3,
      ease: 'easeOut',
    },
  },
  exit: { opacity: 0.8 },
}

export function Home({ showFeatures = true, showFAQ = true }: HomePageProps) {
  const { theme } = useTheme()

  return (
    <>
      {/* Background visual effects */}
      <BackgroundEffect variant="default" />

      {/* Site Header with Navigation */}
      <header data-testid="site-header" role="banner">
        <Navbar />
      </header>

      <AnimatePresence mode="wait">
        <motion.main
          key={theme}
          className="min-h-screen bg-base-100 pt-16 overflow-x-hidden transition-colors duration-300"
          data-testid="home-page"
          data-theme-active={theme}
          role="main"
          aria-label="Homepage"
          variants={themeTransitionVariants}
          initial="initial"
          animate="animate"
          exit="exit"
        >
          {/* Hero Section - First viewport content */}
          <HeroSection />

          {/* Features Section - Responsive grid layout */}
          {showFeatures && (
            <div id="features-section">
              <FeaturesSection className="px-4 sm:px-6 lg:px-8" />
            </div>
          )}

          {/* FAQ Section - Responsive accordion */}
          {showFAQ && (
            <div id="faq-section">
              <FAQSection className="px-4 sm:px-6 lg:px-8" />
            </div>
          )}

          {/* CTA Footer Section */}
          <CTAFooter />
        </motion.main>
      </AnimatePresence>

      {/* Site Footer with copyright and links */}
      <footer
        className="bg-base-200 py-8 px-4 sm:px-6 lg:px-8"
        data-testid="site-footer"
        role="contentinfo"
      >
        <div className="max-w-6xl mx-auto text-center text-base-content/60">
          <p>&copy; {new Date().getFullYear()} URL Shortener. All rights reserved.</p>
        </div>
      </footer>
    </>
  )
}

export default Home

/**
 * Homepage Integration
 * Owner: Scenario 10 - Homepage Integration
 *
 * Assembles all homepage sections in the correct order:
 * 1. Hero Section - Value proposition and primary CTAs
 * 2. Features Section - Key features grid
 * 3. How It Works Section - 3-step process explanation
 * 4. Stats Section - Trust indicators (optional)
 * 5. CTA Section - Final call-to-action
 * 6. Demo Input - URL input teaser (optional)
 *
 * Integrates with existing BackgroundEffect, ThemeContext, and routing.
 */

import { motion } from 'framer-motion';
import { BackgroundEffect } from '../components/BackgroundEffect';
import {
  HeroSection,
  FeaturesSection,
  HowItWorksSection,
  StatsSection,
  CTASection,
  DemoInput,
} from '../components/homepage';

/**
 * Animation variant for page sections
 * Provides smooth fade-in and slide-up effect
 */
const sectionVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      duration: 0.5,
      staggerChildren: 0.2,
    },
  },
};

export function Home() {
  return (
    <>
      {/* Background visual effects */}
      <BackgroundEffect />

      {/* Main content */}
      <motion.main
        className="min-h-screen"
        data-testid="homepage"
        initial="hidden"
        animate="visible"
        variants={sectionVariants}
      >
        {/* Hero Section - Primary value proposition */}
        <HeroSection />

        {/* Features Section - Key capabilities */}
        <FeaturesSection />

        {/* How It Works - Step-by-step process */}
        <HowItWorksSection />

        {/* Stats Section - Trust indicators (Could priority) */}
        <StatsSection />

        {/* Demo Input - URL shortening teaser (Could priority) */}
        <DemoInput />

        {/* CTA Section - Final call-to-action */}
        <CTASection />
      </motion.main>
    </>
  );
}

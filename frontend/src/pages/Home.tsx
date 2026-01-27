/**
 * Home Page Component
 * Owner: Scenario 5 - Homepage Integration
 *
 * Assembles all homepage sections with:
 * - Proper section order: Hero, Features, Analytics Preview, CTA
 * - Framer Motion scroll animations for section transitions
 * - Responsive layout with proper spacing
 *
 * Requirements covered: REQ-1 through REQ-9
 */

import React from 'react';
import { motion } from 'framer-motion';
import {
  HeroSection,
  FeaturesSection,
  AnalyticsPreview,
  CTASection,
} from '../components/homepage';

const sectionVariants = {
  hidden: {
    opacity: 0,
    y: 50,
  },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.6,
      ease: 'easeOut' as const,
    },
  },
};

interface AnimatedSectionProps {
  children: React.ReactNode;
  id: string;
}

const AnimatedSection: React.FC<AnimatedSectionProps> = ({ children, id }) => (
  <motion.div
    id={id}
    initial="hidden"
    whileInView="visible"
    viewport={{ once: true, margin: '-100px' }}
    variants={sectionVariants}
  >
    {children}
  </motion.div>
);

const Home: React.FC = () => {
  return (
    <main className="min-h-screen bg-base-100" data-testid="homepage">
      {/* Hero Section - Full viewport, no animation delay */}
      <AnimatedSection id="hero">
        <HeroSection />
      </AnimatedSection>

      {/* Features Section */}
      <AnimatedSection id="features">
        <FeaturesSection />
      </AnimatedSection>

      {/* Analytics Preview Section */}
      <AnimatedSection id="analytics">
        <AnalyticsPreview />
      </AnimatedSection>

      {/* CTA Section */}
      <AnimatedSection id="cta">
        <CTASection />
      </AnimatedSection>
    </main>
  );
};

export default Home;

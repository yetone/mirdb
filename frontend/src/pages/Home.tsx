/**
 * Home Page Component
 * Owner: Multiple scenarios (coordination required)
 *
 * Main landing page that composes all homepage sections:
 * - Navbar (existing component)
 * - HeroSection
 * - FeatureCards
 * - HowItWorks
 * - StatsSection
 * - Footer
 *
 * Related Requirements: All homepage requirements
 */

import React from 'react';
import { HeroSection } from '../components/homepage';

export const Home: React.FC = () => {
  return (
    <main>
      <HeroSection />
    </main>
  );
};

export default Home;

/**
 * Home Page Component
 * Owner: Multiple scenarios (coordination required)
 *
 * Main landing page that composes all homepage sections
 *
 * Related Requirements: All homepage requirements
 */

import React from 'react';
import Navbar from '../components/Navbar';
import { HeroSection, FeatureCards, HowItWorks, StatsSection, Footer } from '../components/homepage';

const Home: React.FC = () => {
  return (
    <div className="min-h-screen flex flex-col">
      <Navbar />
      <main className="flex-1">
        <HeroSection />
        <FeatureCards />
        <HowItWorks />
        <StatsSection />
      </main>
      <Footer />
    </div>
  );
};

export default Home;

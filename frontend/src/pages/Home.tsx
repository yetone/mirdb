/**
 * Homepage Landing Page
 * Owner: Scenario 1 - Hero Section Rendering (orchestrates all sections)
 *
 * This is the main entry point for the homepage at route '/'.
 * Composes all homepage sections and integrates with existing contexts.
 */

import React from 'react';
import { Navbar } from '../components/Navbar';
import { HeroSection } from '../components/homepage/HeroSection';
import { useAuth } from '../contexts/AuthContext';

export function Home() {
  const { isAuthenticated } = useAuth();

  return (
    <div className="min-h-screen bg-base-200" data-testid="home-page">
      <Navbar />
      <main>
        <HeroSection isAuthenticated={isAuthenticated} />
        {/* Additional sections will be added by their respective scenarios */}
        {/* <FeaturesSection /> */}
        {/* <HowItWorksSection /> */}
        {/* <AnalyticsPreview /> */}
        {/* <StatsSection /> */}
        {/* <CTASection isAuthenticated={isAuthenticated} /> */}
        {/* <Footer /> */}
      </main>
    </div>
  );
}

export default Home;

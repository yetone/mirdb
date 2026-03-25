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
import { FeaturesSection } from '../components/homepage/FeaturesSection';
import { HowItWorksSection } from '../components/homepage/HowItWorksSection';
import { AnalyticsPreview } from '../components/homepage/AnalyticsPreview';
import { Footer } from '../components/homepage/Footer';
import { StatsSection } from '../components/homepage/StatsSection';
import { useAuth } from '../contexts/AuthContext';

export function Home() {
  const { isAuthenticated } = useAuth();

  return (
    <div className="min-h-screen bg-base-200" data-testid="home-page">
      <Navbar />
      <main>
        <HeroSection isAuthenticated={isAuthenticated} />
        <FeaturesSection />
        <HowItWorksSection />
        <AnalyticsPreview />
        <StatsSection />
        {/* Additional sections will be added by their respective scenarios */}
        {/* <CTASection isAuthenticated={isAuthenticated} /> */}
        <Footer />
      </main>
    </div>
  );
}

export default Home;

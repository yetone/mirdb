/**
 * Homepage Landing Page
 * Owner: Scenario 1 - Hero Section Rendering (orchestrates all sections)
 *
 * This is the main entry point for the homepage at route '/'.
 * Composes all homepage sections and integrates with existing contexts.
 */
import React from 'react';
import { useAuth } from '../contexts/AuthContext';
import { HeroSection } from '../components/homepage';

const Home: React.FC = () => {
  const { isAuthenticated } = useAuth();

  return (
    <main className="min-h-screen bg-gray-900">
      <HeroSection isAuthenticated={isAuthenticated} />
      {/* Future sections to be added by other scenarios:
       * - FeaturesSection (Scenario 4)
       * - HowItWorksSection (Scenario 5)
       * - AnalyticsPreview (Scenario 6)
       * - StatsSection (Scenario 11)
       * - CTASection (Scenario 15)
       * - Footer (Scenario 10)
       */}
    </main>
  );
};

export default Home;

import React from 'react';
import { Navbar } from '../components/Navbar';
import { BackgroundEffect } from '../components/BackgroundEffect';
import { HeroSection, FeaturesSection, HowItWorksSection } from '../components/homepage';

export function Home() {
  return (
    <div className="min-h-screen">
      <BackgroundEffect />
      <Navbar />
      <main className="pt-16">
        <HeroSection />
        <FeaturesSection />
        <HowItWorksSection />
        {/* DashboardPreview will be added by Scenario 7 */}
        {/* Footer will be added by Scenario 8 */}
      </main>
    </div>
  );
}

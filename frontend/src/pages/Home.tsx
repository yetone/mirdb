import React from 'react';
import { Navbar } from '../components/Navbar';
import { BackgroundEffect } from '../components/BackgroundEffect';
import { HeroSection } from '../components/homepage/HeroSection';
import { HowItWorksSection } from '../components/homepage/HowItWorksSection';

export function Home() {
  return (
    <div className="min-h-screen">
      <BackgroundEffect />
      <Navbar />
      <main className="pt-16">
        <HeroSection />
        {/* FeaturesSection will be added by Scenario 5 */}
        <HowItWorksSection />
        {/* DashboardPreview will be added by Scenario 7 */}
        {/* Footer will be added by Scenario 8 */}
      </main>
    </div>
  );
}

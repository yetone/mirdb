/**
 * Homepage component rendered at /.
 * Owner: Scenario 1 - Homepage Renders for Anonymous Users at Root Path
 *
 * Responsibilities:
 * - Compose Navbar, HeroSection, FeaturesSection, CTASection, Footer
 * - Read AuthContext; collaborate with AuthGuard from Scenario 3 to redirect when authenticated
 * - Apply BackgroundEffect for visual identity
 */

import React from 'react';
import { Navbar } from '../components/shared/Navbar';
import { BackgroundEffect } from '../components/shared/BackgroundEffect';

export default function Home() {
  return (
    <div data-testid="home-page">
      <BackgroundEffect />
      <Navbar />
      <main>
        <section data-testid="hero-section">
          <h1>Shorten Links, Track Insights</h1>
        </section>
      </main>
    </div>
  );
}

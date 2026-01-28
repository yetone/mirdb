/**
 * Homepage Component
 * Owner: Scenario 5 - Homepage Integration
 *
 * Requirements covered:
 * - REQ-5: Implement animated background effects
 * - REQ-7: Include skip-to-content link for keyboard navigation
 * - REQ-9: Animate section entrances using scroll-triggered animations
 * - NFR-3: Maintain proper semantic HTML structure with heading hierarchy
 * - NFR-4: Include appropriate ARIA labels on interactive elements and landmarks
 *
 * Expected exports:
 * - Home: React.FC - Main homepage component (default export)
 *
 * Composition:
 * - Skip to content link
 * - BackgroundEffect component (existing)
 * - HeroSection
 * - FeaturesSection
 * - HowItWorks
 * - Footer
 * - ARIA landmarks (main, contentinfo)
 */

import { useRef } from 'react';
import { HeroSection, FeaturesSection, HowItWorks, Footer } from '../components/home';
import { BackgroundEffect } from '../components/shared';

export function Home() {
  const mainRef = useRef<HTMLElement>(null);

  const handleSkipToContent = (e: React.MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault();
    mainRef.current?.focus();
    // scrollIntoView may not be available in test environments (jsdom)
    if (typeof mainRef.current?.scrollIntoView === 'function') {
      mainRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  };

  return (
    <>
      {/* Skip to content link - first focusable element */}
      <a
        href="#main-content"
        onClick={handleSkipToContent}
        className="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 focus:z-50 focus:px-4 focus:py-2 focus:bg-primary focus:text-primary-content focus:rounded-md focus:outline-none focus:ring-2 focus:ring-primary-focus"
        data-testid="skip-to-content"
      >
        Skip to main content
      </a>

      {/* Animated background effect */}
      <BackgroundEffect />

      {/* Main content area with ARIA landmark */}
      <main
        id="main-content"
        ref={mainRef}
        tabIndex={-1}
        className="outline-none"
        aria-label="Homepage main content"
      >
        <HeroSection />
        <FeaturesSection />
        <HowItWorks />
      </main>

      {/* Footer has role="contentinfo" internally */}
      <Footer />
    </>
  );
}

export default Home;

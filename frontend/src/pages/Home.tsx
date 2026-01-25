import { useRef } from 'react';
import { Navbar } from '../components/Navbar';
import { BackgroundEffect } from '../components/BackgroundEffect';
import { HeroSection, FeaturesSection, HowItWorksSection, DashboardPreview, Footer } from '../components/homepage';

/**
 * SkipToContent - Accessibility component for keyboard navigation
 * Provides a skip link that becomes visible on focus for screen reader users
 * to bypass navigation and jump directly to main content.
 */
function SkipToContent({ targetRef }: { targetRef: React.RefObject<HTMLElement> }) {
  const handleClick = (e: React.MouseEvent | React.KeyboardEvent) => {
    e.preventDefault();
    if (targetRef.current) {
      targetRef.current.focus();
      // scrollIntoView may not be available in test environments (jsdom)
      if (typeof targetRef.current.scrollIntoView === 'function') {
        targetRef.current.scrollIntoView({ behavior: 'smooth' });
      }
    }
  };

  return (
    <a
      href="#main-content"
      onClick={handleClick}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          handleClick(e);
        }
      }}
      className="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 focus:z-[100] focus:px-4 focus:py-2 focus:bg-primary focus:text-primary-content focus:rounded-md focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary"
      data-testid="skip-to-content"
    >
      Skip to main content
    </a>
  );
}

export function Home() {
  const mainRef = useRef<HTMLElement>(null);

  return (
    <div className="min-h-screen">
      <SkipToContent targetRef={mainRef} />
      <BackgroundEffect />
      <Navbar />
      <main
        ref={mainRef}
        id="main-content"
        className="pt-16"
        tabIndex={-1}
        aria-label="Main content"
      >
        <HeroSection />
        <FeaturesSection />
        <HowItWorksSection />
        <DashboardPreview />
        <Footer />
      </main>
    </div>
  );
}

/**
 * Home Page Component
 * Owner: Scenario 1 - Homepage Hero Section Rendering (primary)
 *        Extended by Scenario 10 - Performance and Page Load
 *
 * Main homepage composing all sections: Navbar, HeroSection,
 * FeaturesSection, Footer, and BackgroundEffect.
 *
 * Performance optimizations:
 * - Lazy loads FeaturesSection and Footer below the fold
 * - BackgroundEffect respects prefers-reduced-motion
 * - Suspense fallback for below-fold content
 *
 * Expected exports:
 * - Home: React.FC component
 */
import { lazy, Suspense } from 'react';
import HeroSection from '../components/Home/HeroSection';
import UrlShortenerForm from '../components/Home/UrlShortenerForm';
import BackgroundEffect from '../components/BackgroundEffect';

const FeaturesSection = lazy(() => import('../components/Home/FeaturesSection'));
const Footer = lazy(() => import('../components/Home/Footer'));

function FeaturesFallback() {
  return (
    <div
      data-testid="features-fallback"
      className="features-fallback py-16 px-4"
      aria-hidden="true"
    >
      <div className="max-w-6xl mx-auto">
        <div className="h-8 bg-gray-200/20 rounded w-1/3 mx-auto mb-12 animate-pulse" />
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="h-48 bg-gray-200/20 rounded-2xl animate-pulse" />
          ))}
        </div>
      </div>
    </div>
  );
}

export default function Home() {
  return (
    <>
      <a href="#main-content" className="skip-link">
        Skip to main content
      </a>
      <BackgroundEffect />
      <main data-testid="home-page" id="main-content" className="home-page min-h-screen flex flex-col relative">
        <HeroSection urlShortenerForm={<UrlShortenerForm />} />
        <Suspense fallback={<FeaturesFallback />}>
          <FeaturesSection />
          <Footer />
        </Suspense>
      </main>
    </>
  );
}

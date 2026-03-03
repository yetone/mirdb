/**
 * Homepage / Landing Page Component
 * Owner: Scenario 1 - Homepage Hero Section Rendering
 *
 * This is the main landing page for the URL Shortening Service.
 * It renders the hero section with navigation buttons and integrates
 * with existing components like Navbar, BackgroundEffect, and ThemeToggle.
 */
import { Navbar } from '../components/Navbar';
import { BackgroundEffect } from '../components/BackgroundEffect';
import { HeroSection } from '../components/HeroSection';
import GuestShortener from '../components/GuestShortener';
import { FeaturesSection } from '../components/FeatureCard';
import { Footer } from '../components/Footer';

export function Home() {
  return (
    <div className="min-h-screen bg-base-100 overflow-x-hidden" data-testid="home-page">
      <BackgroundEffect />
      <Navbar />
      <main className="pt-16">
        <HeroSection />
        <section className="py-12 px-4" data-testid="guest-shortener-section">
          <GuestShortener />
        </section>
        <FeaturesSection />
      </main>
      <Footer />
    </div>
  );
}

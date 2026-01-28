/**
 * Homepage Component
 * Owner: Scenario 8 - Homepage Integration
 *
 * Main homepage that assembles all sections:
 * - Navbar with Login/Register links
 * - HeroSection
 * - FeaturesSection
 * - CTASection
 * - Footer
 * - BackgroundEffect
 *
 * Requirements: All REQ-*, All US-*
 */

import { Link } from 'react-router-dom';
import { Navbar } from '../components/Navbar';
import { BackgroundEffect } from '../components/BackgroundEffect';
import { FuturisticButton } from '../components/FuturisticButton';
import { GlassMorphismCard } from '../components/GlassMorphismCard';
import { FeaturesSection } from '../components/homepage';
import { Link2, BarChart3, Folder, Share2 } from 'lucide-react';

const Home = () => {
  return (
    <div className="min-h-screen">
      <BackgroundEffect />
      <Navbar />

      {/* Hero Section - Placeholder */}
      <section className="container mx-auto px-4 py-20 text-center">
        <h1 className="text-5xl font-bold mb-6 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent">
          Shorten URLs. Track Every Click.
        </h1>
        <p className="text-xl text-base-content/70 mb-8 max-w-2xl mx-auto">
          Create memorable links with powerful analytics in seconds.
          Track clicks, manage links, and share statistics effortlessly.
        </p>
        <div className="flex justify-center gap-4">
          <Link to="/register">
            <FuturisticButton variant="primary" size="lg">
              Get Started Free
            </FuturisticButton>
          </Link>
          <Link to="/login">
            <FuturisticButton variant="ghost" size="lg">
              Sign In
            </FuturisticButton>
          </Link>
        </div>
      </section>

      {/* Features Section */}
      <FeaturesSection />

      {/* CTA Section - Placeholder */}
      <section className="container mx-auto px-4 py-16 text-center">
        <GlassMorphismCard className="p-12">
          <h2 className="text-3xl font-bold mb-4">Ready to Get Started?</h2>
          <p className="text-base-content/70 mb-8">
            Join thousands of users who trust us with their links.
          </p>
          <Link to="/register">
            <FuturisticButton variant="primary" size="lg">
              Create Free Account
            </FuturisticButton>
          </Link>
        </GlassMorphismCard>
      </section>

      {/* Footer - Placeholder */}
      <footer className="bg-base-200/50 py-8">
        <div className="container mx-auto px-4 text-center">
          <p className="text-base-content/70">
            &copy; 2026 URL Shortener. All rights reserved.
          </p>
          <div className="flex justify-center gap-4 mt-4">
            <Link to="/terms" className="link link-hover text-sm">Terms of Service</Link>
            <Link to="/privacy" className="link link-hover text-sm">Privacy Policy</Link>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default Home;

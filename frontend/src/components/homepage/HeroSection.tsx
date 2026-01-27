/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Implementation
 *
 * Full-viewport hero section with:
 * - Three.js background effect (BackgroundEffect component)
 * - Product tagline: "SHORTEN. TRACK. GROW."
 * - Value proposition subtitle
 * - Primary CTA button ("Get Started Free")
 * - Secondary link to login
 * - Glassmorphism overlay for text readability
 *
 * Props:
 * - onGetStarted?: () => void - Callback for primary CTA
 * - onLogin?: () => void - Callback for login link
 *
 * Requirements covered: REQ-1, REQ-8
 */

import React from 'react';
import { useNavigate } from 'react-router-dom';
import BackgroundEffect from '../BackgroundEffect';
import FuturisticButton from '../FuturisticButton';

interface HeroSectionProps {
  onGetStarted?: () => void;
  onLogin?: () => void;
}

const HeroSection: React.FC<HeroSectionProps> = ({ onGetStarted, onLogin }) => {
  const navigate = useNavigate();

  const handleGetStarted = () => {
    if (onGetStarted) {
      onGetStarted();
    } else {
      navigate('/register');
    }
  };

  const handleLogin = () => {
    if (onLogin) {
      onLogin();
    } else {
      navigate('/login');
    }
  };

  return (
    <section
      className="relative min-h-screen flex items-center justify-center overflow-hidden"
      data-testid="hero-section"
    >
      {/* Three.js Background Effect */}
      <BackgroundEffect />

      {/* Content Overlay */}
      <div className="relative z-10 text-center px-4 max-w-4xl mx-auto">
        {/* Glassmorphism Card for better readability */}
        <div className="backdrop-blur-md bg-base-100/20 border border-base-content/10 rounded-2xl p-8 md:p-12 lg:p-16 shadow-2xl">
          {/* Product Tagline */}
          <h1
            className="text-5xl md:text-6xl lg:text-7xl font-bold mb-6 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
            data-testid="hero-tagline"
          >
            SHORTEN. TRACK. GROW.
          </h1>

          {/* Value Proposition */}
          <p
            className="text-xl md:text-2xl text-base-content/80 mb-10 max-w-2xl mx-auto"
            data-testid="hero-value-proposition"
          >
            Transform long URLs into powerful insights. Track clicks, analyze
            geographic data, and share your analytics with the world.
          </p>

          {/* Primary CTA */}
          <div className="mb-6">
            <FuturisticButton
              onClick={handleGetStarted}
              variant="primary"
              data-testid="hero-cta-button"
            >
              Get Started Free
            </FuturisticButton>
          </div>

          {/* Secondary Login Link */}
          <p className="text-base-content/70">
            Already have an account?{' '}
            <button
              onClick={handleLogin}
              className="text-primary hover:text-primary-focus underline underline-offset-4 transition-colors font-medium"
              data-testid="hero-login-link"
            >
              Sign in
            </button>
          </p>
        </div>
      </div>
    </section>
  );
};

export default HeroSection;

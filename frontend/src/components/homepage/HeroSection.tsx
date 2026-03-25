/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Rendering
 *
 * Displays the main hero area with:
 * - Product headline and tagline
 * - Primary CTA (Get Started / Go to Dashboard)
 * - Secondary CTA (Learn More)
 * - BackgroundEffect animation
 */
import React from 'react';
import { useNavigate } from 'react-router-dom';
import FuturisticButton from '../FuturisticButton';
import BackgroundEffect from '../BackgroundEffect';
import type { HeroSectionProps } from '../../types/homepage';

const HeroSection: React.FC<HeroSectionProps> = ({ isAuthenticated = false }) => {
  const navigate = useNavigate();

  const handleGetStarted = () => {
    navigate('/register');
  };

  const handleGoToDashboard = () => {
    navigate('/dashboard');
  };

  return (
    <section
      className="relative min-h-[80vh] flex items-center justify-center px-4"
      aria-labelledby="hero-heading"
    >
      <BackgroundEffect />

      <div className="relative z-10 text-center max-w-4xl mx-auto">
        <h1
          id="hero-heading"
          className="text-4xl md:text-6xl font-bold bg-gradient-to-r from-purple-400 via-pink-500 to-blue-500 bg-clip-text text-transparent mb-6"
        >
          Shorten Your URLs, Amplify Your Reach
        </h1>

        <p className="text-lg md:text-xl text-gray-300 mb-8 max-w-2xl mx-auto">
          Transform long URLs into powerful, trackable short links.
          Get detailed analytics and insights on every click.
        </p>

        <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
          {isAuthenticated ? (
            <FuturisticButton
              variant="primary"
              size="lg"
              onClick={handleGoToDashboard}
              aria-label="Go to your dashboard"
            >
              Go to Dashboard
            </FuturisticButton>
          ) : (
            <FuturisticButton
              variant="primary"
              size="lg"
              onClick={handleGetStarted}
              aria-label="Get started with URL shortening"
            >
              Get Started
            </FuturisticButton>
          )}

          <FuturisticButton
            variant="outline"
            size="lg"
            aria-label="Learn more about our features"
          >
            Learn More
          </FuturisticButton>
        </div>
      </div>
    </section>
  );
};

export { HeroSection };
export default HeroSection;

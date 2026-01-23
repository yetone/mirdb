/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display and Content
 *
 * A prominent hero section for the homepage featuring:
 * - Main headline communicating the URL shortening service value
 * - Supporting subheadline explaining key benefits
 * - Primary CTA button ("Get Started" / "Go to Dashboard" based on auth state)
 * - Secondary CTA ("Login" for unauthenticated users)
 * - Optional BackgroundEffect integration
 *
 * Related Requirements: REQ-1, REQ-2, REQ-4, REQ-8
 *
 * Expected exports:
 * - HeroSection: React.FC component
 *
 * Props:
 * - None (uses AuthContext internally)
 *
 * Dependencies:
 * - AuthContext: For conditional CTA rendering
 * - FuturisticButton: For CTA buttons
 * - BackgroundEffect: For visual background
 * - react-router-dom: For navigation links
 */

import React from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';

export const HeroSection: React.FC = () => {
  const { isAuthenticated } = useAuth();

  return (
    <section className="hero min-h-screen bg-base-200">
      <div className="hero-content text-center">
        <div className="max-w-2xl">
          <h1 className="text-5xl font-bold">
            Shorten Links. Track Clicks. Grow Insights.
          </h1>
          <p className="py-6 text-lg">
            Create memorable, short links instantly. Track every click with detailed
            statistics and understand your audience with geographic and device insights.
          </p>
          <div className="flex gap-4 justify-center">
            {isAuthenticated ? (
              <Link
                to="/dashboard"
                className="btn btn-primary btn-lg"
              >
                Go to Dashboard
              </Link>
            ) : (
              <>
                <Link
                  to="/register"
                  className="btn btn-primary btn-lg"
                  data-testid="get-started-button"
                >
                  Get Started
                </Link>
                <Link
                  to="/login"
                  className="btn btn-outline btn-lg"
                  data-testid="login-button"
                >
                  Login
                </Link>
              </>
            )}
          </div>
        </div>
      </div>
    </section>
  );
};

export default HeroSection;

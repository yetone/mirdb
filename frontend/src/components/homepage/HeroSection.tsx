/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display, Scenario 4 - Authenticated User CTA
 *
 * Displays the main hero section of the homepage with:
 * - Compelling headline and subheadline
 * - Primary CTA (Get Started / Go to Dashboard based on auth state)
 * - Secondary login link
 * - Optional background effects
 *
 * Requirements covered:
 * - REQ-1: Hero section with product tagline
 * - REQ-2: Prominent CTA buttons
 * - REQ-10: Dashboard redirect for authenticated users
 */

import React from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { FuturisticButton } from '../FuturisticButton';

interface HeroSectionProps {
  isAuthenticated?: boolean;
}

export function HeroSection({ isAuthenticated: isAuthenticatedProp }: HeroSectionProps) {
  const auth = useAuth();
  const isAuthenticated = isAuthenticatedProp ?? auth.isAuthenticated;

  return (
    <section className="hero min-h-[80vh] flex items-center justify-center px-4">
      <div className="text-center max-w-4xl mx-auto">
        <h1 className="text-5xl md:text-6xl font-bold mb-6 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent">
          Shorten URLs. Track Every Click.
        </h1>
        <p className="text-xl md:text-2xl text-base-content/70 mb-8 max-w-2xl mx-auto">
          Transform long, unwieldy URLs into clean, memorable short links. Get detailed analytics on every click.
        </p>

        <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
          {isAuthenticated ? (
            <FuturisticButton to="/dashboard" variant="primary" size="lg" data-testid="hero-cta">
              Go to Dashboard
            </FuturisticButton>
          ) : (
            <>
              <FuturisticButton to="/register" variant="primary" size="lg" data-testid="hero-cta">
                Get Started
              </FuturisticButton>
              <span className="text-base-content/60">
                Already have an account?{' '}
                <Link to="/login" className="text-primary hover:underline" data-testid="hero-login-link">
                  Login
                </Link>
              </span>
            </>
          )}
        </div>
      </div>
    </section>
  );
}

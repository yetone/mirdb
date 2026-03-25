/**
 * Final CTA Section Component
 * Owner: Scenario 15 - Final CTA Section
 *
 * Final conversion prompt above footer:
 * - Compelling headline
 * - CTA button (adapts to auth state)
 */

import React from 'react';
import { FuturisticButton } from '../FuturisticButton';
import { CTASectionProps } from '../../types/homepage';

export function CTASection({ isAuthenticated = false }: CTASectionProps) {
  return (
    <section
      className="py-16 px-4 bg-gradient-to-r from-primary/10 to-secondary/10"
      aria-label="Call to action"
      data-testid="cta-section"
    >
      <div className="max-w-4xl mx-auto text-center">
        <h2
          className="text-3xl md:text-4xl font-bold mb-4 text-base-content"
          data-testid="cta-headline"
        >
          Ready to Supercharge Your Links?
        </h2>
        <p className="text-lg text-base-content/70 mb-8 max-w-2xl mx-auto">
          Join thousands of users who trust us to shorten, track, and optimize their URLs.
          Start for free today.
        </p>
        {isAuthenticated ? (
          <FuturisticButton
            to="/dashboard"
            variant="primary"
            size="lg"
            data-testid="cta-dashboard-button"
          >
            Go to Dashboard
          </FuturisticButton>
        ) : (
          <FuturisticButton
            to="/register"
            variant="primary"
            size="lg"
            data-testid="cta-register-button"
          >
            Get Started Free
          </FuturisticButton>
        )}
      </div>
    </section>
  );
}

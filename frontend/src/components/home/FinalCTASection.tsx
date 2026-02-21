import React from 'react';
import { FuturisticButton } from '../common/FuturisticButton';

export function FinalCTASection() {
  return (
    <section
      className="py-20 px-4"
      data-testid="final-cta-section"
      aria-labelledby="final-cta-heading"
    >
      <div className="max-w-3xl mx-auto text-center">
        <h2
          id="final-cta-heading"
          className="text-3xl md:text-4xl font-bold mb-6"
        >
          Ready to Get Started?
        </h2>
        <p className="text-lg text-base-content/70 mb-8">
          Join thousands of users who trust our platform for their URL shortening
          and analytics needs. Start creating powerful short links today.
        </p>
        <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
          <FuturisticButton
            to="/register"
            variant="primary"
            aria-label="Sign up for free"
            data-testid="final-cta-signup"
          >
            Sign Up for Free
          </FuturisticButton>
          <FuturisticButton
            to="/login"
            variant="outline"
            aria-label="Already have an account? Sign in"
            data-testid="final-cta-signin"
          >
            Already have an account?
          </FuturisticButton>
        </div>
      </div>
    </section>
  );
}

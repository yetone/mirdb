/**
 * Call-to-Action Section Component
 * Owner: Scenario 4 - CTA Section Implementation
 *
 * Final section encouraging user registration:
 * - Prominent "Create Free Account" button (FuturisticButton)
 * - Secondary "Sign In" link
 * - Brief reinforcement of value proposition
 *
 * Props:
 * - onCreateAccount?: () => void - Callback for registration CTA
 * - onSignIn?: () => void - Callback for sign-in link
 *
 * Requirements covered: REQ-9, REQ-5
 */

import React from 'react';
import { useNavigate } from 'react-router-dom';
import FuturisticButton from '../FuturisticButton';

interface CTASectionProps {
  onCreateAccount?: () => void;
  onSignIn?: () => void;
}

export const CTASection: React.FC<CTASectionProps> = ({ onCreateAccount, onSignIn }) => {
  const navigate = useNavigate();

  const handleCreateAccount = () => {
    if (onCreateAccount) {
      onCreateAccount();
    } else {
      navigate('/register');
    }
  };

  const handleSignIn = () => {
    if (onSignIn) {
      onSignIn();
    } else {
      navigate('/login');
    }
  };

  return (
    <section
      className="py-20 px-4 text-center"
      data-testid="cta-section"
      aria-labelledby="cta-heading"
    >
      <div className="max-w-3xl mx-auto">
        {/* Value Reinforcement Text */}
        <h2
          id="cta-heading"
          className="text-3xl md:text-4xl font-bold mb-6 text-base-content"
          data-testid="cta-value-text"
        >
          Ready to supercharge your links?
        </h2>

        <p className="text-xl text-base-content/70 mb-10">
          Join thousands of users who trust us to shorten, track, and analyze
          their links. Start for free today.
        </p>

        {/* Primary CTA Button */}
        <div className="mb-6">
          <FuturisticButton
            onClick={handleCreateAccount}
            variant="primary"
            data-testid="cta-primary-button"
          >
            Create Free Account
          </FuturisticButton>
        </div>

        {/* Secondary Sign In Link */}
        <p className="text-base-content/70">
          Already have an account?{' '}
          <button
            onClick={handleSignIn}
            className="text-primary hover:text-primary-focus underline underline-offset-4 transition-colors font-medium"
            data-testid="cta-signin-link"
          >
            Sign In
          </button>
        </p>
      </div>
    </section>
  );
};

export default CTASection;

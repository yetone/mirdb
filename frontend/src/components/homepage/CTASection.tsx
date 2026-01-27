/**
 * Final CTA Section Component
 * Owner: Scenario 5 - Call-to-Action and Navigation
 *
 * Requirements: REQ-2
 * User Stories: US-2, US-3
 *
 * Expected functionality:
 * - Reinforcing call-to-action headline
 * - Primary "Get Started" button
 * - Secondary "Sign In" link
 * - Optional: Footer with copyright
 * - Links to registration flow
 *
 * Props:
 * - onGetStarted?: () => void
 * - onSignIn?: () => void
 */

import { motion } from 'framer-motion';
import { useNavigate } from 'react-router-dom';
import { FuturisticButton } from '../FuturisticButton';

interface CTASectionProps {
  onGetStarted?: () => void;
  onSignIn?: () => void;
}

export function CTASection({ onGetStarted, onSignIn }: CTASectionProps) {
  const navigate = useNavigate();

  const handleGetStarted = () => {
    if (onGetStarted) {
      onGetStarted();
    }
    navigate('/register');
  };

  const handleSignIn = () => {
    if (onSignIn) {
      onSignIn();
    }
    navigate('/login');
  };

  return (
    <section
      data-testid="cta-section"
      aria-labelledby="cta-heading"
      className="py-20 px-4 sm:px-6 lg:px-8 bg-gradient-to-b from-transparent to-base-200/50"
    >
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true, margin: '-100px' }}
        transition={{ duration: 0.5 }}
        className="max-w-3xl mx-auto text-center"
      >
        <h2
          id="cta-heading"
          className="text-3xl sm:text-4xl font-bold tracking-tight mb-4"
        >
          Ready to Get Started?
        </h2>

        <p className="text-lg text-base-content/70 mb-8 max-w-xl mx-auto">
          Join thousands of users who are already shortening their URLs and
          tracking their link performance.
        </p>

        <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
          <FuturisticButton
            variant="primary"
            size="lg"
            onClick={handleGetStarted}
            data-testid="cta-get-started-button"
          >
            Get Started Free
          </FuturisticButton>
        </div>

        <p className="mt-6 text-sm text-base-content/60">
          Already have an account?{' '}
          <button
            onClick={handleSignIn}
            data-testid="cta-sign-in-link"
            className="text-primary hover:text-primary-focus underline underline-offset-2 font-medium transition-colors"
          >
            Sign In
          </button>
        </p>
      </motion.div>
    </section>
  );
}

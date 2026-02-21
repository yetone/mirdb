/**
 * Call-to-Action Section Component.
 * Owner: Scenario 5 - CTA & Footer
 *
 * Expected behavior:
 * - Center-aligned banner with final sign-up prompt
 * - Prominent CTA button linking to registration
 * - Brief reinforcing message
 * - Visually distinct background
 */

import { useNavigate } from 'react-router-dom';

function CTASection() {
  const navigate = useNavigate();

  const handleSignUp = () => {
    navigate('/register');
  };

  return (
    <section
      className="bg-primary py-16 px-4"
      data-testid="cta-section"
    >
      <div
        className="container mx-auto max-w-4xl text-center"
        data-testid="cta-content"
      >
        <h2
          className="text-3xl md:text-4xl font-bold text-primary-content mb-4"
          data-testid="cta-prompt"
        >
          Ready to Get Started?
        </h2>
        <p className="text-lg text-primary-content mb-8 max-w-2xl mx-auto">
          Join thousands of users who trust us to shorten their links and track their analytics. Sign up for free today.
        </p>
        <button
          onClick={handleSignUp}
          className="btn bg-base-100 text-base-content hover:bg-base-200 btn-lg border-0"
          data-testid="cta-button"
        >
          Sign Up Free
        </button>
      </div>
    </section>
  );
}

export default CTASection;

/**
 * CTA Section Component.
 * Owner: Scenario 7 - Call-to-Action Section
 *
 * Bottom call-to-action section:
 * - Heading: "Ready to shorten your first link?"
 * - Primary CTA button: "Create Free Account" -> /register
 * - Secondary text: "Start tracking clicks in 30 seconds"
 *
 * Min height: 300px
 */
import { Link } from 'react-router-dom';

export function CTASection() {
  return (
    <section
      className="cta-section flex flex-col items-center justify-center px-4 py-16"
      style={{ minHeight: '300px' }}
      aria-labelledby="cta-headline"
      data-testid="cta-section"
    >
      <div className="max-w-2xl mx-auto text-center">
        {/* CTA Heading */}
        <h2
          id="cta-headline"
          className="text-3xl md:text-4xl font-bold mb-6"
        >
          Ready to shorten your first link?
        </h2>

        {/* Primary CTA Button */}
        <Link
          to="/register"
          className="btn btn-primary btn-lg mb-4"
          role="button"
        >
          Create Free Account
        </Link>

        {/* Secondary Text */}
        <p className="text-base-content/70 text-lg">
          Start tracking clicks in 30 seconds
        </p>
      </div>
    </section>
  );
}

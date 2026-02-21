/**
 * Social Proof Section Component.
 * Owner: Scenario 4 - Demo & Social Proof
 *
 * Expected behavior:
 * - Trust indicators (privacy, speed, reliability icons)
 * - Optional usage statistics display
 * - Testimonial placeholders if available
 * - Full-width banner or horizontal card layout
 */

import { SOCIAL_PROOF } from '../../utils/constants';

function getIcon(iconName: string) {
  switch (iconName) {
    case 'lock':
      return (
        <svg
          className="w-8 h-8"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
          data-testid="trust-indicator-icon"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"
          />
        </svg>
      );
    case 'zap':
      return (
        <svg
          className="w-8 h-8"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
          data-testid="trust-indicator-icon"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M13 10V3L4 14h7v7l9-11h-7z"
          />
        </svg>
      );
    case 'check':
      return (
        <svg
          className="w-8 h-8"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
          data-testid="trust-indicator-icon"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
          />
        </svg>
      );
    default:
      return (
        <svg
          className="w-8 h-8"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
          data-testid="trust-indicator-icon"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M5 13l4 4L19 7"
          />
        </svg>
      );
  }
}

function SocialProofSection() {
  return (
    <section
      id="social-proof"
      className="py-16 lg:py-24 px-4 bg-base-200"
      data-testid="social-proof-section"
    >
      <div className="container mx-auto max-w-6xl">
        <div className="text-center mb-12">
          <h2 className="text-3xl md:text-4xl font-bold text-base-content mb-4">
            Why Choose Us
          </h2>
          <p className="text-base-content/80 max-w-2xl mx-auto">
            Built with security, speed, and reliability at its core.
          </p>
        </div>

        {/* Trust Indicators Grid */}
        <div
          className="grid grid-cols-1 md:grid-cols-3 gap-6 lg:gap-8"
          data-testid="trust-indicators-container"
        >
          {SOCIAL_PROOF.map((item, index) => (
            <div
              key={item.title}
              className="bg-base-100 rounded-xl p-6 shadow-md text-center hover:shadow-lg transition-shadow"
              data-testid={`trust-indicator-${index}`}
            >
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-primary/10 text-primary mb-4">
                {getIcon(item.icon)}
              </div>
              <h3
                className="text-lg font-semibold text-base-content mb-2"
                data-testid="trust-indicator-title"
              >
                {item.title}
              </h3>
              <p
                className="text-base-content/80 text-sm"
                data-testid="trust-indicator-description"
              >
                {item.description}
              </p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

export default SocialProofSection;

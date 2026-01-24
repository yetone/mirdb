/**
 * Social Proof Section Component
 * Owner: Scenario 5 - Social Proof Section
 *
 * Displays trust-building elements:
 * - Usage statistics (URLs shortened, clicks tracked)
 * - Trust indicators (security badge, uptime commitment)
 * - Optional: User testimonials or company logos
 *
 * Requirements: REQ-5
 */

import { GlassMorphismCard } from '../GlassMorphismCard';
import type { Statistic, SocialProofSectionProps } from '../../types/landing';

const defaultStatistics: Statistic[] = [
  {
    value: '10K+',
    label: 'URLs Shortened',
  },
  {
    value: '500K+',
    label: 'Clicks Tracked',
  },
  {
    value: '1K+',
    label: 'Active Users',
  },
];

function ShieldIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      fill="none"
      viewBox="0 0 24 24"
      strokeWidth={1.5}
      stroke="currentColor"
      className="w-6 h-6"
      aria-hidden="true"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M9 12.75 11.25 15 15 9.75m-3-7.036A11.959 11.959 0 0 1 3.598 6 11.99 11.99 0 0 0 3 9.749c0 5.592 3.824 10.29 9 11.623 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.571-.598-3.751h-.152c-3.196 0-6.1-1.248-8.25-3.285Z"
      />
    </svg>
  );
}

function UptimeIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      fill="none"
      viewBox="0 0 24 24"
      strokeWidth={1.5}
      stroke="currentColor"
      className="w-6 h-6"
      aria-hidden="true"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M12 6v6h4.5m4.5 0a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z"
      />
    </svg>
  );
}

function StatisticCard({ statistic }: { statistic: Statistic }) {
  return (
    <GlassMorphismCard className="text-center">
      <div data-testid={`statistic-${statistic.label.toLowerCase().replace(/\s+/g, '-')}`}>
        <div className="text-4xl md:text-5xl font-bold text-primary mb-2" data-testid="statistic-value">
          {statistic.value}
        </div>
        <div className="text-base-content/70" data-testid="statistic-label">
          {statistic.label}
        </div>
      </div>
    </GlassMorphismCard>
  );
}

function TrustIndicator({ icon, title, description, testId }: {
  icon: React.ReactNode;
  title: string;
  description: string;
  testId: string;
}) {
  return (
    <div
      className="flex items-start gap-4 p-4"
      data-testid={testId}
    >
      <div className="text-primary flex-shrink-0">
        {icon}
      </div>
      <div>
        <h3 className="font-semibold text-base-content">{title}</h3>
        <p className="text-sm text-base-content/70">{description}</p>
      </div>
    </div>
  );
}

export function SocialProofSection({
  statistics = defaultStatistics,
  showTestimonials = false
}: SocialProofSectionProps) {
  return (
    <section
      id="social-proof"
      className="py-16 px-4 md:px-8 lg:px-16"
      aria-labelledby="social-proof-heading"
      data-testid="social-proof-section"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="social-proof-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12 text-base-content"
        >
          Trusted by Thousands
        </h2>

        {/* Statistics Grid */}
        <div
          className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-12"
          data-testid="statistics-grid"
        >
          {statistics.map((stat) => (
            <StatisticCard key={stat.label} statistic={stat} />
          ))}
        </div>

        {/* Trust Indicators */}
        <div
          className="grid grid-cols-1 md:grid-cols-2 gap-6"
          data-testid="trust-indicators"
        >
          <GlassMorphismCard>
            <TrustIndicator
              icon={<ShieldIcon />}
              title="Secure & Private"
              description="Your data is protected with industry-standard encryption. We never share your information."
              testId="trust-indicator-security"
            />
          </GlassMorphismCard>
          <GlassMorphismCard>
            <TrustIndicator
              icon={<UptimeIcon />}
              title="99.9% Uptime"
              description="Reliable service you can count on. Your links are always available when you need them."
              testId="trust-indicator-uptime"
            />
          </GlassMorphismCard>
        </div>

        {/* Optional Testimonials Section */}
        {showTestimonials && (
          <div
            className="mt-12"
            data-testid="testimonials-section"
          >
            <h3 className="text-2xl font-semibold text-center mb-8 text-base-content">
              What Our Users Say
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              <GlassMorphismCard>
                <blockquote className="text-base-content/80">
                  "This URL shortener has transformed how I share links. The analytics are incredibly useful!"
                </blockquote>
                <footer className="mt-4 text-sm text-base-content/60">
                  — Happy User
                </footer>
              </GlassMorphismCard>
            </div>
          </div>
        )}
      </div>
    </section>
  );
}

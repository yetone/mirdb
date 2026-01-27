/**
 * How It Works Section Component
 * Owner: Scenario 3 - How It Works Section
 *
 * Requirements: REQ-5
 * User Stories: US-5
 *
 * Expected functionality:
 * - 3-step process explanation:
 *   1. Create short link - paste URL, get short link
 *   2. Share anywhere - social, email, messages
 *   3. Track analytics - see clicks, referrers, locations
 * - Horizontal stepper on desktop
 * - Vertical stepper on mobile
 * - Visual connectors between steps
 * - Numbered indicators or icons for each step
 * - Smooth scroll anchor: #how-it-works
 */
import React from 'react';
import { Link2, Share2, BarChart3 } from 'lucide-react';

interface Step {
  number: number;
  title: string;
  description: string;
  icon: React.ReactNode;
}

const steps: Step[] = [
  {
    number: 1,
    title: 'Create Short Link',
    description: 'Paste your long URL and get a short, memorable link instantly',
    icon: <Link2 className="w-6 h-6" aria-hidden="true" />,
  },
  {
    number: 2,
    title: 'Share Anywhere',
    description: 'Share your short link via social media, email, or messages',
    icon: <Share2 className="w-6 h-6" aria-hidden="true" />,
  },
  {
    number: 3,
    title: 'Track Analytics',
    description: 'Monitor clicks, referrers, and locations with detailed statistics',
    icon: <BarChart3 className="w-6 h-6" aria-hidden="true" />,
  },
];

export const HowItWorksSection: React.FC = () => {
  return (
    <section
      id="how-it-works"
      className="py-16 px-4 sm:px-6 lg:px-8"
      aria-labelledby="how-it-works-heading"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="how-it-works-heading"
          className="text-3xl font-bold text-center mb-12 text-base-content"
        >
          How It Works
        </h2>

        {/* Steps container - horizontal on desktop, vertical on mobile */}
        <div
          className="flex flex-col md:flex-row items-center md:items-start justify-center gap-4 md:gap-0"
          role="list"
          aria-label="Steps to use the service"
        >
          {steps.map((step, index) => (
            <React.Fragment key={step.number}>
              {/* Step item */}
              <div
                className="flex flex-col items-center text-center max-w-xs"
                role="listitem"
                data-testid={`step-${step.number}`}
              >
                {/* Number indicator */}
                <div
                  className="w-16 h-16 rounded-full bg-primary text-primary-content flex items-center justify-center text-2xl font-bold mb-4"
                  aria-label={`Step ${step.number}`}
                  data-testid={`step-number-${step.number}`}
                >
                  {step.number}
                </div>

                {/* Icon */}
                <div className="mb-3 text-primary">{step.icon}</div>

                {/* Title */}
                <h3 className="text-xl font-semibold mb-2 text-base-content">
                  {step.title}
                </h3>

                {/* Description */}
                <p className="text-base-content/70">{step.description}</p>
              </div>

              {/* Connector between steps */}
              {index < steps.length - 1 && (
                <div
                  className="step-connector hidden md:flex items-center justify-center flex-1 px-4"
                  aria-hidden="true"
                  data-testid={`connector-${index + 1}`}
                >
                  <div className="w-full h-0.5 bg-primary/30 relative">
                    <div className="absolute right-0 top-1/2 -translate-y-1/2 w-0 h-0 border-t-4 border-t-transparent border-b-4 border-b-transparent border-l-8 border-l-primary/30" />
                  </div>
                </div>
              )}

              {/* Vertical connector for mobile */}
              {index < steps.length - 1 && (
                <div
                  className="step-connector-vertical md:hidden h-8 w-0.5 bg-primary/30 my-2"
                  aria-hidden="true"
                  data-testid={`connector-vertical-${index + 1}`}
                />
              )}
            </React.Fragment>
          ))}
        </div>
      </div>
    </section>
  );
};

export default HowItWorksSection;

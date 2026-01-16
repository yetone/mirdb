import React from 'react';

interface Step {
  number: number;
  title: string;
  description: string;
}

const steps: Step[] = [
  {
    number: 1,
    title: 'Paste your long URL',
    description: 'Copy and paste your lengthy URL into our shortener',
  },
  {
    number: 2,
    title: 'Get your short link instantly',
    description: 'We generate a compact, easy-to-share link in seconds',
  },
  {
    number: 3,
    title: 'Track performance with analytics',
    description: 'Monitor clicks, locations, and referrers in real-time',
  },
];

interface HowItWorksSectionProps {
  'data-testid'?: string
}

const HowItWorksSection: React.FC<HowItWorksSectionProps> = ({ 'data-testid': testId }) => {
  return (
    <section
      id="how-it-works"
      className="py-16 px-4 md:px-8"
      aria-labelledby="how-it-works-title"
      data-testid={testId || 'how-it-works-section'}
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="how-it-works-title"
          className="text-3xl md:text-4xl font-bold text-center mb-12"
        >
          How It Works
        </h2>

        <div className="flex flex-col md:flex-row items-center justify-center gap-8 md:gap-4">
          {steps.map((step, index) => (
            <React.Fragment key={step.number}>
              <div
                className="flex flex-col items-center text-center max-w-xs"
                data-testid={`step-${step.number}`}
              >
                <div
                  className="w-16 h-16 rounded-full bg-primary text-primary-content flex items-center justify-center text-2xl font-bold mb-4"
                  aria-label={`Step ${step.number}`}
                  data-testid={`step-number-${step.number}`}
                >
                  {step.number}
                </div>
                <h3 className="text-xl font-semibold mb-2" data-testid={`step-title-${step.number}`}>
                  {step.title}
                </h3>
                <p className="text-base-content/70" data-testid={`step-description-${step.number}`}>
                  {step.description}
                </p>
              </div>

              {/* Connector line between steps */}
              {index < steps.length - 1 && (
                <div
                  className="hidden md:block w-16 h-1 bg-primary/30 rounded-full"
                  data-testid={`connector-${index + 1}`}
                  aria-hidden="true"
                />
              )}

              {/* Vertical connector for mobile */}
              {index < steps.length - 1 && (
                <div
                  className="md:hidden w-1 h-8 bg-primary/30 rounded-full"
                  data-testid={`connector-mobile-${index + 1}`}
                  aria-hidden="true"
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

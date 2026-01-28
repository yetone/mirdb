/**
 * How It Works Section Component
 * Owner: Scenario 3 - How It Works Section
 *
 * Requirements covered:
 * - REQ-3: Include "How It Works" section explaining the 3-step user workflow
 *
 * Expected exports:
 * - HowItWorks: React.FC - Workflow explanation component
 *
 * Steps:
 * 1. "Paste Your URL" - Enter any long URL
 * 2. "Get Your Short Link" - Instantly receive a short link
 * 3. "Track Performance" - Monitor clicks with analytics
 *
 * Layout:
 * - Numbered step indicators (1, 2, 3)
 * - Icon + title + description for each step
 * - Ordered list for semantic structure
 */

interface Step {
  id: number;
  title: string;
  description: string;
  icon: React.ReactNode;
}

// Clipboard/Paste icon
const PasteIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    className="h-8 w-8"
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2}
    aria-hidden="true"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"
    />
  </svg>
);

// Link/Short URL icon
const ShortLinkIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    className="h-8 w-8"
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2}
    aria-hidden="true"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
    />
  </svg>
);

// Analytics/Chart icon
const AnalyticsIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    className="h-8 w-8"
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2}
    aria-hidden="true"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
    />
  </svg>
);

const steps: Step[] = [
  {
    id: 1,
    title: 'Paste Your URL',
    description: 'Enter any long URL - no sign-up required to try.',
    icon: <PasteIcon />,
  },
  {
    id: 2,
    title: 'Get Your Short Link',
    description: 'Instantly receive a short, shareable link.',
    icon: <ShortLinkIcon />,
  },
  {
    id: 3,
    title: 'Track Performance',
    description: 'Monitor clicks and traffic with detailed analytics.',
    icon: <AnalyticsIcon />,
  },
];

export function HowItWorks() {
  return (
    <section
      className="py-16 md:py-24 px-4 bg-base-200/50"
      aria-labelledby="how-it-works-heading"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="how-it-works-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12"
        >
          How It Works
        </h2>

        {/* Ordered list for semantic structure */}
        <ol
          className="grid grid-cols-1 md:grid-cols-3 gap-8"
          data-testid="how-it-works-steps"
        >
          {steps.map((step) => (
            <li
              key={step.id}
              className="flex flex-col items-center text-center"
              data-testid="how-it-works-step"
            >
              {/* Step number indicator */}
              <div className="flex items-center justify-center w-12 h-12 rounded-full bg-primary text-primary-content font-bold text-xl mb-4">
                {step.id}
              </div>

              {/* Icon */}
              <div className="text-primary mb-4" data-testid="step-icon">
                {step.icon}
              </div>

              {/* Title */}
              <h3 className="text-xl font-semibold mb-2">{step.title}</h3>

              {/* Description */}
              <p className="text-base-content/70">{step.description}</p>
            </li>
          ))}
        </ol>
      </div>
    </section>
  );
}

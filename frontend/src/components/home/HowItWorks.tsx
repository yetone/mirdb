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

// Clipboard/Paste icon SVG
const ClipboardIcon = () => (
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

// Link/Share icon SVG
const ShareIcon = () => (
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
      d="M8.684 13.342C8.886 12.938 9 12.482 9 12c0-.482-.114-.938-.316-1.342m0 2.684a3 3 0 110-2.684m0 2.684l6.632 3.316m-6.632-6l6.632-3.316m0 0a3 3 0 105.367-2.684 3 3 0 00-5.367 2.684zm0 9.316a3 3 0 105.368 2.684 3 3 0 00-5.368-2.684z"
    />
  </svg>
);

// Chart/Analytics icon SVG
const ChartBarIcon = () => (
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
    description: 'Enter any long URL - no sign-up required to try. Simply paste your link and watch the magic happen.',
    icon: <ClipboardIcon />,
  },
  {
    id: 2,
    title: 'Get Your Short Link',
    description: 'Instantly receive a short, shareable link. Copy it with one click and share it anywhere.',
    icon: <ShareIcon />,
  },
  {
    id: 3,
    title: 'Track Performance',
    description: 'Monitor clicks and traffic with detailed analytics. See where your visitors come from and when they click.',
    icon: <ChartBarIcon />,
  },
];

export function HowItWorks() {
  return (
    <section
      className="py-16 md:py-24 px-4 bg-base-200"
      aria-labelledby="how-it-works-heading"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="how-it-works-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12"
        >
          How It Works
        </h2>

        <ol className="grid grid-cols-1 md:grid-cols-3 gap-8 md:gap-12">
          {steps.map((step) => (
            <li
              key={step.id}
              className="flex flex-col items-center text-center"
              data-testid="step-item"
              data-step={step.id}
            >
              {/* Step container - wraps everything for test querying */}
              <div data-testid={`step-${step.id}`}>
                {/* Icon and number container */}
                <div className="relative mb-6">
                {/* Icon container */}
                <div
                  className="w-20 h-20 rounded-full bg-primary/10 flex items-center justify-center text-primary"
                  data-testid="step-icon"
                >
                  {step.icon}
                </div>

                {/* Step number badge */}
                <div
                  className="absolute -top-2 -right-2 w-8 h-8 rounded-full bg-primary text-primary-content flex items-center justify-center text-sm font-bold"
                  data-testid="step-number"
                >
                  {step.id}
                </div>
              </div>

                {/* Title */}
                <h3 className="text-xl font-semibold mb-3">{step.title}</h3>

                {/* Description */}
                <p
                  className="text-base-content/70 max-w-xs"
                  data-testid="step-description"
                >
                  {step.description}
                </p>
              </div>
            </li>
          ))}
        </ol>
      </div>
    </section>
  );
}

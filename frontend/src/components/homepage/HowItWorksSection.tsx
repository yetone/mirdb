/**
 * How It Works Section Component
 * Owner: Scenario 4 - How It Works Section Display
 *
 * Displays a 3-step visual process guide.
 *
 * Steps to display:
 * 1. "Paste Your URL" - Input icon with label
 * 2. "Get Short Link" - Link icon with arrow
 * 3. "Track & Share" - Chart icon with label
 *
 * Visual elements:
 * - Step numbers (1, 2, 3)
 * - Icons for each step
 * - Connecting lines/arrows between steps
 *
 * Layout:
 * - Desktop: Horizontal steps with arrows
 * - Mobile: Vertical steps stack
 */

interface Step {
  number: number
  title: string
  description: string
  icon: React.ReactNode
}

interface StepCardProps extends Step {
  isLast: boolean
}

// Clipboard/Paste icon for "Paste Your URL"
function ClipboardIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      className="h-10 w-10"
      fill="none"
      viewBox="0 0 24 24"
      stroke="currentColor"
      aria-hidden="true"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={2}
        d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"
      />
    </svg>
  )
}

// Link icon for "Get Short Link"
function LinkIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      className="h-10 w-10"
      fill="none"
      viewBox="0 0 24 24"
      stroke="currentColor"
      aria-hidden="true"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={2}
        d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
      />
    </svg>
  )
}

// Share/Chart icon for "Track & Share"
function ShareChartIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      className="h-10 w-10"
      fill="none"
      viewBox="0 0 24 24"
      stroke="currentColor"
      aria-hidden="true"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={2}
        d="M8.684 13.342C8.886 12.938 9 12.482 9 12c0-.482-.114-.938-.316-1.342m0 2.684a3 3 0 110-2.684m0 2.684l6.632 3.316m-6.632-6l6.632-3.316m0 0a3 3 0 105.367-2.684 3 3 0 00-5.367 2.684zm0 9.316a3 3 0 105.368 2.684 3 3 0 00-5.368-2.684z"
      />
    </svg>
  )
}

// Arrow icon for connecting steps (horizontal on desktop)
function ArrowIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      className="h-8 w-8 text-primary hidden md:block"
      fill="none"
      viewBox="0 0 24 24"
      stroke="currentColor"
      aria-hidden="true"
      data-testid="step-connector-arrow"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={2}
        d="M17 8l4 4m0 0l-4 4m4-4H3"
      />
    </svg>
  )
}

// Vertical connector line for mobile
function VerticalConnector() {
  return (
    <div
      className="h-8 w-0.5 bg-primary mx-auto md:hidden"
      aria-hidden="true"
      data-testid="step-connector-line"
    />
  )
}

function StepCard({ number, title, description, icon, isLast }: StepCardProps) {
  return (
    <>
      <div
        className="flex flex-col items-center text-center flex-1"
        data-testid="step-card"
      >
        {/* Step number indicator */}
        <div
          className="w-12 h-12 rounded-full bg-primary text-primary-content flex items-center justify-center text-xl font-bold mb-4"
          data-testid="step-number"
          aria-label={`Step ${number}`}
        >
          {number}
        </div>

        {/* Step icon */}
        <div className="text-primary mb-3" data-testid="step-icon">
          {icon}
        </div>

        {/* Step title */}
        <h3
          className="text-lg font-semibold mb-2"
          data-testid="step-title"
        >
          {title}
        </h3>

        {/* Step description */}
        <p
          className="text-base-content/80 text-sm max-w-xs"
          data-testid="step-description"
        >
          {description}
        </p>
      </div>

      {/* Connector between steps */}
      {!isLast && (
        <div className="flex items-center justify-center mx-4" data-testid="step-connector">
          <ArrowIcon />
          <VerticalConnector />
        </div>
      )}
    </>
  )
}

const steps: Step[] = [
  {
    number: 1,
    title: 'Paste Your URL',
    description: 'Copy any long URL and paste it into our input field. No sign-up required to get started.',
    icon: <ClipboardIcon />,
  },
  {
    number: 2,
    title: 'Get Short Link',
    description: 'Click the shorten button and instantly receive a compact, shareable link.',
    icon: <LinkIcon />,
  },
  {
    number: 3,
    title: 'Track & Share',
    description: 'Share your link anywhere and track its performance with detailed analytics.',
    icon: <ShareChartIcon />,
  },
]

export default function HowItWorksSection() {
  return (
    <section
      className="py-16 px-4 bg-base-100"
      aria-labelledby="how-it-works-heading"
      data-testid="how-it-works-section"
    >
      <div className="container mx-auto max-w-4xl">
        <h2
          id="how-it-works-heading"
          className="text-3xl font-bold text-center mb-12"
        >
          How It Works
        </h2>

        <div
          className="flex flex-col md:flex-row items-center justify-center"
          data-testid="steps-container"
        >
          {steps.map((step, index) => (
            <StepCard
              key={step.number}
              {...step}
              isLast={index === steps.length - 1}
            />
          ))}
        </div>
      </div>
    </section>
  )
}

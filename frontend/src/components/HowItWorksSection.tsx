import React from 'react'

interface Step {
  number: number
  title: string
  description: string
  icon: React.ReactNode
}

const steps: Step[] = [
  {
    number: 1,
    title: 'Paste Your Long URL',
    description: 'Copy any long URL and paste it into our shortening tool.',
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-12 w-12"
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
    ),
  },
  {
    number: 2,
    title: 'Get a Short, Shareable Link',
    description: 'Instantly receive a compact, easy-to-share shortened URL.',
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-12 w-12"
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
    ),
  },
  {
    number: 3,
    title: 'Track Clicks and Analytics',
    description: 'Monitor your link performance with detailed click analytics.',
    icon: (
      <svg
        xmlns="http://www.w3.org/2000/svg"
        className="h-12 w-12"
        fill="none"
        viewBox="0 0 24 24"
        stroke="currentColor"
        aria-hidden="true"
      >
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
        />
      </svg>
    ),
  },
]

const HowItWorksSection: React.FC = () => {
  return (
    <section
      id="how-it-works"
      className="py-16 px-4 bg-base-200"
      aria-labelledby="how-it-works-heading"
      data-testid="how-it-works-section"
    >
      <div className="max-w-6xl mx-auto">
        <h2
          id="how-it-works-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-12"
        >
          How It Works
        </h2>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-8" role="list">
          {steps.map((step, index) => (
            <div
              key={step.number}
              className="card bg-base-100 shadow-xl hover:shadow-2xl transition-shadow duration-300"
              role="listitem"
              data-testid={`step-${step.number}`}
            >
              <div className="card-body items-center text-center">
                {/* Step Number */}
                <div
                  className="badge badge-primary badge-lg text-xl font-bold mb-4"
                  aria-label={`Step ${step.number}`}
                  data-testid={`step-number-${step.number}`}
                >
                  {step.number}
                </div>

                {/* Icon */}
                <div className="text-primary mb-4" data-testid={`step-icon-${step.number}`}>
                  {step.icon}
                </div>

                {/* Title */}
                <h3 className="card-title text-xl mb-2" data-testid={`step-title-${step.number}`}>
                  {step.title}
                </h3>

                {/* Description */}
                <p className="text-base-content/70" data-testid={`step-description-${step.number}`}>
                  {step.description}
                </p>

                {/* Connector arrow (except for last item) */}
                {index < steps.length - 1 && (
                  <div className="hidden md:flex absolute -right-4 top-1/2 transform -translate-y-1/2 text-primary">
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      className="h-8 w-8"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                      aria-hidden="true"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M9 5l7 7-7 7"
                      />
                    </svg>
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}

export default HowItWorksSection

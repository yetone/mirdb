import React from 'react'

export interface StepIndicatorProps {
  stepNumber: number
  title: string
  description: string
  isLast?: boolean
}

const StepIndicator: React.FC<StepIndicatorProps> = ({
  stepNumber,
  title,
  description,
  isLast = false,
}) => {
  return (
    <div className="flex flex-col md:flex-row items-center" data-testid={`step-${stepNumber}`}>
      <div className="flex flex-col items-center text-center max-w-xs">
        <div
          className="w-16 h-16 rounded-full bg-primary text-primary-content flex items-center justify-center text-2xl font-bold mb-4"
          data-testid={`step-number-${stepNumber}`}
          aria-label={`Step ${stepNumber}`}
        >
          {stepNumber}
        </div>
        <h3 className="text-xl font-semibold mb-2" data-testid={`step-title-${stepNumber}`}>
          {title}
        </h3>
        <p className="text-base-content/70" data-testid={`step-description-${stepNumber}`}>
          {description}
        </p>
      </div>
      {!isLast && (
        <div
          className="hidden md:flex items-center mx-6"
          data-testid={`step-connector-${stepNumber}`}
          aria-hidden="true"
        >
          <svg
            className="w-8 h-8 text-primary"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M13 7l5 5m0 0l-5 5m5-5H6"
            />
          </svg>
        </div>
      )}
      {!isLast && (
        <div
          className="flex md:hidden items-center my-4"
          data-testid={`step-connector-mobile-${stepNumber}`}
          aria-hidden="true"
        >
          <svg
            className="w-8 h-8 text-primary rotate-90"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M13 7l5 5m0 0l-5 5m5-5H6"
            />
          </svg>
        </div>
      )}
    </div>
  )
}

export default StepIndicator

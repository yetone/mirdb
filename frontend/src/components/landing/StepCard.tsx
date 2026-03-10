/**
 * Step Card Component
 * Owner: Scenario 5 - How It Works Section
 *
 * Individual step card with number, icon, and description.
 */

import { ReactNode } from 'react';

export interface StepCardProps {
  number: number;
  icon: ReactNode;
  title: string;
  description: string;
  isLast?: boolean;
}

/**
 * Individual step card for the How It Works section
 */
export function StepCard({ number, icon, title, description, isLast = false }: StepCardProps) {
  return (
    <div className="flex flex-col items-center relative" data-testid={`step-${number}`}>
      {/* Step card content */}
      <div className="flex flex-col items-center text-center p-6 rounded-xl bg-base-200/50 backdrop-blur-sm border border-base-300 w-full max-w-xs">
        {/* Step number badge */}
        <div
          className="w-12 h-12 rounded-full bg-primary text-primary-content flex items-center justify-center text-xl font-bold mb-4"
          data-testid={`step-${number}-number`}
          aria-hidden="true"
        >
          {number}
        </div>

        {/* Icon */}
        <div
          className="w-16 h-16 flex items-center justify-center text-primary mb-4"
          data-testid={`step-${number}-icon`}
          aria-hidden="true"
        >
          {icon}
        </div>

        {/* Title */}
        <h3
          className="text-lg font-semibold text-base-content mb-2"
          data-testid={`step-${number}-title`}
        >
          {title}
        </h3>

        {/* Description */}
        <p
          className="text-sm text-base-content/70"
          data-testid={`step-${number}-description`}
        >
          {description}
        </p>
      </div>

      {/* Connector arrow (hidden on mobile, shown on desktop) */}
      {!isLast && (
        <div
          className="hidden lg:flex absolute right-0 top-1/2 transform translate-x-full -translate-y-1/2 px-4"
          data-testid={`step-${number}-connector`}
          aria-hidden="true"
        >
          <svg
            className="w-8 h-8 text-primary"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
            xmlns="http://www.w3.org/2000/svg"
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
  );
}

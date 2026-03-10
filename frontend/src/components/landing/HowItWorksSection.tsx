/**
 * How It Works Section Component
 * Owner: Scenario 5 - How It Works Section
 *
 * 3-step visual guide explaining the URL shortening process.
 * Steps: Paste URL -> Get Short Link -> Track and Share
 *
 * Layout:
 * - Horizontal flow on desktop with connectors
 * - Vertical stack on mobile
 */

import type { ReactNode } from 'react';
import { HOW_IT_WORKS_CONTENT } from '../../constants/landingContent';
import { StepCard } from './StepCard';

// Step icons as SVG components
function ClipboardIcon() {
  return (
    <svg
      className="w-12 h-12"
      fill="none"
      stroke="currentColor"
      viewBox="0 0 24 24"
      xmlns="http://www.w3.org/2000/svg"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={1.5}
        d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"
      />
    </svg>
  );
}

function LinkIcon() {
  return (
    <svg
      className="w-12 h-12"
      fill="none"
      stroke="currentColor"
      viewBox="0 0 24 24"
      xmlns="http://www.w3.org/2000/svg"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={1.5}
        d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
      />
    </svg>
  );
}

function ChartIcon() {
  return (
    <svg
      className="w-12 h-12"
      fill="none"
      stroke="currentColor"
      viewBox="0 0 24 24"
      xmlns="http://www.w3.org/2000/svg"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={1.5}
        d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
      />
    </svg>
  );
}

// Map step numbers to icons
const stepIcons: Record<number, ReactNode> = {
  1: <ClipboardIcon />,
  2: <LinkIcon />,
  3: <ChartIcon />,
};

/**
 * How It Works section displaying the 3-step URL shortening process
 */
export function HowItWorksSection() {
  const { title, steps } = HOW_IT_WORKS_CONTENT;

  return (
    <section
      className="py-16 px-4"
      aria-labelledby="how-it-works-title"
      data-testid="how-it-works-section"
    >
      <div className="max-w-6xl mx-auto">
        {/* Section heading */}
        <h2
          id="how-it-works-title"
          className="text-3xl sm:text-4xl font-bold text-center text-base-content mb-12"
          data-testid="how-it-works-title"
        >
          {title}
        </h2>

        {/* Steps container - vertical on mobile, horizontal on desktop */}
        <div
          className="flex flex-col lg:flex-row items-center justify-center gap-8 lg:gap-16"
          data-testid="steps-container"
          role="list"
          aria-label="Steps to shorten a URL"
        >
          {steps.map((step, index) => (
            <div key={step.number} role="listitem">
              <StepCard
                number={step.number}
                icon={stepIcons[step.number]}
                title={step.title}
                description={step.description}
                isLast={index === steps.length - 1}
              />
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

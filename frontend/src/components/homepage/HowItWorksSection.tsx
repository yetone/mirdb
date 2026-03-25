/**
 * How It Works Section Component
 * Owner: Scenario 5 - Usage Steps Guide
 *
 * Displays 3-step usage guide:
 * 1. Paste URL
 * 2. Shorten
 * 3. Share
 *
 * Each step includes:
 * - Step number
 * - Icon
 * - Title
 * - Description
 */

import React from 'react';
import { Link2, Zap, Share2 } from 'lucide-react';
import { Step } from '../../types/homepage';

const steps: Step[] = [
  {
    number: 1,
    icon: Link2,
    title: 'Paste URL',
    description: 'Enter any long URL you want to shorten. We accept links from any website.',
  },
  {
    number: 2,
    icon: Zap,
    title: 'Shorten',
    description: 'Click the shorten button and we instantly generate a compact, trackable link.',
  },
  {
    number: 3,
    icon: Share2,
    title: 'Share',
    description: 'Share your shortened link anywhere and track clicks in real-time.',
  },
];

export function HowItWorksSection() {
  return (
    <section
      className="py-16 px-4"
      aria-label="How it works"
      data-testid="how-it-works-section"
    >
      <div className="max-w-6xl mx-auto">
        <h2 className="text-3xl md:text-4xl font-bold text-center mb-12">
          How It Works
        </h2>
        <div
          className="grid grid-cols-1 md:grid-cols-3 gap-8"
          role="list"
          aria-label="Steps"
        >
          {steps.map((step) => (
            <div
              key={step.number}
              className="flex flex-col items-center text-center p-6"
              role="listitem"
              data-testid={`step-${step.number}`}
            >
              <div
                className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center mb-4 relative"
                aria-hidden="true"
              >
                <step.icon className="w-8 h-8 text-primary" data-testid={`step-${step.number}-icon`} />
                <span
                  className="absolute -top-2 -right-2 w-8 h-8 rounded-full bg-primary text-primary-content flex items-center justify-center font-bold text-sm"
                  data-testid={`step-${step.number}-number`}
                >
                  {step.number}
                </span>
              </div>
              <h3
                className="text-xl font-semibold mb-2"
                data-testid={`step-${step.number}-title`}
              >
                {step.title}
              </h3>
              <p
                className="text-base-content/70"
                data-testid={`step-${step.number}-description`}
              >
                {step.description}
              </p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

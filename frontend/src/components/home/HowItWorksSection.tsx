/**
 * How It Works Section Component
 * Owner: Scenario 1 - Homepage Structure and Layout
 *
 * 3-step process visualization:
 * 1. Paste your long URL
 * 2. Get a short, shareable link
 * 3. Track clicks and analyze performance
 */

import { ClipboardPaste, Link2, TrendingUp } from 'lucide-react';
import type { Step } from '../../types/home';

const steps: Step[] = [
  {
    number: 1,
    title: 'Paste Your URL',
    description:
      'Simply paste your long URL into the input field. No sign-up required to get started.',
    icon: <ClipboardPaste className="w-8 h-8" />,
  },
  {
    number: 2,
    title: 'Get Short Link',
    description:
      'Click the shorten button and instantly receive a compact, easy-to-share link.',
    icon: <Link2 className="w-8 h-8" />,
  },
  {
    number: 3,
    title: 'Track Clicks',
    description:
      'Monitor your link performance with detailed analytics. See who clicks, when, and where.',
    icon: <TrendingUp className="w-8 h-8" />,
  },
];

export default function HowItWorksSection() {
  return (
    <section className="py-20 bg-base-100">
      <div className="container mx-auto px-4">
        <div className="text-center mb-12">
          <h2 className="text-3xl font-bold mb-4">How It Works</h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Get started in three simple steps. No credit card required.
          </p>
        </div>
        <div className="flex flex-col lg:flex-row items-center justify-center gap-8 lg:gap-4">
          {steps.map((step, index) => (
            <div key={step.number} className="flex items-center">
              <div
                className="flex flex-col items-center text-center max-w-xs"
                data-testid={`step-${step.number}`}
              >
                <div className="relative">
                  <div className="w-20 h-20 rounded-full bg-primary/10 flex items-center justify-center mb-4">
                    <div className="text-primary">{step.icon}</div>
                  </div>
                  <div className="absolute -top-2 -right-2 w-8 h-8 rounded-full bg-primary text-primary-content flex items-center justify-center font-bold text-sm">
                    {step.number}
                  </div>
                </div>
                <h3 className="text-xl font-semibold mb-2">{step.title}</h3>
                <p className="text-base-content/70">{step.description}</p>
              </div>
              {index < steps.length - 1 && (
                <div className="hidden lg:block w-24 h-0.5 bg-primary/30 mx-4" />
              )}
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

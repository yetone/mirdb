/**
 * Getting Started section component for MirDB homepage.
 * Owner: Scenario 4 - Getting Started Section
 *
 * Requirements:
 * - Step-by-step installation instructions (REQ-4)
 * - Memcached client connection examples
 * - Basic operations: set, get, delete
 * - Syntax-highlighted code blocks
 */

import { steps } from '../../data/gettingStarted'
import { CodeBlock } from './CodeBlock'

export function GettingStarted() {
  return (
    <section
      id="getting-started"
      className="py-20 px-4 bg-slate-800/50"
      aria-labelledby="getting-started-heading"
      role="region"
    >
      <div className="max-w-4xl mx-auto">
        <h2
          id="getting-started-heading"
          className="text-3xl md:text-4xl font-bold mb-4 text-center"
        >
          Getting Started
        </h2>
        <p className="text-slate-400 text-center mb-12 max-w-2xl mx-auto">
          Get MirDB up and running in minutes. Follow these simple steps to start
          using a persistent key-value store with memcached compatibility.
        </p>

        <ol className="space-y-8" data-testid="steps-list">
          {steps.map((step) => (
            <li
              key={step.number}
              className="bg-slate-900/50 rounded-xl p-6 border border-slate-700"
              data-testid="step-item"
            >
              <div className="flex items-start gap-4">
                <span
                  className="flex-shrink-0 w-10 h-10 rounded-full bg-primary-600 flex items-center justify-center text-lg font-bold"
                  data-testid="step-number"
                  aria-hidden="true"
                >
                  {step.number}
                </span>
                <div className="flex-1 min-w-0">
                  <h3 className="text-xl font-semibold mb-2 text-white">
                    <span className="sr-only">Step {step.number}: </span>
                    {step.title}
                  </h3>
                  <p className="text-slate-400 mb-4">{step.content}</p>
                  {step.code && <CodeBlock code={step.code} />}
                </div>
              </div>
            </li>
          ))}
        </ol>
      </div>
    </section>
  )
}

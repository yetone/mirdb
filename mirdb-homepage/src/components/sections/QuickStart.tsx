/**
 * Quick Start Section.
 * Owner: Scenario 4 - Quick Start Section
 *
 * Displays:
 * - Step-by-step setup instructions
 * - Clone, configure, run commands
 * - Link to GitHub repository
 */

import { quickStartSteps, GITHUB_REPO_URL } from '../../data/quickstart';

export function QuickStart() {
  return (
    <section id="quickstart" className="py-16 px-6" aria-labelledby="quickstart-heading">
      <div className="max-w-4xl mx-auto">
        <h2 id="quickstart-heading" className="text-3xl font-bold text-center mb-12 text-gray-900 dark:text-white">
          Quick Start
        </h2>

        <div className="space-y-8">
          {quickStartSteps.map((step) => (
            <div key={step.step} className="flex gap-4" data-testid={`quickstart-step-${step.step}`}>
              <div className="flex-shrink-0 w-10 h-10 rounded-full bg-blue-600 text-white flex items-center justify-center font-bold">
                {step.step}
              </div>
              <div className="flex-grow">
                <h3 className="text-xl font-semibold mb-3 text-gray-900 dark:text-white">
                  {step.title}
                </h3>
                <pre className="bg-gray-900 dark:bg-gray-800 text-gray-100 p-4 rounded-lg overflow-x-auto">
                  <code className="language-bash">{step.code}</code>
                </pre>
              </div>
            </div>
          ))}
        </div>

        <div className="mt-12 text-center">
          <a
            href={GITHUB_REPO_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 px-6 py-3 bg-gray-900 dark:bg-gray-700 text-white rounded-lg hover:bg-gray-800 dark:hover:bg-gray-600 transition-colors"
            data-testid="github-link"
          >
            <svg
              className="w-5 h-5"
              fill="currentColor"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path
                fillRule="evenodd"
                d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z"
                clipRule="evenodd"
              />
            </svg>
            View on GitHub
          </a>
        </div>
      </div>
    </section>
  );
}

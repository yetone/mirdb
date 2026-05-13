import React from 'react';
import { DOC_LINKS } from '../../utils/constants';
import * as Icons from 'lucide-react';

export default function Docs() {
  return (
    <section id="docs" aria-label="Documentation" className="py-16 px-4 sm:px-6 lg:px-8">
      <div className="mx-auto max-w-6xl">
        <h2 className="text-3xl sm:text-4xl font-bold text-center text-gray-900 dark:text-white">
          Documentation
        </h2>
        <p className="mt-4 text-center text-gray-600 dark:text-gray-400 max-w-2xl mx-auto">
          Explore resources to learn more about MirDB
        </p>

        <div className="mt-10 grid gap-6 md:grid-cols-3">
          {DOC_LINKS.map((link) => {
            const IconComponent = (Icons as Record<string, React.ComponentType<{ className?: string }>>)[link.icon];
            return (
              <a
                key={link.id}
                href={link.url}
                target="_blank"
                rel="noopener noreferrer"
                aria-label={`${link.title} - opens in new tab`}
                className="group block rounded-xl border border-gray-200 bg-white p-6 transition-all duration-200 hover:border-brand-400 hover:shadow-md focus-visible:border-brand-500 focus-visible:shadow-md focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-brand-500 dark:border-gray-800 dark:bg-gray-950 dark:hover:border-brand-500 dark:focus-visible:border-brand-500"
              >
                <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-lg bg-brand-50 text-brand-600 transition-colors group-hover:bg-brand-100 dark:bg-brand-950 dark:text-brand-400 dark:group-hover:bg-brand-900">
                  {IconComponent && <IconComponent className="h-6 w-6" aria-hidden="true" />}
                </div>
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                  {link.title}
                </h3>
                <p className="mt-2 text-sm leading-relaxed text-gray-600 dark:text-gray-400">
                  {link.description}
                </p>
                <span className="mt-4 inline-flex items-center gap-1 text-sm font-medium text-brand-600 transition-colors group-hover:text-brand-700 dark:text-brand-400 dark:group-hover:text-brand-300">
                  Learn more
                  <svg
                    className="h-4 w-4 transition-transform group-hover:translate-x-0.5"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    aria-hidden="true"
                  >
                    <path d="M5 12h14" />
                    <path d="m12 5 7 7-7 7" />
                  </svg>
                </span>
              </a>
            );
          })}
        </div>
      </div>
    </section>
  );
}

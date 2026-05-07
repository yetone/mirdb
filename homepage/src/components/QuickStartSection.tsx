import React from 'react';
import CodeBlock from '@/components/ui/CodeBlock';
import { QUICK_START_STEPS } from '@/lib/constants';
import { cn } from '@/lib/utils';

export default function QuickStartSection() {
  return (
    <section
      id="quick-start"
      className="py-20 px-4 sm:px-6 lg:px-8"
      data-testid="quick-start-section"
    >
      <div className="max-w-4xl mx-auto">
        <div className="text-center mb-12">
          <h2 className="text-3xl sm:text-4xl font-bold mb-4 text-[var(--foreground)]">
            Getting Started
          </h2>
          <p className="text-lg text-[var(--muted-foreground)] max-w-2xl mx-auto">
            Get MirDB running locally in minutes. Follow these simple steps to
            clone, build, and start the server.
          </p>
        </div>

        <div className="space-y-10">
          {QUICK_START_STEPS.map((step, index) => (
            <div
              key={step.title}
              className="relative"
              data-testid={`quick-start-step-${index}`}
            >
              {/* Step number + title */}
              <div className="flex items-start gap-4 mb-3">
                <span
                  className={cn(
                    'flex-shrink-0 w-8 h-8 rounded-full',
                    'bg-brand-600 text-white',
                    'flex items-center justify-center',
                    'text-sm font-bold'
                  )}
                  aria-hidden="true"
                >
                  {index + 1}
                </span>
                <div>
                  <h3 className="text-xl font-semibold text-[var(--foreground)]">
                    {step.title}
                  </h3>
                  <p className="text-[var(--muted-foreground)] mt-1">
                    {step.description}
                  </p>
                </div>
              </div>

              {/* Code block */}
              <div className="ml-12">
                <CodeBlock
                  code={step.code}
                  language={step.language}
                  data-testid={`code-block-step-${index}`}
                />
              </div>
            </div>
          ))}
        </div>

        {/* Configuration note */}
        <div className="mt-12 p-4 rounded-lg border border-[var(--border)] bg-[var(--card)]">
          <p className="text-sm text-[var(--muted-foreground)]">
            <strong className="text-[var(--foreground)]">Default settings:</strong>{' '}
            The server listens on{' '}
            <code className="px-1.5 py-0.5 rounded bg-[var(--code-bg)] text-[var(--code-fg)] text-xs">
              0.0.0.0:12333
            </code>{' '}
            and uses{' '}
            <code className="px-1.5 py-0.5 rounded bg-[var(--code-bg)] text-[var(--code-fg)] text-xs">
              /tmp/mirdb
            </code>{' '}
            as the working directory. Customize these in{' '}
            <code className="px-1.5 py-0.5 rounded bg-[var(--code-bg)] text-[var(--code-fg)] text-xs">
              etc/mirdb.toml
            </code>
            .
          </p>
        </div>
      </div>
    </section>
  );
}

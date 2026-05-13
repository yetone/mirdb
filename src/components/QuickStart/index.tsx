import { Check, Copy } from 'lucide-react';
import { QUICK_START_EXAMPLES } from '../../utils/constants';
import { useClipboard } from '../../hooks/useClipboard';
import { cn } from '../../utils/cn';

function CodeBlock({ title, code, language }: { title: string; code: string; language: string }) {
  const { copy, copied } = useClipboard();

  return (
    <div className="rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 overflow-hidden">
      <div className="flex items-center justify-between px-4 py-2 bg-gray-50 dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700">
        <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
          {title}
        </span>
        <button
          type="button"
          onClick={() => copy(code)}
          aria-label={copied ? 'Code copied to clipboard' : `Copy ${title} code to clipboard`}
          className={cn(
            'inline-flex items-center gap-1.5 px-3 py-1 text-xs font-medium rounded-md transition-colors',
            'focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-brand-500',
            copied
              ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
              : 'bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-300 dark:hover:bg-gray-600',
          )}
        >
          {copied ? <Check size={14} aria-hidden="true" /> : <Copy size={14} aria-hidden="true" />}
          {copied ? 'Copied' : 'Copy'}
        </button>
      </div>
      <pre className="p-4 overflow-x-auto text-sm font-mono text-gray-800 dark:text-gray-200 bg-white dark:bg-gray-800 leading-relaxed">
        <code>{code}</code>
      </pre>
    </div>
  );
}

export default function QuickStart() {
  return (
    <section
      id="quickstart"
      aria-label="Quick Start Guide"
      className="py-16 px-4 sm:px-6 lg:px-8 max-w-4xl mx-auto"
    >
      <h2 className="text-3xl sm:text-4xl font-bold text-center mb-4 text-gray-900 dark:text-white">
        Quick Start
      </h2>
      <p className="text-center text-gray-600 dark:text-gray-400 mb-10 max-w-2xl mx-auto">
        Get started with MirDB in minutes using these simple commands
      </p>

      <div className="space-y-6" role="list" aria-label="Code examples">
        {QUICK_START_EXAMPLES.map((example) => (
          <div key={example.id} role="listitem">
            <CodeBlock
              title={example.title}
              code={example.code}
              language={example.language}
            />
          </div>
        ))}
      </div>

      <div
        role="status"
        aria-live="polite"
        aria-atomic="true"
        className="sr-only"
        id="copy-announcement"
      />
    </section>
  );
}

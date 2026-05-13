import React from 'react';
import { Play, Monitor, Smartphone } from 'lucide-react';
import { DEMO_CAPTIONS } from '../../utils/constants';
import { cn } from '../../utils/cn';
import { useMediaQuery } from '../../hooks/useMediaQuery';

export default function Demo() {
  const isMobile = useMediaQuery('(max-width: 640px)');

  return (
    <section
      id="demo"
      aria-labelledby="demo-heading"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-gray-50 dark:bg-gray-900 transition-colors"
    >
      <div className="max-w-4xl mx-auto">
        <div className="text-center mb-10">
          <h2
            id="demo-heading"
            className="text-3xl sm:text-4xl font-bold text-gray-900 dark:text-white mb-4"
          >
            See MirDB in Action
          </h2>
          <p className="text-lg text-gray-600 dark:text-gray-400 max-w-2xl mx-auto">
            Watch how MirDB handles commands just like Memcached — with the added benefit of persistent storage.
          </p>
        </div>

        <div
          className={cn(
            'rounded-xl border overflow-hidden shadow-lg',
            'bg-white dark:bg-gray-800',
            'border-gray-200 dark:border-gray-700',
            'transition-colors',
          )}
        >
          <div className="p-4 sm:p-6">
            <div
              className={cn(
                'rounded-lg overflow-hidden',
                'bg-gray-900 dark:bg-gray-950',
                'ring-1 ring-gray-200 dark:ring-gray-700',
              )}
            >
              <img
                src="/assets/usage.gif"
                alt="Animated demonstration of MirDB usage showing SET, GET, and DELETE commands in a terminal"
                className="w-full h-auto max-w-full block"
                loading="lazy"
              />
            </div>
          </div>

          <div
            className={cn(
              'px-4 pb-4 sm:px-6 sm:pb-6',
              'border-t border-gray-100 dark:border-gray-700',
            )}
          >
            <div className={cn(
              'flex items-center gap-2 pt-4 mb-4',
              'text-sm font-medium text-gray-700 dark:text-gray-300',
            )}>
              {isMobile ? (
                <Smartphone size={16} className="shrink-0" />
              ) : (
                <Monitor size={16} className="shrink-0" />
              )}
              <span>Command walkthrough</span>
            </div>

            <ol
              aria-label="Demo command explanations"
              className="space-y-3"
            >
              {DEMO_CAPTIONS.map((caption, index) => (
                <li
                  key={caption.id}
                  className="flex items-start gap-3 text-sm"
                >
                  <span
                    className={cn(
                      'flex-shrink-0 w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold',
                      'bg-brand-100 dark:bg-brand-900/30',
                      'text-brand-700 dark:text-brand-300',
                    )}
                    aria-hidden="true"
                  >
                    {index + 1}
                  </span>
                  <span className="text-gray-700 dark:text-gray-300 pt-0.5">
                    {caption.text}
                  </span>
                </li>
              ))}
            </ol>
          </div>
        </div>
      </div>
    </section>
  );
}

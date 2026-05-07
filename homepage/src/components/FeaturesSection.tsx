import { FEATURES } from '@/lib/constants';
import { cn } from '@/lib/utils';

function FeatureIcon({ icon }: { icon: string }) {
  const icons: Record<string, React.ReactNode> = {
    protocol: (
      <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
      </svg>
    ),
    disk: (
      <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />
      </svg>
    ),
    tree: (
      <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
      </svg>
    ),
  };

  return (
    <div className="w-12 h-12 rounded-lg bg-brand-500/10 text-brand-500 flex items-center justify-center">
      {icons[icon] || icons.protocol}
    </div>
  );
}

export default function FeaturesSection() {
  return (
    <section
      id="features"
      className="py-20 px-4 sm:px-6 lg:px-8"
      data-testid="features-section"
    >
      <div className="max-w-7xl mx-auto">
        <div className="text-center mb-16">
          <h2
            className={cn(
              'text-3xl sm:text-4xl font-bold',
              'text-[var(--foreground)]',
              'mb-4'
            )}
          >
            Key Features
          </h2>
          <p className="text-lg text-[var(--muted-foreground)] max-w-2xl mx-auto">
            Everything you need for high-performance persistent key-value storage.
          </p>
        </div>

        {/* Feature cards grid - 3 columns on desktop */}
        <div
          className={cn(
            'grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3',
            'gap-8'
          )}
          data-testid="features-grid"
        >
          {FEATURES.map((feature) => (
            <div
              key={feature.title}
              className={cn(
                'p-6 rounded-xl',
                'border border-[var(--border)]',
                'bg-[var(--card)]',
                'hover:border-brand-500/30 transition-colors'
              )}
              data-testid="feature-card"
            >
              <FeatureIcon icon={feature.icon} />
              <h3 className="mt-4 text-xl font-semibold text-[var(--foreground)]">
                {feature.title}
              </h3>
              <p className="mt-2 text-[var(--muted-foreground)] leading-relaxed">
                {feature.description}
              </p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

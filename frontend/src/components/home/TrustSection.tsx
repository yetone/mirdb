import React from 'react';
import { GlassMorphismCard } from '../common/GlassMorphismCard';

interface TrustMetric {
  id: string;
  value: string;
  label: string;
  icon: React.ReactNode;
}

const UsersIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor" data-testid="metric-icon-svg">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z" />
  </svg>
);

const LinkIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor" data-testid="metric-icon-svg">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
  </svg>
);

const ClickIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor" data-testid="metric-icon-svg">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 15l-2 5L9 9l11 4-5 2zm0 0l5 5M7.188 2.239l.777 2.897M5.136 7.965l-2.898-.777M13.95 4.05l-2.122 2.122m-5.657 5.656l-2.12 2.122" />
  </svg>
);

const UptimeIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor" data-testid="metric-icon-svg">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
  </svg>
);

const trustMetrics: TrustMetric[] = [
  {
    id: 'active-users',
    value: '10,000+',
    label: 'Active Users',
    icon: <UsersIcon />,
  },
  {
    id: 'links-created',
    value: '1,000,000+',
    label: 'Links Created',
    icon: <LinkIcon />,
  },
  {
    id: 'clicks-tracked',
    value: '50,000,000+',
    label: 'Clicks Tracked',
    icon: <ClickIcon />,
  },
  {
    id: 'uptime',
    value: '99.9%',
    label: 'Uptime',
    icon: <UptimeIcon />,
  },
];

export function TrustSection() {
  return (
    <section className="py-16 px-4 bg-base-200/30" data-testid="trust-section">
      <div className="max-w-6xl mx-auto">
        <h2 className="text-3xl font-bold text-center mb-4" data-testid="trust-heading">
          Trusted by Thousands
        </h2>
        <p className="text-base-content/70 text-center mb-12 max-w-2xl mx-auto" data-testid="trust-subheading">
          Join thousands of users who trust us to manage their links and track their performance.
        </p>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
          {trustMetrics.map((metric) => (
            <GlassMorphismCard
              key={metric.id}
              testId={`trust-metric-${metric.id}`}
            >
              <div className="flex flex-col items-center text-center">
                <div className="text-primary mb-3" data-testid={`metric-icon-${metric.id}`}>
                  {metric.icon}
                </div>
                <div
                  className="text-3xl font-bold text-primary mb-1"
                  data-testid={`metric-value-${metric.id}`}
                >
                  {metric.value}
                </div>
                <div
                  className="text-sm text-base-content/70"
                  data-testid={`metric-label-${metric.id}`}
                >
                  {metric.label}
                </div>
              </div>
            </GlassMorphismCard>
          ))}
        </div>
      </div>
    </section>
  );
}

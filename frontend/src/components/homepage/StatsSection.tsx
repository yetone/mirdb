/**
 * Stats/Social Proof Section Component
 * Owner: Scenario 11 - Social Proof Elements
 *
 * Displays platform statistics:
 * - Total users registered
 * - Total links shortened
 *
 * Uses useStats hook to fetch data, with graceful fallback.
 */

import React from 'react';
import { Users, Link2 } from 'lucide-react';
import { GlassMorphismCard } from '../GlassMorphismCard';
import { useStats, StatsResult } from '../../hooks/useStats';

interface StatItemProps {
  icon: React.ReactNode;
  value: number;
  label: string;
  testId: string;
}

function StatItem({ icon, value, label, testId }: StatItemProps) {
  const formattedValue = value >= 1000
    ? `${(value / 1000).toFixed(1)}K+`
    : value.toLocaleString();

  return (
    <div className="text-center" data-testid={testId}>
      <div
        className="w-12 h-12 mx-auto rounded-full bg-primary/20 flex items-center justify-center mb-3"
        data-testid={`${testId}-icon`}
      >
        {icon}
      </div>
      <div
        className="text-3xl md:text-4xl font-bold text-primary mb-1"
        data-testid={`${testId}-value`}
      >
        {formattedValue}
      </div>
      <div
        className="text-sm text-base-content/70"
        data-testid={`${testId}-label`}
      >
        {label}
      </div>
    </div>
  );
}

interface StatsSectionProps {
  /** Optional override for stats (used in testing) */
  statsOverride?: StatsResult;
}

export function StatsSection({ statsOverride }: StatsSectionProps) {
  const hookResult = useStats();
  const { stats, isLoading, error } = statsOverride ?? hookResult;

  // Hide section on error (graceful fallback)
  if (error) {
    return (
      <section
        className="py-16 px-4"
        aria-labelledby="stats-heading"
        data-testid="stats-section"
        data-error="true"
      >
        <div className="max-w-4xl mx-auto text-center">
          <p className="text-base-content/50" data-testid="stats-error">
            Statistics temporarily unavailable
          </p>
        </div>
      </section>
    );
  }

  // Show loading state
  if (isLoading || !stats) {
    return (
      <section
        className="py-16 px-4"
        aria-labelledby="stats-heading"
        data-testid="stats-section"
        data-loading="true"
      >
        <div className="max-w-4xl mx-auto">
          <h2
            id="stats-heading"
            className="text-2xl md:text-3xl font-bold text-center mb-10 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
          >
            Trusted by Thousands
          </h2>
          <div className="flex justify-center gap-8 md:gap-16">
            <div className="animate-pulse" data-testid="stats-loading">
              <div className="w-12 h-12 mx-auto rounded-full bg-base-content/10 mb-3"></div>
              <div className="h-8 w-16 mx-auto bg-base-content/10 rounded mb-1"></div>
              <div className="h-4 w-20 mx-auto bg-base-content/10 rounded"></div>
            </div>
            <div className="animate-pulse">
              <div className="w-12 h-12 mx-auto rounded-full bg-base-content/10 mb-3"></div>
              <div className="h-8 w-16 mx-auto bg-base-content/10 rounded mb-1"></div>
              <div className="h-4 w-20 mx-auto bg-base-content/10 rounded"></div>
            </div>
          </div>
        </div>
      </section>
    );
  }

  return (
    <section
      className="py-16 px-4"
      aria-labelledby="stats-heading"
      data-testid="stats-section"
    >
      <div className="max-w-4xl mx-auto">
        <h2
          id="stats-heading"
          className="text-2xl md:text-3xl font-bold text-center mb-10 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
        >
          Trusted by Thousands
        </h2>
        <GlassMorphismCard
          className="flex flex-col md:flex-row justify-center items-center gap-8 md:gap-16 py-8"
          data-testid="stats-card"
        >
          <StatItem
            icon={<Users className="w-6 h-6 text-primary" aria-hidden="true" />}
            value={stats.userCount}
            label="Registered Users"
            testId="stat-users"
          />
          <div className="hidden md:block w-px h-20 bg-base-content/20" aria-hidden="true" />
          <StatItem
            icon={<Link2 className="w-6 h-6 text-primary" aria-hidden="true" />}
            value={stats.linksCreated}
            label="Links Created"
            testId="stat-links"
          />
        </GlassMorphismCard>
      </div>
    </section>
  );
}

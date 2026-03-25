/**
 * Analytics Preview Component
 * Owner: Scenario 6 - Analytics Preview Section
 *
 * Displays sample analytics visualization:
 * - Mock chart/graph showing click data over a week
 * - Sample metrics (clicks, visitors, conversion rate)
 * - Theme-adaptive colors
 * - Hover interactions on chart bars
 */

import React, { useState } from 'react';
import { TrendingUp, MousePointerClick, Users, Target } from 'lucide-react';
import { GlassMorphismCard } from '../GlassMorphismCard';

// Sample data for the chart
const SAMPLE_CHART_DATA = [
  { day: 'Mon', clicks: 245 },
  { day: 'Tue', clicks: 312 },
  { day: 'Wed', clicks: 287 },
  { day: 'Thu', clicks: 456 },
  { day: 'Fri', clicks: 523 },
  { day: 'Sat', clicks: 389 },
  { day: 'Sun', clicks: 276 },
];

const SAMPLE_METRICS = {
  totalClicks: 2488,
  uniqueVisitors: 1847,
  conversionRate: 12.4,
  clickGrowth: 23.5,
  visitorGrowth: 18.2,
  conversionGrowth: 5.7,
};

interface ChartBarProps {
  height: number;
  index: number;
  clicks: number;
  onHover: (index: number | null) => void;
  isHovered: boolean;
}

function ChartBar({ height, index, clicks, onHover, isHovered }: ChartBarProps) {
  return (
    <div
      data-testid={`chart-bar-${index}`}
      className={`
        w-full rounded-t-lg transition-all duration-300 cursor-pointer
        bg-gradient-to-t from-primary to-primary/60
        hover:from-primary hover:to-secondary
        ${isHovered ? 'scale-105 shadow-lg shadow-primary/30' : ''}
      `}
      style={{ height: `${height}%`, minHeight: '8px' }}
      onMouseEnter={() => onHover(index)}
      onMouseLeave={() => onHover(null)}
      role="presentation"
      aria-hidden="true"
    />
  );
}

interface MetricCardProps {
  testId: string;
  icon: React.ReactNode;
  label: string;
  value: string;
  growth: number;
}

function MetricCard({ testId, icon, label, value, growth }: MetricCardProps) {
  const isPositive = growth >= 0;

  return (
    <div
      data-testid={testId}
      className="flex items-center gap-4 p-4 rounded-xl bg-base-200/50 backdrop-blur-sm"
    >
      <div className="p-3 rounded-lg bg-primary/10 text-primary">
        {icon}
      </div>
      <div className="flex-1">
        <p className="text-sm text-base-content/60">{label}</p>
        <p className="text-2xl font-bold text-base-content">{value}</p>
      </div>
      <div
        data-testid={`growth-indicator-${testId}`}
        className={`flex items-center gap-1 text-sm font-medium ${
          isPositive ? 'text-success' : 'text-error'
        }`}
      >
        <TrendingUp
          size={16}
          className={isPositive ? '' : 'rotate-180'}
        />
        <span>{isPositive ? '+' : ''}{growth}%</span>
      </div>
    </div>
  );
}

export function AnalyticsPreview() {
  const [hoveredBar, setHoveredBar] = useState<number | null>(null);

  const maxClicks = Math.max(...SAMPLE_CHART_DATA.map(d => d.clicks));

  return (
    <section
      data-testid="analytics-preview-section"
      className="py-16 px-4 md:px-8"
      aria-label="Analytics preview"
    >
      <div className="max-w-6xl mx-auto">
        {/* Section Header */}
        <div className="text-center mb-12">
          <h2 className="text-3xl md:text-4xl font-bold mb-4 text-base-content">
            Powerful Analytics at Your Fingertips
          </h2>
          <p className="text-base-content/70 max-w-2xl mx-auto">
            Track every click, understand your audience, and optimize your links with
            our comprehensive analytics dashboard.
          </p>
        </div>

        <div className="grid lg:grid-cols-2 gap-8">
          {/* Chart Section */}
          <GlassMorphismCard className="p-6">
            <div
              data-testid="analytics-chart"
              role="img"
              aria-label="Weekly click statistics chart showing click trends from Monday to Sunday"
              className="relative"
            >
              <h3 className="text-lg font-semibold mb-6 text-base-content">
                Clicks This Week
              </h3>

              {/* Chart Container */}
              <div className="relative h-64">
                {/* Y-axis grid lines */}
                <div className="absolute inset-0 flex flex-col justify-between pointer-events-none">
                  {[0, 1, 2, 3, 4].map(i => (
                    <div
                      key={i}
                      className="border-t border-base-content/10 w-full"
                    />
                  ))}
                </div>

                {/* Bars */}
                <div className="absolute inset-0 flex items-end justify-around gap-2 pt-4 pb-8">
                  {SAMPLE_CHART_DATA.map((data, index) => (
                    <div key={data.day} className="flex-1 flex flex-col items-center h-full">
                      <div className="flex-1 w-full flex items-end justify-center px-1">
                        <ChartBar
                          height={(data.clicks / maxClicks) * 100}
                          index={index}
                          clicks={data.clicks}
                          onHover={setHoveredBar}
                          isHovered={hoveredBar === index}
                        />
                      </div>
                      <span className="mt-2 text-xs text-base-content/60 font-medium">
                        {data.day}
                      </span>
                    </div>
                  ))}
                </div>

                {/* Tooltip */}
                {hoveredBar !== null && (
                  <div
                    data-testid="chart-tooltip"
                    className="absolute top-2 left-1/2 transform -translate-x-1/2
                      bg-base-300 text-base-content px-3 py-1 rounded-lg shadow-lg
                      text-sm font-medium z-10"
                  >
                    {SAMPLE_CHART_DATA[hoveredBar].day}: {SAMPLE_CHART_DATA[hoveredBar].clicks} clicks
                  </div>
                )}
              </div>
            </div>
          </GlassMorphismCard>

          {/* Metrics Section */}
          <div className="flex flex-col justify-center">
            <div
              data-testid="metrics-grid"
              className="grid gap-4"
            >
              <MetricCard
                testId="metric-clicks"
                icon={<MousePointerClick size={24} />}
                label="Total Clicks"
                value={SAMPLE_METRICS.totalClicks.toLocaleString()}
                growth={SAMPLE_METRICS.clickGrowth}
              />
              <MetricCard
                testId="metric-visitors"
                icon={<Users size={24} />}
                label="Unique Visitors"
                value={SAMPLE_METRICS.uniqueVisitors.toLocaleString()}
                growth={SAMPLE_METRICS.visitorGrowth}
              />
              <MetricCard
                testId="metric-conversion"
                icon={<Target size={24} />}
                label="Conversion Rate"
                value={`${SAMPLE_METRICS.conversionRate}%`}
                growth={SAMPLE_METRICS.conversionGrowth}
              />
            </div>

            <p className="mt-6 text-sm text-base-content/50 text-center">
              Sample data for demonstration purposes
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}

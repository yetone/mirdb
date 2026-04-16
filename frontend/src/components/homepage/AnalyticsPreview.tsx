/**
 * Analytics Preview Section Component.
 * Owner: Scenario 5 - Analytics Preview Section
 *
 * Displays sample analytics charts to showcase the product's analytics capabilities.
 * Uses Recharts for chart visualizations with theme-aware colors.
 *
 * Charts included:
 * - Line/Bar chart showing "clicks over time"
 * - Pie/Donut chart showing "browser distribution"
 */

import React from 'react';
import { motion } from 'framer-motion';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Legend,
} from 'recharts';
import { GlassMorphismCard } from '../GlassMorphismCard';
import {
  clicksOverTimeData,
  browserDistributionData,
} from '../../data/sampleAnalytics';

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.2,
    },
  },
};

const chartVariants = {
  hidden: { opacity: 0, y: 30 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.6,
      ease: 'easeOut',
    },
  },
};

export const AnalyticsPreview: React.FC = () => {
  return (
    <section
      data-testid="analytics-preview-section"
      className="py-16 px-4 md:px-8"
      aria-labelledby="analytics-preview-heading"
    >
      <div className="max-w-7xl mx-auto">
        <h2
          id="analytics-preview-heading"
          className="text-3xl md:text-4xl font-bold text-center mb-4"
        >
          Powerful Analytics
        </h2>
        <p
          data-testid="analytics-preview-description"
          className="text-base-content/80 text-center mb-12 max-w-2xl mx-auto"
        >
          Track every click with detailed insights. See where your audience comes
          from and understand how they engage with your links.
        </p>

        <motion.div
          className="grid grid-cols-1 lg:grid-cols-2 gap-8"
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: '-50px' }}
        >
          {/* Clicks Over Time Chart */}
          <motion.div variants={chartVariants}>
            <GlassMorphismCard
              data-testid="clicks-over-time-chart"
              className="p-6"
            >
              <h3
                data-testid="clicks-over-time-label"
                className="text-xl font-semibold mb-4"
              >
                Clicks Over Time
              </h3>
              <div
                data-testid="clicks-over-time-container"
                className="h-64"
                role="img"
                aria-label="Area chart showing clicks over time for the past week"
              >
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart
                    data={clicksOverTimeData}
                    margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
                  >
                    <defs>
                      <linearGradient id="colorClicks" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="hsl(var(--p))" stopOpacity={0.8} />
                        <stop offset="95%" stopColor="hsl(var(--p))" stopOpacity={0.1} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid
                      strokeDasharray="3 3"
                      className="stroke-base-content/10"
                    />
                    <XAxis
                      dataKey="date"
                      className="text-base-content/70"
                      tick={{ fill: 'currentColor', fontSize: 12 }}
                      axisLine={{ stroke: 'currentColor', strokeOpacity: 0.2 }}
                      tickLine={{ stroke: 'currentColor', strokeOpacity: 0.2 }}
                    />
                    <YAxis
                      className="text-base-content/70"
                      tick={{ fill: 'currentColor', fontSize: 12 }}
                      axisLine={{ stroke: 'currentColor', strokeOpacity: 0.2 }}
                      tickLine={{ stroke: 'currentColor', strokeOpacity: 0.2 }}
                    />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: 'hsl(var(--b1))',
                        border: '1px solid hsl(var(--bc) / 0.1)',
                        borderRadius: '8px',
                        boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)',
                      }}
                      labelStyle={{ color: 'hsl(var(--bc))' }}
                    />
                    <Area
                      type="monotone"
                      dataKey="clicks"
                      stroke="hsl(var(--p))"
                      strokeWidth={2}
                      fillOpacity={1}
                      fill="url(#colorClicks)"
                      name="Clicks"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
              <p
                data-testid="clicks-chart-sample-note"
                className="text-sm text-base-content/50 mt-4 text-center"
              >
                Sample data showing weekly click trends
              </p>
            </GlassMorphismCard>
          </motion.div>

          {/* Browser Distribution Chart */}
          <motion.div variants={chartVariants}>
            <GlassMorphismCard
              data-testid="browser-distribution-chart"
              className="p-6"
            >
              <h3
                data-testid="browser-distribution-label"
                className="text-xl font-semibold mb-4"
              >
                Browser Distribution
              </h3>
              <div
                data-testid="browser-distribution-container"
                className="h-64"
                role="img"
                aria-label="Pie chart showing browser distribution of visitors"
              >
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={browserDistributionData}
                      cx="50%"
                      cy="50%"
                      innerRadius={60}
                      outerRadius={80}
                      paddingAngle={2}
                      dataKey="value"
                      nameKey="browser"
                      label={({ browser, value }) => `${browser}: ${value}%`}
                      labelLine={{ stroke: 'currentColor', strokeOpacity: 0.3 }}
                    >
                      {browserDistributionData.map((entry, index) => (
                        <Cell
                          key={`cell-${index}`}
                          fill={entry.color}
                          data-testid={`browser-cell-${entry.browser.toLowerCase()}`}
                        />
                      ))}
                    </Pie>
                    <Tooltip
                      contentStyle={{
                        backgroundColor: 'hsl(var(--b1))',
                        border: '1px solid hsl(var(--bc) / 0.1)',
                        borderRadius: '8px',
                        boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)',
                      }}
                      formatter={(value: number) => [`${value}%`, 'Share']}
                    />
                    <Legend
                      verticalAlign="bottom"
                      height={36}
                      formatter={(value) => (
                        <span className="text-base-content/70">{value}</span>
                      )}
                    />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <p
                data-testid="browser-chart-sample-note"
                className="text-sm text-base-content/50 mt-4 text-center"
              >
                Sample data showing visitor browser preferences
              </p>
            </GlassMorphismCard>
          </motion.div>
        </motion.div>
      </div>
    </section>
  );
};

export default AnalyticsPreview;

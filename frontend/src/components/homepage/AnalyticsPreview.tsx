/**
 * Analytics Preview Component
 * Owner: Scenario 3 - Analytics Preview Implementation
 *
 * Showcases analytics capabilities with:
 * - Visual mockup/screenshot of analytics dashboard
 * - Key statistics display (clicks, referrers, browsers, locations)
 * - Recharts visualization examples
 * - Wrapped in GlassMorphismCard
 *
 * Requirements covered: REQ-4
 */

import React from 'react';
import {
  AreaChart,
  Area,
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts';
import { TrendingUp, Users, Globe, Share2 } from 'lucide-react';
import { GlassMorphismCard } from '../GlassMorphismCard';

// Sample analytics data for preview
const clicksOverTimeData = [
  { date: 'Mon', clicks: 1200 },
  { date: 'Tue', clicks: 1800 },
  { date: 'Wed', clicks: 2100 },
  { date: 'Thu', clicks: 1950 },
  { date: 'Fri', clicks: 2400 },
  { date: 'Sat', clicks: 1700 },
  { date: 'Sun', clicks: 1697 },
];

const referrerData = [
  { name: 'Google', value: 4521 },
  { name: 'Twitter', value: 2834 },
  { name: 'Facebook', value: 1923 },
  { name: 'Direct', value: 3569 },
];

const locationData = [
  { name: 'United States', clicks: 5234 },
  { name: 'United Kingdom', clicks: 2145 },
  { name: 'Germany', clicks: 1823 },
  { name: 'Canada', clicks: 1456 },
  { name: 'Australia', clicks: 1189 },
];

const browserData = [
  { name: 'Chrome', value: 45 },
  { name: 'Firefox', value: 25 },
  { name: 'Safari', value: 20 },
  { name: 'Edge', value: 10 },
];

const COLORS = ['#6366f1', '#8b5cf6', '#a855f7', '#d946ef'];

interface StatCardProps {
  icon: React.ReactNode;
  label: string;
  value: string;
}

const StatCard: React.FC<StatCardProps> = ({ icon, label, value }) => (
  <div className="flex items-center gap-3 p-4 bg-base-200/50 rounded-xl">
    <div className="p-2 bg-primary/20 rounded-lg text-primary">{icon}</div>
    <div>
      <p className="text-sm text-base-content/60">{label}</p>
      <p className="text-xl font-bold">{value}</p>
    </div>
  </div>
);

export const AnalyticsPreview: React.FC = () => {
  return (
    <section
      className="py-20 px-4 md:px-8"
      data-testid="analytics-preview-section"
      aria-labelledby="analytics-heading"
    >
      <div className="max-w-7xl mx-auto">
        {/* Section Heading */}
        <div className="text-center mb-12">
          <h2
            id="analytics-heading"
            className="text-3xl md:text-4xl font-bold mb-4"
            data-testid="analytics-heading"
          >
            Powerful Analytics at Your Fingertips
          </h2>
          <p className="text-lg text-base-content/70 max-w-2xl mx-auto">
            Get detailed insights into your link performance with real-time analytics,
            geographic data, and traffic source breakdowns.
          </p>
        </div>

        {/* Analytics Dashboard Preview */}
        <GlassMorphismCard className="p-6 md:p-8">
          {/* Statistics Row */}
          <div
            className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8"
            data-testid="statistics-display"
          >
            <StatCard
              icon={<TrendingUp className="w-5 h-5" />}
              label="Total Clicks"
              value="12,847"
            />
            <StatCard
              icon={<Users className="w-5 h-5" />}
              label="Unique Visitors"
              value="8,392"
            />
            <StatCard
              icon={<Globe className="w-5 h-5" />}
              label="Countries"
              value="42"
            />
            <StatCard
              icon={<Share2 className="w-5 h-5" />}
              label="Referral Sources"
              value="15"
            />
          </div>

          {/* Charts Grid */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Clicks Over Time Chart */}
            <div
              className="bg-base-200/30 rounded-xl p-4"
              data-testid="clicks-chart"
            >
              <h3 className="text-lg font-semibold mb-4">Clicks Over Time</h3>
              <ResponsiveContainer width="100%" height={200}>
                <AreaChart data={clicksOverTimeData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="currentColor" opacity={0.1} />
                  <XAxis dataKey="date" stroke="currentColor" opacity={0.5} />
                  <YAxis stroke="currentColor" opacity={0.5} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: 'rgba(0,0,0,0.8)',
                      border: 'none',
                      borderRadius: '8px',
                    }}
                  />
                  <Area
                    type="monotone"
                    dataKey="clicks"
                    stroke="#6366f1"
                    fill="url(#colorClicks)"
                  />
                  <defs>
                    <linearGradient id="colorClicks" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#6366f1" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#6366f1" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                </AreaChart>
              </ResponsiveContainer>
            </div>

            {/* Referrer Sources Chart */}
            <div
              className="bg-base-200/30 rounded-xl p-4"
              data-testid="referrer-chart"
            >
              <h3 className="text-lg font-semibold mb-4">Traffic Sources</h3>
              <ResponsiveContainer width="100%" height={200}>
                <PieChart>
                  <Pie
                    data={referrerData}
                    cx="50%"
                    cy="50%"
                    innerRadius={50}
                    outerRadius={80}
                    paddingAngle={5}
                    dataKey="value"
                    label={({ name, percent }) =>
                      `${name} ${(percent * 100).toFixed(0)}%`
                    }
                    labelLine={false}
                  >
                    {referrerData.map((_, index) => (
                      <Cell
                        key={`cell-${index}`}
                        fill={COLORS[index % COLORS.length]}
                      />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{
                      backgroundColor: 'rgba(0,0,0,0.8)',
                      border: 'none',
                      borderRadius: '8px',
                    }}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>

            {/* Geographic Data Chart */}
            <div
              className="bg-base-200/30 rounded-xl p-4"
              data-testid="location-chart"
            >
              <h3 className="text-lg font-semibold mb-4">Top Locations</h3>
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={locationData} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" stroke="currentColor" opacity={0.1} />
                  <XAxis type="number" stroke="currentColor" opacity={0.5} />
                  <YAxis
                    dataKey="name"
                    type="category"
                    stroke="currentColor"
                    opacity={0.5}
                    width={100}
                    tick={{ fontSize: 12 }}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: 'rgba(0,0,0,0.8)',
                      border: 'none',
                      borderRadius: '8px',
                    }}
                  />
                  <Bar dataKey="clicks" fill="#8b5cf6" radius={[0, 4, 4, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>

            {/* Browser Breakdown Chart */}
            <div
              className="bg-base-200/30 rounded-xl p-4"
              data-testid="browser-chart"
            >
              <h3 className="text-lg font-semibold mb-4">Browser Breakdown</h3>
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={browserData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="currentColor" opacity={0.1} />
                  <XAxis dataKey="name" stroke="currentColor" opacity={0.5} />
                  <YAxis stroke="currentColor" opacity={0.5} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: 'rgba(0,0,0,0.8)',
                      border: 'none',
                      borderRadius: '8px',
                    }}
                  />
                  <Legend />
                  <Bar dataKey="value" name="Usage %" fill="#a855f7" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </GlassMorphismCard>
      </div>
    </section>
  );
};

export default AnalyticsPreview;

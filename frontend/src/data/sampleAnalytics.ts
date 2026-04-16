/**
 * Sample Analytics Data.
 * Owner: Scenario 5 - Analytics Preview Section
 *
 * Provides realistic sample data for analytics preview charts.
 * Data demonstrates the value of analytics features to new visitors.
 */

export interface ClicksOverTimeDataPoint {
  date: string;
  clicks: number;
}

export interface BrowserDistributionDataPoint {
  browser: string;
  value: number;
  color: string;
}

/**
 * Sample data for clicks over time line/bar chart.
 * Shows the last 7 days of click activity.
 */
export const clicksOverTimeData: ClicksOverTimeDataPoint[] = [
  { date: 'Mon', clicks: 245 },
  { date: 'Tue', clicks: 312 },
  { date: 'Wed', clicks: 287 },
  { date: 'Thu', clicks: 423 },
  { date: 'Fri', clicks: 389 },
  { date: 'Sat', clicks: 156 },
  { date: 'Sun', clicks: 198 },
];

/**
 * Sample data for browser distribution pie/donut chart.
 * Shows realistic browser usage percentages.
 */
export const browserDistributionData: BrowserDistributionDataPoint[] = [
  { browser: 'Chrome', value: 45, color: '#4285F4' },
  { browser: 'Safari', value: 25, color: '#0FB5EE' },
  { browser: 'Firefox', value: 15, color: '#FF7139' },
  { browser: 'Edge', value: 10, color: '#0078D7' },
  { browser: 'Other', value: 5, color: '#6B7280' },
];

/**
 * Total clicks across all sample data
 */
export const totalSampleClicks = clicksOverTimeData.reduce(
  (sum, day) => sum + day.clicks,
  0
);

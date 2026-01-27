/**
 * Mock data for homepage tests.
 * Created by the first scenario builder.
 *
 * Expected exports:
 * - mockFeatures - array of feature data
 * - mockStats - statistics mock data
 * - mockSteps - how it works steps data
 */

export const mockFeatures = [
  {
    id: 1,
    icon: 'link',
    title: 'URL Shortening',
    description: 'Create short, memorable links in seconds',
  },
  {
    id: 2,
    icon: 'chart',
    title: 'Click Analytics',
    description: 'Track link performance with detailed analytics',
  },
  {
    id: 3,
    icon: 'share',
    title: 'Shareable Stats',
    description: 'Share your analytics with others',
  },
];

export const mockStats = {
  totalUrls: 1000000,
  totalClicks: 50000000,
  activeUsers: 10000,
};

export const mockSteps = [
  {
    id: 1,
    number: 1,
    title: 'Create Short Link',
    description: 'Paste your long URL and get a short, memorable link instantly',
  },
  {
    id: 2,
    number: 2,
    title: 'Share Anywhere',
    description: 'Share your short link via social media, email, or messages',
  },
  {
    id: 3,
    number: 3,
    title: 'Track Analytics',
    description: 'Monitor clicks, referrers, and locations with detailed statistics',
  },
];

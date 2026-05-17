import { Feature, NavLink } from '../types/homepage';

export const BRAND = {
  name: 'MirDB',
  tagline: 'Shorten URLs, Track Clicks, Understand Your Audience',
} as const;

export const FEATURES: Feature[] = [
  {
    id: 'url-shortening',
    title: 'URL Shortening',
    description: 'Create short, memorable links from any URL in seconds.',
    icon: 'link',
  },
  {
    id: 'analytics',
    title: 'Click Analytics',
    description: 'Track clicks, referrers, browsers, and geographic locations.',
    icon: 'chart',
  },
  {
    id: 'dashboard',
    title: 'Dashboard',
    description: 'Manage all your shortened URLs in one intuitive dashboard.',
    icon: 'dashboard',
  },
  {
    id: 'share-stats',
    title: 'Share Stats',
    description: 'Generate public share tokens for viewing URL analytics.',
    icon: 'share',
  },
];

export const NAV_LINKS: NavLink[] = [
  { label: 'Login', to: '/login', ariaLabel: 'Go to login page' },
  { label: 'Register', to: '/register', ariaLabel: 'Go to registration page' },
];

export const ROUTES = {
  HOME: '/',
  LOGIN: '/login',
  REGISTER: '/register',
  DASHBOARD: '/dashboard',
  STATS: '/stats/:shortCode',
  SETTINGS: '/settings',
} as const;

export const THEMES = ['light', 'dark', 'cyberpunk', 'synthwave'] as const;

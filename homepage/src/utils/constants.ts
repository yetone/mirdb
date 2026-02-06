/**
 * Application constants for the Product Landing Page.
 *
 * Owner: First builder (shared resource)
 */

import type { NavigationItem, Feature, Testimonial, SocialLink } from '../types';

export const BREAKPOINTS = {
  mobile: 768,
  tablet: 1024,
} as const;

export const NAVIGATION_ITEMS: NavigationItem[] = [
  { label: 'Features', href: '#features' },
  { label: 'About', href: '#about' },
  { label: 'Contact', href: '#contact' },
];

export const FEATURE_LIST: Feature[] = [
  {
    icon: 'speed',
    title: 'Lightning Fast',
    description: 'Experience blazing fast performance with our optimized infrastructure.',
  },
  {
    icon: 'security',
    title: 'Secure by Default',
    description: 'Your data is protected with enterprise-grade security measures.',
  },
  {
    icon: 'scale',
    title: 'Infinitely Scalable',
    description: 'Scale effortlessly from prototype to production with zero configuration.',
  },
];

export const TESTIMONIALS: Testimonial[] = [
  {
    quote: 'This product has transformed how we work. Highly recommended!',
    author: 'Jane Doe',
    role: 'CTO',
    company: 'TechCorp',
  },
  {
    quote: 'The best development platform we have ever used. Our productivity increased by 40%.',
    author: 'John Smith',
    role: 'Engineering Lead',
    company: 'DevStudio',
  },
  {
    quote: 'Seamless integration and incredible support. A game-changer for our team.',
    author: 'Sarah Chen',
    role: 'Product Manager',
    company: 'InnovateCo',
  },
];

export const TRUSTED_COMPANIES = [
  { name: 'TechCorp', logo: '/images/logos/techcorp.svg' },
  { name: 'DevStudio', logo: '/images/logos/devstudio.svg' },
  { name: 'InnovateCo', logo: '/images/logos/innovateco.svg' },
  { name: 'CloudSync', logo: '/images/logos/cloudsync.svg' },
  { name: 'DataFlow', logo: '/images/logos/dataflow.svg' },
];

export const USER_STATISTICS = {
  userCount: '10,000+',
  userLabel: 'Happy Users',
  projectCount: '50,000+',
  projectLabel: 'Projects Built',
  uptimePercent: '99.9%',
  uptimeLabel: 'Uptime',
};

export const SOCIAL_LINKS: SocialLink[] = [
  { platform: 'Twitter', url: 'https://twitter.com', icon: 'twitter' },
  { platform: 'GitHub', url: 'https://github.com', icon: 'github' },
  { platform: 'LinkedIn', url: 'https://linkedin.com', icon: 'linkedin' },
];

export const COMPANY_INFO = {
  name: 'Product Landing Page',
  logo: '/images/logo.svg',
};

export const HERO_CONTENT = {
  headline: 'Build Something Amazing',
  subheadline: 'The modern platform for building, deploying, and scaling your applications with confidence.',
  primaryCTA: {
    text: 'Get Started',
    href: '/signup',
  },
  secondaryCTA: {
    text: 'Learn More',
    href: '#features',
  },
};

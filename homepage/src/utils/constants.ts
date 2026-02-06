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
];

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

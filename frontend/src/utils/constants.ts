/**
 * Shared constants for homepage content.
 *
 * This file is created by the first scenario builder and
 * contains all text content, feature data, and configuration.
 */

import type { HeroContent, Feature, NavLink, FooterLink, SocialProofItem } from '../types/homepage';

export const HERO_CONTENT: HeroContent = {
  headline: 'Shorten Links, Track Insights',
  subheadline: 'Create memorable short URLs and powerful analytics in seconds. Monitor clicks, referrers, and locations—all in one place.',
  ctaText: 'Get Started Free',
  secondaryCtaText: 'Learn More',
};

export const FEATURES: Feature[] = [
  {
    icon: 'link',
    title: 'URL Shortening',
    description: 'Transform long, cumbersome links into short, shareable URLs with a single click.',
  },
  {
    icon: 'chart',
    title: 'Analytics Dashboard',
    description: 'Gain deep insights with detailed statistics on clicks, devices, locations, and referrers.',
  },
  {
    icon: 'shield',
    title: 'Secure & Reliable',
    description: 'Your data is protected with encrypted storage and reliable uptime.',
  },
  {
    icon: 'share',
    title: 'Share Insights',
    description: 'Generate share tokens to let others view your link statistics.',
  },
];

export const NAV_LINKS: NavLink[] = [
  { label: 'Features', href: '#features' },
  { label: 'About', href: '#about' },
];

export const FOOTER_LINKS: FooterLink[] = [
  { label: 'Terms of Service', href: '/terms' },
  { label: 'Privacy Policy', href: '/privacy' },
  { label: 'Contact', href: '/contact' },
  { label: 'GitHub', href: 'https://github.com', isExternal: true },
];

export const SOCIAL_PROOF: SocialProofItem[] = [
  {
    icon: 'lock',
    title: 'Privacy First',
    description: 'Your data stays secure with end-to-end encryption.',
  },
  {
    icon: 'zap',
    title: 'Lightning Fast',
    description: 'Redirect in milliseconds with our global CDN.',
  },
  {
    icon: 'check',
    title: '99.9% Uptime',
    description: 'Reliable service you can count on.',
  },
];

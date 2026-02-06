/**
 * Application constants for the Product Landing Page.
 *
 * Owner: First builder (shared resource)
 */

import type { NavigationItem, Feature, Testimonial, SocialLink } from '../types'

export const BREAKPOINTS = {
  mobile: 768,
  tablet: 1024,
}

export const NAVIGATION_ITEMS: NavigationItem[] = [
  { label: 'Features', href: '#features' },
  { label: 'About', href: '#about' },
  { label: 'Contact', href: '#contact' },
]

export const FEATURE_LIST: Feature[] = [
  {
    icon: 'rocket',
    title: 'Lightning Fast',
    description: 'Experience blazing fast performance with our optimized architecture.',
  },
  {
    icon: 'shield',
    title: 'Secure by Design',
    description: 'Your data is protected with enterprise-grade security measures.',
  },
  {
    icon: 'chart',
    title: 'Scalable Solution',
    description: 'Grow without limits. Our platform scales with your needs.',
  },
  {
    icon: 'globe',
    title: 'Global Reach',
    description: 'Deploy worldwide with our distributed infrastructure.',
  },
  {
    icon: 'code',
    title: 'Developer Friendly',
    description: 'Clean APIs and comprehensive documentation for easy integration.',
  },
  {
    icon: 'support',
    title: '24/7 Support',
    description: 'Our team is always here to help you succeed.',
  },
]

export const TESTIMONIALS: Testimonial[] = [
  {
    quote: 'This product has transformed our workflow completely.',
    author: 'Jane Doe',
    role: 'CTO',
    company: 'TechCorp',
  },
]

export const SOCIAL_LINKS: SocialLink[] = [
  { platform: 'twitter', url: 'https://twitter.com', icon: 'twitter' },
  { platform: 'github', url: 'https://github.com', icon: 'github' },
  { platform: 'linkedin', url: 'https://linkedin.com', icon: 'linkedin' },
]

export const COMPANY_INFO = {
  name: 'Product Landing Page',
  logo: '/images/logo.svg',
}

/**
 * Shared type definitions for the Product Landing Page.
 *
 * This file is created by the first scenario builder and
 * should contain types used across multiple components.
 */

export interface NavigationItem {
  label: string;
  href: string;
}

export interface Feature {
  icon: string;
  title: string;
  description: string;
}

export interface Testimonial {
  quote: string;
  author: string;
  role?: string;
  company?: string;
}

export interface SocialLink {
  platform: string;
  url: string;
  icon: string;
}

export interface ButtonProps {
  variant?: 'primary' | 'secondary';
  href?: string;
  onClick?: () => void;
  children: React.ReactNode;
  className?: string;
  'aria-label'?: string;
}

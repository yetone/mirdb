import { ReactNode } from 'react';

export type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave';

export interface Feature {
  id: string;
  title: string;
  description: string;
  icon: ReactNode | string;
}

export interface NavLink {
  label: string;
  to: string;
  ariaLabel?: string;
}

export interface SEOMeta {
  title: string;
  description: string;
  canonicalUrl?: string;
  ogImage?: string;
}

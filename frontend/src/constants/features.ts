import { createElement } from 'react'
import { Link2, BarChart3, Shield, Globe } from 'lucide-react'
import { Feature } from '../types/homepage'

/**
 * Homepage feature list configuration.
 * Owner: Scenario 1 - Homepage Hero Section
 *
 * List of 4-5 product features displayed on the homepage.
 * Each feature has: id, icon, title, description
 */
export const FEATURES: Feature[] = [
  {
    id: 'instant-shortening',
    icon: createElement(Link2, {
      className: 'w-8 h-8 text-primary',
      'aria-hidden': 'true'
    }),
    title: 'Instant Shortening',
    description: 'Shorten any URL in seconds. No account required for basic usage.',
  },
  {
    id: 'click-tracking',
    icon: createElement(BarChart3, {
      className: 'w-8 h-8 text-primary',
      'aria-hidden': 'true'
    }),
    title: 'Click Tracking & Analytics',
    description: 'Track every click with detailed analytics including location, device, and referrer data.',
  },
  {
    id: 'secure-dashboard',
    icon: createElement(Shield, {
      className: 'w-8 h-8 text-primary',
      'aria-hidden': 'true'
    }),
    title: 'Secure Account Dashboard',
    description: 'Manage all your shortened URLs from a secure, personal dashboard.',
  },
  {
    id: 'global-speed',
    icon: createElement(Globe, {
      className: 'w-8 h-8 text-primary',
      'aria-hidden': 'true'
    }),
    title: 'Global Redirect Speed',
    description: 'Lightning-fast redirects worldwide with optimized infrastructure.',
  },
]

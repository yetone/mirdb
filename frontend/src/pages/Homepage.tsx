import { Navigation } from '../components/Navigation'
import { Hero } from '../components/Hero'
import { FeaturedContent } from '../components/FeaturedContent'
import { Footer } from '../components/Footer'
import type { FeaturedItem } from '../types/FeaturedContent.types'
import './Homepage.css'

export interface HomepageProps {
  headline?: string
  subheadline?: string
  ctaText?: string
  ctaHref?: string
  featuredItems?: FeaturedItem[]
  onFeaturedItemClick?: (item: FeaturedItem) => void
}

const DEFAULT_FEATURED_ITEMS: FeaturedItem[] = [
  {
    id: '1',
    title: 'Lightning Fast Performance',
    description: 'Experience sub-millisecond response times with our optimized key-value store.',
    imageUrl: '/images/feature-performance.png',
    link: '/features#performance',
  },
  {
    id: '2',
    title: 'Easy Integration',
    description: 'Memcached protocol support means drop-in replacement for existing systems.',
    imageUrl: '/images/feature-integration.png',
    link: '/features#integration',
  },
  {
    id: '3',
    title: 'Production Ready',
    description: 'Battle-tested reliability with built-in persistence and replication.',
    imageUrl: '/images/feature-production.png',
    link: '/features#production',
  },
]

/**
 * Homepage component - Main landing page that assembles all homepage sections
 *
 * Integrates:
 * - Navigation: Header with nav links, responsive mobile menu, auth-aware links
 * - Hero: Main banner with headline, subheadline, and CTA
 * - FeaturedContent: Grid of featured content cards
 * - Footer: Site map, legal links, social links, contact info
 *
 * REQ-1: Homepage shall display a hero section with primary messaging and CTA
 * REQ-2: Homepage shall provide navigation links to all major application sections
 * REQ-3: Homepage shall display featured content or highlights relevant to users
 * REQ-4: Homepage shall include a footer with secondary navigation and legal links
 * REQ-7: Homepage shall display appropriate messaging for authenticated vs. unauthenticated users
 */
export function Homepage({
  headline = 'Welcome to MirDB',
  subheadline = 'A high-performance persistent key-value store with Memcached protocol. Lightning fast, painless setup, and production ready.',
  ctaText = 'Get Started',
  ctaHref = '#getting-started',
  featuredItems = DEFAULT_FEATURED_ITEMS,
  onFeaturedItemClick,
}: HomepageProps = {}) {
  return (
    <div className="homepage" data-testid="homepage">
      <Navigation />
      <main className="homepage-main" data-testid="homepage-main">
        <Hero
          headline={headline}
          subheadline={subheadline}
          ctaText={ctaText}
          ctaHref={ctaHref}
        />
        <FeaturedContent
          items={featuredItems}
          sectionTitle="Featured"
          onItemClick={onFeaturedItemClick}
        />
      </main>
      <Footer />
    </div>
  )
}

export default Homepage

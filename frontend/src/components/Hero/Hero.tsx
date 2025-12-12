import { useAuth, type User } from '../../context'
import './Hero.css'

export interface HeroProps {
  headline?: string
  subheadline?: string
  ctaText?: string
  ctaHref?: string
  onCtaClick?: () => void
  authenticatedHeadline?: string
  authenticatedSubheadline?: string
  authenticatedCtaText?: string
  authenticatedCtaHref?: string
}

const DEFAULT_HEADLINE = 'Welcome to MirDB'
const DEFAULT_SUBHEADLINE = 'A high-performance persistent key-value store with Memcached protocol'
const DEFAULT_CTA_TEXT = 'Get Started'
const DEFAULT_CTA_HREF = '#getting-started'

const DEFAULT_AUTH_CTA_TEXT = 'Go to Dashboard'
const DEFAULT_AUTH_CTA_HREF = '/dashboard'

function getAuthenticatedHeadline(user: User | null): string {
  if (user?.name) {
    return `Welcome back, ${user.name}!`
  }
  return 'Welcome back!'
}

function getAuthenticatedSubheadline(): string {
  return 'Pick up where you left off. Your dashboard awaits.'
}

export function Hero({
  headline = DEFAULT_HEADLINE,
  subheadline = DEFAULT_SUBHEADLINE,
  ctaText = DEFAULT_CTA_TEXT,
  ctaHref = DEFAULT_CTA_HREF,
  onCtaClick,
  authenticatedHeadline,
  authenticatedSubheadline,
  authenticatedCtaText = DEFAULT_AUTH_CTA_TEXT,
  authenticatedCtaHref = DEFAULT_AUTH_CTA_HREF,
}: HeroProps = {}) {
  // Try to use auth context, but handle case where it's not available
  let isAuthenticated = false
  let user: User | null = null
  try {
    const auth = useAuth()
    isAuthenticated = auth.isAuthenticated
    user = auth.user
  } catch {
    // Auth context not available, treat as unauthenticated
    isAuthenticated = false
  }

  const displayHeadline = isAuthenticated
    ? (authenticatedHeadline || getAuthenticatedHeadline(user))
    : headline
  const displaySubheadline = isAuthenticated
    ? (authenticatedSubheadline || getAuthenticatedSubheadline())
    : subheadline
  const displayCtaText = isAuthenticated ? authenticatedCtaText : ctaText
  const displayCtaHref = isAuthenticated ? authenticatedCtaHref : ctaHref

  const handleClick = () => {
    if (onCtaClick) {
      onCtaClick()
    }
  }

  return (
    <header
      className="hero"
      role="banner"
      data-testid="hero-section"
      data-authenticated={isAuthenticated ? 'true' : 'false'}
    >
      <div className="hero-content">
        <h1 className="hero-headline" data-testid="hero-headline">
          {displayHeadline}
        </h1>
        <p className="hero-subheadline" data-testid="hero-subheadline">
          {displaySubheadline}
        </p>
        <a
          href={displayCtaHref}
          className="hero-cta"
          data-testid="hero-cta"
          onClick={handleClick}
        >
          {displayCtaText}
        </a>
      </div>
    </header>
  )
}

export default Hero

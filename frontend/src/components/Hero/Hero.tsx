import './Hero.css'

export interface HeroProps {
  headline?: string
  subheadline?: string
  ctaText?: string
  ctaHref?: string
  onCtaClick?: () => void
}

const DEFAULT_HEADLINE = 'Welcome to MirDB'
const DEFAULT_SUBHEADLINE = 'A high-performance persistent key-value store with Memcached protocol'
const DEFAULT_CTA_TEXT = 'Get Started'
const DEFAULT_CTA_HREF = '#getting-started'

export function Hero({
  headline = DEFAULT_HEADLINE,
  subheadline = DEFAULT_SUBHEADLINE,
  ctaText = DEFAULT_CTA_TEXT,
  ctaHref = DEFAULT_CTA_HREF,
  onCtaClick,
}: HeroProps = {}) {
  const handleClick = () => {
    if (onCtaClick) {
      onCtaClick()
    }
  }

  return (
    <header className="hero" role="banner" data-testid="hero-section">
      <div className="hero-content">
        <h1 className="hero-headline" data-testid="hero-headline">
          {headline}
        </h1>
        <p className="hero-subheadline" data-testid="hero-subheadline">
          {subheadline}
        </p>
        <a
          href={ctaHref}
          className="hero-cta"
          data-testid="hero-cta"
          onClick={handleClick}
        >
          {ctaText}
        </a>
      </div>
    </header>
  )
}

export default Hero

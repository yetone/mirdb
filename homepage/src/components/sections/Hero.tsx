/**
 * Hero section component with headline, value proposition, and CTAs.
 * Owner: Scenario 2 - Hero Section
 *
 * Requirements:
 * - REQ-3: Compelling headline and subheadline
 * - REQ-4: Primary CTA "Get Started" or "Install Now"
 * - REQ-5: Secondary CTA linking to GitHub
 */

import { Button } from '../ui/Button'
import { heroContent, GITHUB_URL } from '../../config/content'
import './Hero.css'

export function Hero() {
  const handleGetStarted = () => {
    const quickStartSection = document.getElementById('quick-start')
    if (quickStartSection) {
      quickStartSection.scrollIntoView({ behavior: 'smooth' })
    }
  }

  return (
    <section className="hero" id="hero">
      <div className="container">
        <div className="hero__content">
          <h1 className="hero__title">
            {heroContent.title}
            <span className="hero__subtitle">{heroContent.subtitle}</span>
          </h1>
          <p className="hero__description">
            {heroContent.description}
          </p>
          <div className="hero__cta">
            <Button
              variant="primary"
              size="lg"
              onClick={handleGetStarted}
              className="hero__cta-primary"
            >
              Get Started
            </Button>
            <Button
              variant="secondary"
              size="lg"
              href={GITHUB_URL}
              external
              className="hero__cta-secondary"
            >
              View on GitHub
            </Button>
          </div>
        </div>
      </div>
    </section>
  )
}

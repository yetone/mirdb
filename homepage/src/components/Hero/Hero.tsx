/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Features:
 * - Product name "MirDB" display
 * - Tagline: "A Persistent Key-Value Store with Memcached protocol"
 * - Animated logo.gif
 * - Primary CTA button linking to GitHub
 */

import { PRODUCT_NAME, TAGLINE, GITHUB_URL } from '../../utils/constants'
import './Hero.css'

export function Hero() {
  return (
    <section className="hero" aria-label="Hero section">
      <div className="hero-container">
        <div className="hero-logo">
          <img
            src="/assets/logo.gif"
            alt={`${PRODUCT_NAME} animated logo`}
            className="hero-logo-image"
          />
        </div>
        <div className="hero-content">
          <h1 className="hero-title">{PRODUCT_NAME}</h1>
          <p className="hero-tagline">{TAGLINE}</p>
          <div className="hero-actions">
            <a
              href={GITHUB_URL}
              className="hero-cta"
              target="_blank"
              rel="noopener noreferrer"
            >
              Get Started on GitHub
            </a>
          </div>
        </div>
      </div>
    </section>
  )
}

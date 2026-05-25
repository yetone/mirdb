/**
 * Hero Section component.
 * Owner: Scenario 1 - Hero Section and Branding
 *
 * Displays the MirDB logo, tagline, value proposition,
 * primary CTA button, and external navigation links.
 */

import { GITHUB_REPO_URL, DOCS_URL, COMMUNITY_URL } from '../../utils/constants';

export interface HeroSectionProps {
  tagline?: string;
  ctaText?: string;
  onCtaClick?: () => void;
}

const DEFAULT_TAGLINE = 'Fast, persistent key-value store with Memcached protocol';

export default function HeroSection({
  tagline = DEFAULT_TAGLINE,
  ctaText = 'Get Started Now',
  onCtaClick,
}: HeroSectionProps) {
  return (
    <section
      className="hero-section"
      aria-label="Hero"
      data-testid="hero-section"
    >
      <div className="hero-container">
        {/* Logo */}
        <div className="hero-logo" data-testid="hero-logo">
          <img
            src="/mirdb-logo.svg"
            alt="MirDB Logo"
            width={80}
            height={80}
            data-testid="hero-logo-img"
          />
          <h1 className="hero-title" data-testid="hero-title">MirDB</h1>
        </div>

        {/* Tagline */}
        <p className="hero-tagline" data-testid="hero-tagline">
          {tagline}
        </p>

        {/* Value Proposition */}
        <p className="hero-value-prop" data-testid="hero-value-prop">
          A high-performance persistent key-value store built in Rust,
          compatible with the Memcached protocol for easy integration.
        </p>

        {/* CTA Button */}
        <button
          type="button"
          className="hero-cta-button"
          data-testid="hero-cta-button"
          onClick={onCtaClick}
        >
          {ctaText}
        </button>

        {/* External Links */}
        <nav className="hero-links" aria-label="External resources" data-testid="hero-links">
          <a
            href={GITHUB_REPO_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="hero-link"
            data-testid="hero-link-github"
          >
            GitHub
          </a>
          <a
            href={DOCS_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="hero-link"
            data-testid="hero-link-docs"
          >
            Documentation
          </a>
          <a
            href={COMMUNITY_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="hero-link"
            data-testid="hero-link-community"
          >
            Community
          </a>
        </nav>
      </div>
    </section>
  );
}

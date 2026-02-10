/**
 * Hero Section Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Displays the main hero section with:
 * - Product name "MirDB"
 * - Tagline "A Persistent Key-Value Store with Memcached Protocol"
 * - Value proposition text
 * - Primary CTA button (GitHub link)
 * - Secondary CTA button (Documentation link)
 * - Background gradient
 */

import React from 'react';
import { Button } from '@/components/ui/Button';
import {
  GITHUB_URL,
  DOCS_URL,
  SITE_TITLE,
  SITE_TAGLINE,
  SITE_DESCRIPTION,
} from '@/utils/constants';

const heroStyles: React.CSSProperties = {
  position: 'relative',
  minHeight: '80vh',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  overflow: 'hidden',
  padding: 'var(--spacing-2xl) var(--container-padding)',
};

const backgroundStyles: React.CSSProperties = {
  position: 'absolute',
  inset: 0,
  background: 'linear-gradient(135deg, var(--bg-secondary) 0%, var(--bg-primary) 50%, var(--bg-tertiary) 100%)',
  zIndex: 0,
};

const decorativeCircle1Styles: React.CSSProperties = {
  position: 'absolute',
  top: '10%',
  right: '5%',
  width: '300px',
  height: '300px',
  background: 'var(--accent-primary)',
  opacity: 0.1,
  borderRadius: '50%',
  filter: 'blur(60px)',
};

const decorativeCircle2Styles: React.CSSProperties = {
  position: 'absolute',
  bottom: '10%',
  left: '5%',
  width: '400px',
  height: '400px',
  background: 'var(--accent-secondary)',
  opacity: 0.1,
  borderRadius: '50%',
  filter: 'blur(80px)',
};

const contentStyles: React.CSSProperties = {
  position: 'relative',
  zIndex: 1,
  maxWidth: '900px',
  margin: '0 auto',
  textAlign: 'center',
  padding: 'var(--spacing-xl)',
};

const titleStyles: React.CSSProperties = {
  fontSize: 'clamp(3rem, 8vw, 5rem)',
  fontWeight: 800,
  color: 'var(--text-primary)',
  marginBottom: 'var(--spacing-lg)',
  lineHeight: 1.1,
};

const taglineStyles: React.CSSProperties = {
  fontSize: 'clamp(1.25rem, 3vw, 1.75rem)',
  fontWeight: 500,
  color: 'var(--accent-primary)',
  marginBottom: 'var(--spacing-xl)',
};

const descriptionStyles: React.CSSProperties = {
  fontSize: 'var(--font-size-lg)',
  color: 'var(--text-secondary)',
  maxWidth: '700px',
  margin: '0 auto var(--spacing-2xl)',
  lineHeight: 1.7,
};

const ctaContainerStyles: React.CSSProperties = {
  display: 'flex',
  flexWrap: 'wrap',
  gap: 'var(--spacing-md)',
  justifyContent: 'center',
  alignItems: 'center',
};

const GitHubIcon: React.FC = () => (
  <svg
    width="20"
    height="20"
    viewBox="0 0 24 24"
    fill="currentColor"
    aria-hidden="true"
  >
    <path
      fillRule="evenodd"
      d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z"
      clipRule="evenodd"
    />
  </svg>
);

const BookIcon: React.FC = () => (
  <svg
    width="20"
    height="20"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
  >
    <path d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
  </svg>
);

export const Hero: React.FC = () => {
  return (
    <section
      id="hero"
      style={heroStyles}
      aria-label="Hero section"
    >
      {/* Background gradient */}
      <div style={backgroundStyles} aria-hidden="true" />

      {/* Decorative elements */}
      <div style={decorativeCircle1Styles} aria-hidden="true" />
      <div style={decorativeCircle2Styles} aria-hidden="true" />

      {/* Content */}
      <div style={contentStyles}>
        <h1 style={titleStyles}>{SITE_TITLE}</h1>

        <p style={taglineStyles}>{SITE_TAGLINE}</p>

        <p style={descriptionStyles}>{SITE_DESCRIPTION}</p>

        <div style={ctaContainerStyles}>
          <Button
            variant="primary"
            size="lg"
            href={GITHUB_URL}
            isExternal
            aria-label="View MirDB on GitHub"
          >
            <GitHubIcon />
            View on GitHub
          </Button>

          <Button
            variant="secondary"
            size="lg"
            href={DOCS_URL}
            aria-label="Get started with MirDB documentation"
          >
            <BookIcon />
            Get Started
          </Button>
        </div>
      </div>
    </section>
  );
};

export default Hero;

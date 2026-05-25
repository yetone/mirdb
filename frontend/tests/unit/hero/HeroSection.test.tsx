/**
 * Unit tests for HeroSection component.
 * Covers REQ-1 (logo/tagline), REQ-10 (GitHub/docs links).
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import HeroSection from '../../../src/components/hero/HeroSection';
import { GITHUB_REPO_URL, DOCS_URL, COMMUNITY_URL } from '../../../src/utils/constants';

describe('HeroSection', () => {
  it('renders the hero section container', () => {
    render(<HeroSection />);
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
  });

  it('displays the MirDB logo image', () => {
    render(<HeroSection />);
    const logoImg = screen.getByTestId('hero-logo-img');
    expect(logoImg).toBeInTheDocument();
    expect(logoImg).toHaveAttribute('src', '/mirdb-logo.svg');
    expect(logoImg).toHaveAttribute('alt', 'MirDB Logo');
  });

  it('displays the MirDB title', () => {
    render(<HeroSection />);
    expect(screen.getByTestId('hero-title')).toHaveTextContent('MirDB');
  });

  it('displays the default tagline', () => {
    render(<HeroSection />);
    expect(screen.getByTestId('hero-tagline')).toHaveTextContent(
      'Fast, persistent key-value store with Memcached protocol'
    );
  });

  it('displays a custom tagline when provided', () => {
    const customTagline = 'Custom tagline for testing';
    render(<HeroSection tagline={customTagline} />);
    expect(screen.getByTestId('hero-tagline')).toHaveTextContent(customTagline);
  });

  it('displays the value proposition text', () => {
    render(<HeroSection />);
    expect(screen.getByTestId('hero-value-prop')).toBeInTheDocument();
    expect(screen.getByTestId('hero-value-prop')).toHaveTextContent(/high-performance persistent key-value store/);
  });

  it('renders the primary CTA button with default text', () => {
    render(<HeroSection />);
    const ctaButton = screen.getByTestId('hero-cta-button');
    expect(ctaButton).toBeInTheDocument();
    expect(ctaButton).toHaveTextContent('Get Started Now');
  });

  it('renders the CTA button with custom text when provided', () => {
    render(<HeroSection ctaText="Learn More" />);
    expect(screen.getByTestId('hero-cta-button')).toHaveTextContent('Learn More');
  });

  it('calls onCtaClick handler when CTA button is clicked', async () => {
    const user = userEvent.setup();
    const onCtaClick = vi.fn();
    render(<HeroSection onCtaClick={onCtaClick} />);
    await user.click(screen.getByTestId('hero-cta-button'));
    expect(onCtaClick).toHaveBeenCalledTimes(1);
  });

  it('renders external navigation links', () => {
    render(<HeroSection />);
    expect(screen.getByTestId('hero-link-github')).toBeInTheDocument();
    expect(screen.getByTestId('hero-link-docs')).toBeInTheDocument();
    expect(screen.getByTestId('hero-link-community')).toBeInTheDocument();
  });

  it('GitHub link points to the correct repository URL', () => {
    render(<HeroSection />);
    const githubLink = screen.getByTestId('hero-link-github');
    expect(githubLink).toHaveAttribute('href', GITHUB_REPO_URL);
  });

  it('Documentation link points to the correct URL', () => {
    render(<HeroSection />);
    const docsLink = screen.getByTestId('hero-link-docs');
    expect(docsLink).toHaveAttribute('href', DOCS_URL);
  });

  it('Community link points to the correct URL', () => {
    render(<HeroSection />);
    const communityLink = screen.getByTestId('hero-link-community');
    expect(communityLink).toHaveAttribute('href', COMMUNITY_URL);
  });

  it('all external links open in a new tab', () => {
    render(<HeroSection />);
    const links = [
      screen.getByTestId('hero-link-github'),
      screen.getByTestId('hero-link-docs'),
      screen.getByTestId('hero-link-community'),
    ];
    for (const link of links) {
      expect(link).toHaveAttribute('target', '_blank');
    }
  });

  it('all external links have rel="noopener noreferrer"', () => {
    render(<HeroSection />);
    const links = [
      screen.getByTestId('hero-link-github'),
      screen.getByTestId('hero-link-docs'),
      screen.getByTestId('hero-link-community'),
    ];
    for (const link of links) {
      expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    }
  });

  it('has proper ARIA attributes for accessibility', () => {
    render(<HeroSection />);
    expect(screen.getByTestId('hero-section')).toHaveAttribute('aria-label', 'Hero');
    expect(screen.getByTestId('hero-links')).toHaveAttribute('aria-label', 'External resources');
  });
});

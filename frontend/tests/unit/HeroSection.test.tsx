/**
 * HeroSection Unit Tests
 * Owner: Scenario 1 - Homepage Hero Section Rendering
 *
 * Tests for headline, subheading, and CTA rendering.
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import HeroSection from '../../src/components/Home/HeroSection';

function renderWithRouter(ui: React.ReactElement) {
  return render(<MemoryRouter>{ui}</MemoryRouter>);
}

describe('HeroSection', () => {
  it('renders the hero section container', () => {
    renderWithRouter(<HeroSection />);
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
  });

  it('renders headline with non-empty text content describing the URL shortening service', () => {
    renderWithRouter(<HeroSection />);
    const headline = screen.getByTestId('hero-headline');
    expect(headline).toBeInTheDocument();
    expect(headline.textContent).toBeTruthy();
    expect(headline.textContent!.length).toBeGreaterThan(0);
  });

  it('renders subheading with descriptive benefit text', () => {
    renderWithRouter(<HeroSection />);
    const subheading = screen.getByTestId('hero-subheading');
    expect(subheading).toBeInTheDocument();
    expect(subheading.textContent).toBeTruthy();
    expect(subheading.textContent!.length).toBeGreaterThan(20);
  });

  it('renders CTA button linking to /register', () => {
    renderWithRouter(<HeroSection />);
    const ctaButton = screen.getByTestId('hero-cta-button');
    expect(ctaButton).toBeInTheDocument();
    expect(ctaButton).toHaveAttribute('href', '/register');
  });

  it('renders the URL shortener form when provided as a prop', () => {
    renderWithRouter(
      <HeroSection urlShortenerForm={<div data-testid="mock-form">Mock Form</div>} />
    );
    expect(screen.getByTestId('hero-form-container')).toBeInTheDocument();
    expect(screen.getByTestId('mock-form')).toBeInTheDocument();
  });

  it('has accessible landmark and aria label', () => {
    renderWithRouter(<HeroSection />);
    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toHaveAttribute('aria-label', 'Hero section');
  });
});

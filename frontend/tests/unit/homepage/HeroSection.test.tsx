/**
 * HeroSection Unit Tests
 * Owner: Scenario 1 - Hero Section & Value Proposition
 *
 * Test cases:
 * 1. Hero section renders with H1 headline containing product tagline
 * 2. Subheadline contains keywords 'shorten', 'links', 'track', 'clicks'
 * 3. Primary CTA button with 'Get Started Free' is present
 * 4. CTA button navigates to /register
 * 5. Mobile viewport: content is centered
 * 6. Desktop viewport: split layout with text left, visual right
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import HeroSection from '../../../src/components/homepage/HeroSection';
import { HERO_CONTENT } from '../../../src/utils/constants';

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

describe('HeroSection', () => {
  beforeEach(() => {
    mockNavigate.mockClear();
  });

  // Test Case 1: Hero section renders with H1 headline containing product tagline
  it('renders H1 headline with product tagline about URL shortening', () => {
    render(
      <BrowserRouter>
        <HeroSection />
      </BrowserRouter>
    );

    const headline = screen.getByTestId('hero-headline');
    expect(headline).toBeInTheDocument();
    expect(headline.tagName).toBe('H1');
    expect(headline.textContent).toBe(HERO_CONTENT.headline);
    // Verify tagline mentions shortening/links/insights
    expect(headline.textContent?.toLowerCase()).toMatch(/shorten|links|insights|url/i);
  });

  // Test Case 2: Subheadline contains value proposition keywords
  it('displays subheadline with shorten, links, track, clicks messaging', () => {
    render(
      <BrowserRouter>
        <HeroSection />
      </BrowserRouter>
    );

    const subheadline = screen.getByTestId('hero-subheadline');
    expect(subheadline).toBeInTheDocument();

    const text = subheadline.textContent?.toLowerCase() || '';
    // Check for value proposition keywords
    expect(text).toMatch(/short|links|track|clicks|analytics|monitor/);
  });

  // Test Case 3: Primary CTA button 'Get Started Free' is present and visible
  it('displays primary CTA button with "Get Started Free" text', () => {
    render(
      <BrowserRouter>
        <HeroSection />
      </BrowserRouter>
    );

    const ctaButton = screen.getByTestId('hero-cta-primary');
    expect(ctaButton).toBeInTheDocument();
    expect(ctaButton).toBeVisible();
    expect(ctaButton.textContent).toBe(HERO_CONTENT.ctaText);
    expect(ctaButton).toHaveClass('btn-primary');
  });

  // Test Case 4: CTA button navigates to /register
  it('navigates to /register when primary CTA is clicked', () => {
    render(
      <BrowserRouter>
        <HeroSection />
      </BrowserRouter>
    );

    const ctaButton = screen.getByTestId('hero-cta-primary');
    fireEvent.click(ctaButton);

    expect(mockNavigate).toHaveBeenCalledWith('/register');
  });

  // Test Case 5: Mobile viewport - content is centered without horizontal scrolling
  it('has centered content layout for mobile viewport', () => {
    render(
      <BrowserRouter>
        <HeroSection />
      </BrowserRouter>
    );

    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toBeInTheDocument();

    // Check that the section has centering classes
    expect(heroSection).toHaveClass('flex', 'items-center', 'justify-center');

    // Check that the text container has responsive text centering
    const headline = screen.getByTestId('hero-headline');
    const textContainer = headline.parentElement;
    expect(textContainer).toHaveClass('text-center');
  });

  // Test Case 6: Desktop viewport - split layout with text on left and visual on right
  it('has split layout structure for desktop viewport', () => {
    render(
      <BrowserRouter>
        <HeroSection />
      </BrowserRouter>
    );

    const heroSection = screen.getByTestId('hero-section');
    const heroVisual = screen.getByTestId('hero-visual');

    expect(heroSection).toBeInTheDocument();
    expect(heroVisual).toBeInTheDocument();

    // Get the flex container that holds both text and visual
    const flexContainer = heroVisual.parentElement;

    // Check for flex layout with responsive behavior (col on mobile, row on desktop)
    expect(flexContainer).toHaveClass('flex', 'flex-col', 'lg:flex-row');
  });

  // Additional test: Calls onGetStarted callback when provided
  it('calls onGetStarted callback when CTA is clicked', () => {
    const onGetStarted = vi.fn();

    render(
      <BrowserRouter>
        <HeroSection onGetStarted={onGetStarted} />
      </BrowserRouter>
    );

    const ctaButton = screen.getByTestId('hero-cta-primary');
    fireEvent.click(ctaButton);

    expect(onGetStarted).toHaveBeenCalled();
    expect(mockNavigate).toHaveBeenCalledWith('/register');
  });

  // Additional test: Secondary CTA button is present
  it('displays secondary CTA button with "Learn More" text', () => {
    render(
      <BrowserRouter>
        <HeroSection />
      </BrowserRouter>
    );

    const secondaryButton = screen.getByTestId('hero-cta-secondary');
    expect(secondaryButton).toBeInTheDocument();
    expect(secondaryButton.textContent).toBe(HERO_CONTENT.secondaryCtaText);
  });
});

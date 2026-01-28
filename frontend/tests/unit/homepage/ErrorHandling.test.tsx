/**
 * Error Handling and Edge Cases Unit Tests
 * Scenario 10 - Error Handling and Edge Cases
 *
 * Tests for verifying homepage components handle error conditions gracefully:
 * - Rendering without ThemeContext provider
 * - HeroSection with missing props
 * - FeaturesSection with empty features array
 * - Footer with missing links array
 *
 * Requirements: Defensive coding, graceful degradation
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import HeroSection from '../../../src/components/homepage/HeroSection';
import { FeaturesSection } from '../../../src/components/homepage/FeaturesSection';
import { Footer } from '../../../src/components/homepage/Footer';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    h1: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => <h1 {...props}>{children}</h1>,
    p: ({ children, ...props }: React.HTMLAttributes<HTMLParagraphElement>) => <p {...props}>{children}</p>,
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => <div {...props}>{children}</div>,
    button: ({ children, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement>) => <button {...props}>{children}</button>,
    footer: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <footer {...props}>{children}</footer>
    ),
    section: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <section {...props}>{children}</section>
    ),
  },
}));

const renderWithRouter = (component: React.ReactNode) => {
  return render(<BrowserRouter>{component}</BrowserRouter>);
};

describe('Error Handling - HeroSection with missing props', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // Test Case 4: HeroSection handles missing props gracefully with defaults
  it('renders with default content when no props are provided', () => {
    renderWithRouter(<HeroSection />);

    // Should render with default headline
    const headline = screen.getByTestId('hero-headline');
    expect(headline).toBeInTheDocument();
    expect(headline).toHaveTextContent(/shorten urls/i);
  });

  it('renders with default subheadline when content prop is undefined', () => {
    renderWithRouter(<HeroSection content={undefined} />);

    const subheadline = screen.getByTestId('hero-subheadline');
    expect(subheadline).toBeInTheDocument();
    expect(subheadline.textContent?.length).toBeGreaterThan(0);
  });

  it('renders with default CTAs when content prop is not provided', () => {
    renderWithRouter(<HeroSection />);

    // Primary CTA should use default
    const primaryCTA = screen.getByTestId('hero-primary-cta');
    expect(primaryCTA).toBeInTheDocument();
    expect(primaryCTA).toHaveAttribute('href', '/register');

    // Secondary CTA should use default
    const secondaryCTA = screen.getByTestId('hero-secondary-cta');
    expect(secondaryCTA).toBeInTheDocument();
    expect(secondaryCTA).toHaveAttribute('href', '/login');
  });

  it('renders hero section container even with empty content object', () => {
    // Pass partial content to verify component handles incomplete data
    const partialContent = {
      headline: 'Test Headline',
      subheadline: 'Test subheadline',
      primaryCTA: {
        label: 'Test CTA',
        href: '/test',
      },
      // secondaryCTA is intentionally omitted
    };

    renderWithRouter(<HeroSection content={partialContent} />);

    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toBeInTheDocument();

    // Primary CTA should work
    expect(screen.getByTestId('hero-primary-cta')).toHaveTextContent('Test CTA');

    // Secondary CTA should not be rendered when not provided
    expect(screen.queryByTestId('hero-secondary-cta')).not.toBeInTheDocument();
  });

  it('handles content with all fields populated correctly', () => {
    const fullContent = {
      headline: 'Full Headline',
      subheadline: 'Full subheadline text',
      primaryCTA: { label: 'Primary', href: '/primary' },
      secondaryCTA: { label: 'Secondary', href: '/secondary' },
    };

    renderWithRouter(<HeroSection content={fullContent} />);

    expect(screen.getByTestId('hero-headline')).toHaveTextContent('Full Headline');
    expect(screen.getByTestId('hero-subheadline')).toHaveTextContent('Full subheadline text');
    expect(screen.getByTestId('hero-primary-cta')).toHaveTextContent('Primary');
    expect(screen.getByTestId('hero-secondary-cta')).toHaveTextContent('Secondary');
  });
});

describe('Error Handling - FeaturesSection with empty features array', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // Test Case 5: FeaturesSection handles empty state appropriately
  it('renders section container even with empty features array', () => {
    render(<FeaturesSection features={[]} />);

    const section = screen.getByTestId('features-section');
    expect(section).toBeInTheDocument();
  });

  it('renders grid container with empty features array', () => {
    render(<FeaturesSection features={[]} />);

    const grid = screen.getByTestId('feature-cards-grid');
    expect(grid).toBeInTheDocument();
  });

  it('renders no feature cards when features array is empty', () => {
    render(<FeaturesSection features={[]} />);

    // Should have no feature cards
    const featureCards = screen.queryAllByTestId('feature-card');
    expect(featureCards).toHaveLength(0);
  });

  it('still renders section heading with empty features', () => {
    render(<FeaturesSection features={[]} />);

    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toBeInTheDocument();
    expect(heading).toHaveTextContent('Powerful Features');
  });

  it('renders with default features when prop is not provided', () => {
    render(<FeaturesSection />);

    // Should render default 4 features
    const featureCards = screen.getAllByTestId('feature-card');
    expect(featureCards).toHaveLength(4);
  });

  it('renders with default features when features is undefined', () => {
    render(<FeaturesSection features={undefined} />);

    // Should render default features
    const featureCards = screen.getAllByTestId('feature-card');
    expect(featureCards.length).toBeGreaterThan(0);
  });

  it('handles single feature in array', () => {
    const singleFeature = [
      {
        id: 'single-feature',
        title: 'Single Feature',
        description: 'A single feature description',
        icon: <span data-testid="custom-icon">Icon</span>,
      },
    ];

    render(<FeaturesSection features={singleFeature} />);

    const featureCards = screen.getAllByTestId('feature-card');
    expect(featureCards).toHaveLength(1);
    expect(screen.getByText('Single Feature')).toBeInTheDocument();
  });
});

describe('Error Handling - Footer with missing links array', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // Test Case 6: Footer handles missing props with defaults
  it('renders with default links when no props provided', () => {
    renderWithRouter(<Footer />);

    const footer = screen.getByTestId('footer-section');
    expect(footer).toBeInTheDocument();

    // Should have default Terms and Privacy links
    expect(screen.getByRole('link', { name: /terms/i })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /privacy/i })).toBeInTheDocument();
  });

  it('renders with default copyright when copyright prop is not provided', () => {
    renderWithRouter(<Footer />);

    const copyright = screen.getByTestId('footer-copyright');
    expect(copyright).toBeInTheDocument();
    // Should contain copyright symbol and year
    expect(copyright.textContent).toMatch(/©/);
    expect(copyright.textContent).toMatch(/\d{4}/);
  });

  it('renders footer container when links is undefined', () => {
    renderWithRouter(<Footer links={undefined} />);

    const footer = screen.getByTestId('footer-section');
    expect(footer).toBeInTheDocument();

    // Should use default links
    const links = screen.getAllByRole('link');
    expect(links.length).toBeGreaterThan(0);
  });

  it('renders footer with empty links array', () => {
    renderWithRouter(<Footer links={[]} />);

    const footer = screen.getByTestId('footer-section');
    expect(footer).toBeInTheDocument();

    // Should have no links in the footer navigation
    const footerNav = footer.querySelector('nav');
    expect(footerNav).toBeInTheDocument();
    const links = footerNav?.querySelectorAll('a');
    expect(links?.length).toBe(0);
  });

  it('renders footer with custom copyright', () => {
    const customCopyright = 'Custom Copyright 2026';
    renderWithRouter(<Footer copyright={customCopyright} />);

    const copyright = screen.getByTestId('footer-copyright');
    expect(copyright).toHaveTextContent('Custom Copyright 2026');
  });

  it('renders footer with both custom links and copyright', () => {
    const customLinks = [
      { label: 'Custom Link 1', href: '/custom1' },
      { label: 'Custom Link 2', href: '/custom2' },
    ];
    const customCopyright = 'Custom Corp 2026';

    renderWithRouter(<Footer links={customLinks} copyright={customCopyright} />);

    expect(screen.getByRole('link', { name: 'Custom Link 1' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Custom Link 2' })).toBeInTheDocument();
    expect(screen.getByTestId('footer-copyright')).toHaveTextContent('Custom Corp 2026');
  });

  it('handles single link in array', () => {
    const singleLink = [{ label: 'Only Link', href: '/only' }];

    renderWithRouter(<Footer links={singleLink} />);

    const link = screen.getByRole('link', { name: 'Only Link' });
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute('href', '/only');
  });
});

describe('Error Handling - Component rendering without crashing', () => {
  it('HeroSection does not throw when rendered', () => {
    expect(() => renderWithRouter(<HeroSection />)).not.toThrow();
  });

  it('FeaturesSection does not throw when rendered with empty array', () => {
    expect(() => render(<FeaturesSection features={[]} />)).not.toThrow();
  });

  it('Footer does not throw when rendered with empty links', () => {
    expect(() => renderWithRouter(<Footer links={[]} />)).not.toThrow();
  });

  it('All components render with data-testid attributes for testing', () => {
    renderWithRouter(<HeroSection />);
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();

    const { unmount } = render(<FeaturesSection />);
    expect(screen.getByTestId('features-section')).toBeInTheDocument();
    unmount();

    renderWithRouter(<Footer />);
    expect(screen.getByTestId('footer-section')).toBeInTheDocument();
  });
});

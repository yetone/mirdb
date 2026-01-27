/**
 * Unit Tests for HeroSection Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests:
 * 1. Hero section renders with headline containing value proposition
 * 2. Subheadline text is visible and provides context
 * 3. Primary CTA button exists and is clickable
 * 4. Secondary CTA (Learn More) link exists
 * 5. Primary CTA navigates to /register
 */
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from './setup';
import HeroSection from '../../../src/components/landing/HeroSection';

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
    vi.clearAllMocks();
  });

  it('renders hero section with headline containing value proposition text', () => {
    render(<HeroSection />);

    // Check that the main headline is present with value proposition
    const headline = screen.getByRole('heading', { level: 1 });
    expect(headline).toBeInTheDocument();
    expect(headline).toHaveTextContent(/shorten links/i);
    expect(headline).toHaveTextContent(/track everything/i);
  });

  it('renders subheadline text that provides context about the service', () => {
    render(<HeroSection />);

    // Check for subheadline with service context
    const subheadline = screen.getByText(/create short, powerful links/i);
    expect(subheadline).toBeInTheDocument();
    expect(subheadline).toHaveTextContent(/analytics/i);
  });

  it('renders primary CTA button with Get Started text that is clickable', () => {
    render(<HeroSection />);

    // Find the primary CTA button
    const primaryCTA = screen.getByRole('button', { name: /get started/i });
    expect(primaryCTA).toBeInTheDocument();

    // Verify it's clickable
    fireEvent.click(primaryCTA);
    expect(mockNavigate).toHaveBeenCalledWith('/register');
  });

  it('renders secondary CTA link with Learn More text', () => {
    render(<HeroSection />);

    // Find the Learn More link
    const learnMoreLink = screen.getByRole('button', { name: /learn more/i });
    expect(learnMoreLink).toBeInTheDocument();
  });

  it('calls onGetStarted callback when primary CTA is clicked if provided', () => {
    const onGetStarted = vi.fn();
    render(<HeroSection onGetStarted={onGetStarted} />);

    const primaryCTA = screen.getByRole('button', { name: /get started/i });
    fireEvent.click(primaryCTA);

    expect(onGetStarted).toHaveBeenCalled();
    expect(mockNavigate).not.toHaveBeenCalled();
  });

  it('calls onLearnMore callback when Learn More is clicked if provided', () => {
    const onLearnMore = vi.fn();
    render(<HeroSection onLearnMore={onLearnMore} />);

    const learnMoreLink = screen.getByRole('button', { name: /learn more/i });
    fireEvent.click(learnMoreLink);

    expect(onLearnMore).toHaveBeenCalled();
  });

  it('scrolls to features section when Learn More is clicked without callback', () => {
    // Create a mock features section
    const featuresSection = document.createElement('div');
    featuresSection.id = 'features';
    document.body.appendChild(featuresSection);

    render(<HeroSection />);

    const learnMoreLink = screen.getByRole('button', { name: /learn more/i });
    fireEvent.click(learnMoreLink);

    expect(featuresSection.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });

    // Cleanup
    document.body.removeChild(featuresSection);
  });

  it('has proper accessibility attributes', () => {
    render(<HeroSection />);

    // Check that section has aria-labelledby
    const section = screen.getByRole('region', { hidden: true }) ||
                   document.querySelector('section[aria-labelledby]');
    expect(section).toHaveAttribute('aria-labelledby', 'hero-heading');

    // Check heading has proper id
    const heading = screen.getByRole('heading', { level: 1 });
    expect(heading).toHaveAttribute('id', 'hero-heading');
  });

  it('displays visual dashboard preview element', () => {
    render(<HeroSection />);

    // Check for dashboard preview elements
    expect(screen.getByText(/total links/i)).toBeInTheDocument();
    expect(screen.getByText(/total clicks/i)).toBeInTheDocument();
    // Use getByRole to find the Shorten button specifically
    expect(screen.getByRole('button', { name: 'Shorten' })).toBeInTheDocument();
  });
});

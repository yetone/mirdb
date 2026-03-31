/**
 * Unit tests for HeroSection Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests cover:
 * - Logo image rendering
 * - Product name display
 * - Tagline display
 * - Value proposition description
 * - CTA buttons functionality
 * - Hover effects
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { HeroSection } from '../../../src/components/sections/HeroSection';

describe('HeroSection', () => {
  beforeEach(() => {
    // Reset window.open mock before each test
    vi.stubGlobal('open', vi.fn());
  });

  // Test Case 2: Component renders with MirDB logo image element
  it('renders the MirDB logo image element', () => {
    render(<HeroSection />);

    const logo = screen.getByTestId('hero-logo');
    expect(logo).toBeInTheDocument();
    expect(logo).toHaveAttribute('src', '/assets/logo.gif');
    expect(logo).toHaveAttribute('alt', 'MirDB Logo');
  });

  // Test Case 3: Component displays product name 'MirDB' in heading
  it('displays product name MirDB in heading', () => {
    render(<HeroSection />);

    const title = screen.getByTestId('hero-title');
    expect(title).toBeInTheDocument();
    expect(title).toHaveTextContent('MirDB');

    // Also verify it's a heading element
    expect(screen.getByRole('heading', { name: 'MirDB' })).toBeInTheDocument();
  });

  // Test Case 4: Component displays tagline about persistent key-value store
  it('displays tagline about persistent key-value store', () => {
    render(<HeroSection />);

    const tagline = screen.getByTestId('hero-tagline');
    expect(tagline).toBeInTheDocument();
    expect(tagline).toHaveTextContent('A Persistent Key-Value Store with Memcached Protocol');
  });

  // Test Case 5: Component displays value proposition mentioning persistence and Memcached protocol
  it('displays value proposition mentioning persistence and Memcached protocol', () => {
    render(<HeroSection />);

    const description = screen.getByTestId('hero-description');
    expect(description).toBeInTheDocument();
    expect(description.textContent).toMatch(/persistent/i);
    expect(description.textContent).toMatch(/memcached/i);
    expect(description.textContent).toMatch(/key-value/i);
  });

  // Test: Get Started button is rendered
  it('renders Get Started button', () => {
    render(<HeroSection />);

    const getStartedButton = screen.getByTestId('get-started-button');
    expect(getStartedButton).toBeInTheDocument();
    expect(getStartedButton).toHaveTextContent('Get Started');
  });

  // Test: View on GitHub button is rendered
  it('renders View on GitHub button', () => {
    render(<HeroSection />);

    const gitHubButton = screen.getByTestId('view-github-button');
    expect(gitHubButton).toBeInTheDocument();
    expect(gitHubButton).toHaveTextContent('View on GitHub');
  });

  // Test: Get Started button calls onGetStarted callback when clicked
  it('calls onGetStarted callback when Get Started button is clicked', () => {
    const onGetStarted = vi.fn();
    render(<HeroSection onGetStarted={onGetStarted} />);

    const getStartedButton = screen.getByTestId('get-started-button');
    fireEvent.click(getStartedButton);

    expect(onGetStarted).toHaveBeenCalledTimes(1);
  });

  // Test: View on GitHub button calls onViewGitHub callback when clicked
  it('calls onViewGitHub callback when View on GitHub button is clicked', () => {
    const onViewGitHub = vi.fn();
    render(<HeroSection onViewGitHub={onViewGitHub} />);

    const gitHubButton = screen.getByTestId('view-github-button');
    fireEvent.click(gitHubButton);

    expect(onViewGitHub).toHaveBeenCalledTimes(1);
  });

  // Test: View on GitHub opens GitHub URL in new tab when no callback provided
  it('opens GitHub URL in new tab when View on GitHub button is clicked without callback', () => {
    render(<HeroSection />);

    const gitHubButton = screen.getByTestId('view-github-button');
    fireEvent.click(gitHubButton);

    expect(window.open).toHaveBeenCalledWith(
      'https://github.com/yetone/mirdb',
      '_blank',
      'noopener,noreferrer'
    );
  });

  // Test: Hero section has proper accessibility attributes
  it('has proper accessibility attributes', () => {
    render(<HeroSection />);

    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-heading');
    expect(heroSection).toHaveAttribute('id', 'hero');
  });

  // Test: CTA buttons container is rendered
  it('renders CTA buttons container', () => {
    render(<HeroSection />);

    const ctaContainer = screen.getByTestId('hero-cta-buttons');
    expect(ctaContainer).toBeInTheDocument();
  });

  // Test: Buttons have hover effect classes
  it('buttons have group class for hover effects', () => {
    render(<HeroSection />);

    const getStartedButton = screen.getByTestId('get-started-button');
    const gitHubButton = screen.getByTestId('view-github-button');

    // Buttons should have the 'group' class for hover effects
    expect(getStartedButton).toHaveClass('group');
    expect(gitHubButton).toHaveClass('group');
  });

  // Test: Hero section renders correctly with all elements
  it('renders complete hero section with all elements', () => {
    render(<HeroSection />);

    // Verify all main elements are present
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
    expect(screen.getByTestId('hero-logo')).toBeInTheDocument();
    expect(screen.getByTestId('hero-title')).toBeInTheDocument();
    expect(screen.getByTestId('hero-tagline')).toBeInTheDocument();
    expect(screen.getByTestId('hero-description')).toBeInTheDocument();
    expect(screen.getByTestId('hero-cta-buttons')).toBeInTheDocument();
    expect(screen.getByTestId('get-started-button')).toBeInTheDocument();
    expect(screen.getByTestId('view-github-button')).toBeInTheDocument();
  });

  // Test: Get Started scrolls to quick-start section when no callback provided
  it('scrolls to quick-start section when Get Started is clicked without callback', () => {
    // Create a mock element with scrollIntoView
    const mockElement = {
      scrollIntoView: vi.fn()
    };
    vi.spyOn(document, 'getElementById').mockReturnValue(mockElement as unknown as HTMLElement);

    render(<HeroSection />);

    const getStartedButton = screen.getByTestId('get-started-button');
    fireEvent.click(getStartedButton);

    expect(document.getElementById).toHaveBeenCalledWith('quick-start');
    expect(mockElement.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });

    vi.restoreAllMocks();
  });
});

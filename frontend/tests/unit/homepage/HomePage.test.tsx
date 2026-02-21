/**
 * HomePage Unit Tests
 * Owner: Scenario 6 - Main HomePage Assembly
 *
 * Test cases:
 * 1. HomePage renders PublicNavbar component
 * 2. HomePage renders HeroSection component
 * 3. HomePage renders FeaturesSection component
 * 4. HomePage renders DemoSection component
 * 5. HomePage renders SocialProofSection component
 * 6. HomePage renders CTASection component
 * 7. HomePage renders Footer component
 * 8. HomePage is rendered at / route (integration)
 * 9. Theme toggle updates all sections (integration)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom';
import HomePage from '../../../src/pages/HomePage';

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

describe('HomePage', () => {
  beforeEach(() => {
    mockNavigate.mockClear();
  });

  afterEach(() => {
    // Clean up any DOM modifications
    document.documentElement.style.scrollBehavior = '';
  });

  // Test Case 1: HomePage renders PublicNavbar component
  it('renders PublicNavbar component', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const navbar = screen.getByTestId('public-navbar');
    expect(navbar).toBeInTheDocument();
  });

  // Test Case 2: HomePage renders HeroSection component
  it('renders HeroSection component', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toBeInTheDocument();
  });

  // Test Case 3: HomePage renders FeaturesSection component
  it('renders FeaturesSection component', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const featuresSection = screen.getByTestId('features-section');
    expect(featuresSection).toBeInTheDocument();
  });

  // Test Case 4: HomePage renders DemoSection component
  it('renders DemoSection component', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const demoSection = screen.getByTestId('demo-section');
    expect(demoSection).toBeInTheDocument();
  });

  // Test Case 5: HomePage renders SocialProofSection component
  it('renders SocialProofSection component', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const socialProofSection = screen.getByTestId('social-proof-section');
    expect(socialProofSection).toBeInTheDocument();
  });

  // Test Case 6: HomePage renders CTASection component
  it('renders CTASection component', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const ctaSection = screen.getByTestId('cta-section');
    expect(ctaSection).toBeInTheDocument();
  });

  // Test Case 7: HomePage renders Footer component
  it('renders Footer component', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const footer = screen.getByTestId('footer');
    expect(footer).toBeInTheDocument();
  });

  // Test Case 8: HomePage is rendered at / route
  it('renders HomePage component at / route', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/other" element={<div>Other Page</div>} />
        </Routes>
      </MemoryRouter>
    );

    const homepage = screen.getByTestId('homepage');
    expect(homepage).toBeInTheDocument();
  });

  // Test Case 9: All sections update with theme (testing theme classes are present)
  it('has theme-aware styling classes', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const homepage = screen.getByTestId('homepage');
    // Check that the homepage has base theme classes
    expect(homepage).toHaveClass('bg-base-100');

    // Check that key sections use DaisyUI theme variables
    const navbar = screen.getByTestId('public-navbar');
    expect(navbar.className).toContain('bg-');

    const featuresSection = screen.getByTestId('features-section');
    expect(featuresSection).toHaveClass('bg-base-200');

    const ctaSection = screen.getByTestId('cta-section');
    expect(ctaSection).toHaveClass('bg-primary');
  });

  // Additional test: All sections appear in correct order
  it('renders sections in correct order', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const homepage = screen.getByTestId('homepage');
    const main = homepage.querySelector('main');

    // Get all section test IDs in order
    const navbar = screen.getByTestId('public-navbar');
    const heroSection = screen.getByTestId('hero-section');
    const featuresSection = screen.getByTestId('features-section');
    const demoSection = screen.getByTestId('demo-section');
    const socialProofSection = screen.getByTestId('social-proof-section');
    const ctaSection = screen.getByTestId('cta-section');
    const footer = screen.getByTestId('footer');

    // Verify navbar comes before main content
    expect(navbar.compareDocumentPosition(heroSection)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);

    // Verify sections are in correct order within main
    expect(heroSection.compareDocumentPosition(featuresSection)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
    expect(featuresSection.compareDocumentPosition(demoSection)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
    expect(demoSection.compareDocumentPosition(socialProofSection)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
    expect(socialProofSection.compareDocumentPosition(ctaSection)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);

    // Verify footer comes after main content
    expect(ctaSection.compareDocumentPosition(footer)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
  });

  // Additional test: Smooth scroll behavior is enabled
  it('enables smooth scroll behavior on mount', async () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    await waitFor(() => {
      expect(document.documentElement.style.scrollBehavior).toBe('smooth');
    });
  });

  // Additional test: Main container has homepage testid
  it('has a homepage container with correct testid', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const homepage = screen.getByTestId('homepage');
    expect(homepage).toBeInTheDocument();
    expect(homepage).toHaveClass('min-h-screen');
  });

  // Additional test: Navbar has sticky positioning
  it('renders navbar with sticky positioning', () => {
    render(
      <BrowserRouter>
        <HomePage />
      </BrowserRouter>
    );

    const navbar = screen.getByTestId('public-navbar');
    expect(navbar).toHaveClass('sticky');
  });
});

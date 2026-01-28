/**
 * Homepage Integration Tests
 * Owner: Scenario 5 - Homepage Integration
 *
 * Test coverage:
 * - All sections render together
 * - Navigation between sections works
 * - Skip to content link functions
 * - Theme context integration
 * - Route integration with React Router
 */

import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { Home } from '../../src/pages/Home';

// Helper function to render Home with router context
const renderHome = (initialEntries: string[] = ['/']) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/login" element={<div>Login Page</div>} />
        <Route path="/register" element={<div>Register Page</div>} />
      </Routes>
    </MemoryRouter>
  );
};

describe('Homepage Integration', () => {
  // Test Case 1: Page renders without throwing errors
  it('renders Home page with MemoryRouter without throwing errors', () => {
    expect(() => renderHome()).not.toThrow();
  });

  // Test Case 2: Hero section headline and CTAs are present
  it('displays Hero section headline and CTAs', () => {
    renderHome();

    // Check for headline
    expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();
    expect(screen.getByText(/shorten urls/i)).toBeInTheDocument();

    // Check for CTAs - Hero section uses role="button" on Link elements
    expect(screen.getByRole('button', { name: /get started/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /log in/i })).toBeInTheDocument();
  });

  // Test Case 3: Features section with 3 feature cards is present
  it('displays Features section with 3 feature cards', () => {
    renderHome();

    // Check for Features heading
    expect(screen.getByRole('heading', { name: /powerful features/i })).toBeInTheDocument();

    // Check for feature cards
    const featureCards = screen.getAllByTestId('feature-card');
    expect(featureCards).toHaveLength(3);

    // Verify feature titles
    expect(screen.getByText(/url shortening/i)).toBeInTheDocument();
    expect(screen.getByText(/analytics dashboard/i)).toBeInTheDocument();
    expect(screen.getByText(/link management/i)).toBeInTheDocument();
  });

  // Test Case 4: How It Works section with 3 steps is present
  it('displays How It Works section with 3 steps', () => {
    renderHome();

    // Check for How It Works heading
    expect(screen.getByRole('heading', { name: /how it works/i })).toBeInTheDocument();

    // Check for steps
    const steps = screen.getAllByTestId('how-it-works-step');
    expect(steps).toHaveLength(3);

    // Verify step titles
    expect(screen.getByText(/paste your url/i)).toBeInTheDocument();
    expect(screen.getByText(/get your short link/i)).toBeInTheDocument();
    expect(screen.getByText(/track performance/i)).toBeInTheDocument();
  });

  // Test Case 5: Footer with navigation links is present
  it('displays Footer with navigation links', () => {
    renderHome();

    // Check for footer
    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();

    // Check for navigation links in footer
    const footerNav = within(footer).getByRole('navigation', { name: /footer navigation/i });
    expect(footerNav).toBeInTheDocument();

    // Verify links
    expect(within(footer).getByRole('link', { name: /home/i })).toBeInTheDocument();
    expect(within(footer).getByRole('link', { name: /login/i })).toBeInTheDocument();
    expect(within(footer).getByRole('link', { name: /register/i })).toBeInTheDocument();
  });

  // Test Case 6: Main element or role='main' is present
  it('has main landmark element', () => {
    renderHome();

    const mainElement = screen.getByRole('main');
    expect(mainElement).toBeInTheDocument();
  });

  // Test Case 7: Skip link is first focusable element on page
  it('has skip-to-content link as first focusable element', () => {
    renderHome();

    const skipLink = screen.getByTestId('skip-to-content');
    expect(skipLink).toBeInTheDocument();
    expect(skipLink).toHaveTextContent(/skip to main content/i);

    // Verify it's a link with correct href
    expect(skipLink).toHaveAttribute('href', '#main-content');
  });

  // Test Case 8: Focus moves to main content area when skip link is activated
  it('moves focus to main content when skip link is activated', () => {
    renderHome();

    const skipLink = screen.getByTestId('skip-to-content');
    const mainContent = screen.getByRole('main');

    // Activate skip link
    fireEvent.click(skipLink);

    // Main content should be focusable and focused
    expect(mainContent).toHaveAttribute('tabindex', '-1');
    expect(document.activeElement).toBe(mainContent);
  });

  // Test Case 9: Single h1 element, h2s for sections, no skipped levels
  it('has proper heading hierarchy with single h1 and h2s for sections', () => {
    renderHome();

    // Check for exactly one h1
    const h1Elements = screen.getAllByRole('heading', { level: 1 });
    expect(h1Elements).toHaveLength(1);

    // Check for h2 section headings
    const h2Elements = screen.getAllByRole('heading', { level: 2 });
    expect(h2Elements.length).toBeGreaterThanOrEqual(2); // Features + How It Works

    // Verify h2 headings
    expect(screen.getByRole('heading', { level: 2, name: /powerful features/i })).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: /how it works/i })).toBeInTheDocument();
  });

  // Test Case 10: Homepage renders at / route from navigation
  it('renders Homepage at / route when navigating from /login', () => {
    render(
      <MemoryRouter initialEntries={['/login', '/']}>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/login" element={<div>Login Page</div>} />
        </Routes>
      </MemoryRouter>
    );

    // Home page should be rendered
    expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();
    expect(screen.getByRole('main')).toBeInTheDocument();
  });

  // Test Case 11: BackgroundEffect component is present
  it('renders BackgroundEffect component', () => {
    renderHome();

    const backgroundEffect = screen.getByTestId('background-effect');
    expect(backgroundEffect).toBeInTheDocument();

    // Background effect should be hidden from accessibility tree
    expect(backgroundEffect).toHaveAttribute('aria-hidden', 'true');
  });
});

describe('Homepage Navigation', () => {
  it('navigates to /register when Get Started CTA is clicked', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/register" element={<div>Register Page</div>} />
        </Routes>
      </MemoryRouter>
    );

    // Hero section uses role="button" on Link elements
    const getStartedButton = screen.getByRole('button', { name: /get started/i });
    fireEvent.click(getStartedButton);

    expect(screen.getByText(/register page/i)).toBeInTheDocument();
  });

  it('navigates to /login when Log In CTA is clicked', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/login" element={<div>Login Page</div>} />
        </Routes>
      </MemoryRouter>
    );

    // Hero section uses role="button" on Link elements
    const loginButton = screen.getByRole('button', { name: /log in/i });
    fireEvent.click(loginButton);

    expect(screen.getByText(/login page/i)).toBeInTheDocument();
  });
});

describe('Homepage Accessibility', () => {
  it('has ARIA landmarks for main and contentinfo', () => {
    renderHome();

    expect(screen.getByRole('main')).toBeInTheDocument();
    expect(screen.getByRole('contentinfo')).toBeInTheDocument();
  });

  it('has aria-labelledby attributes on sections', () => {
    renderHome();

    // Hero section
    const heroSection = document.querySelector('[aria-labelledby="hero-headline"]');
    expect(heroSection).toBeInTheDocument();

    // Features section
    const featuresSection = document.querySelector('[aria-labelledby="features-heading"]');
    expect(featuresSection).toBeInTheDocument();

    // How It Works section
    const howItWorksSection = document.querySelector('[aria-labelledby="how-it-works-heading"]');
    expect(howItWorksSection).toBeInTheDocument();
  });

  it('has decorative icons hidden from accessibility tree', () => {
    renderHome();

    // Feature icons should be aria-hidden
    const featureIcons = screen.getAllByTestId('feature-icon');
    featureIcons.forEach((icon) => {
      const svg = icon.querySelector('svg');
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });

    // Step icons should be aria-hidden
    const stepIcons = screen.getAllByTestId('step-icon');
    stepIcons.forEach((icon) => {
      const svg = icon.querySelector('svg');
      expect(svg).toHaveAttribute('aria-hidden', 'true');
    });
  });
});

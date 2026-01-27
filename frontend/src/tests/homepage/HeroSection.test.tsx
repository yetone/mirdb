/**
 * HeroSection Component Tests
 * Owner: Scenario 1 - Hero Section Implementation
 *
 * Tests for:
 * - Component rendering with full viewport height
 * - Product tagline visibility
 * - Value proposition subtitle
 * - Primary CTA button functionality
 * - Login link functionality
 * - BackgroundEffect integration
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import HeroSection from '../../components/homepage/HeroSection';

// Mock useNavigate
const mockNavigate = vi.fn();

vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

// Mock the BackgroundEffect component to avoid Three.js WebGL issues
vi.mock('../../components/BackgroundEffect', () => ({
  default: () => (
    <div data-testid="background-effect" aria-hidden="true">
      <canvas data-testid="background-canvas" />
    </div>
  ),
}));

const renderWithRouter = (component: React.ReactElement) => {
  return render(<MemoryRouter>{component}</MemoryRouter>);
};

describe('HeroSection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Component renders with full viewport height and visible content
  it('renders with full viewport height and visible content', () => {
    renderWithRouter(<HeroSection />);

    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toBeInTheDocument();
    expect(heroSection).toHaveClass('min-h-screen');
  });

  // Test Case 2: Product tagline is visible
  it('displays the product tagline "SHORTEN. TRACK. GROW."', () => {
    renderWithRouter(<HeroSection />);

    const tagline = screen.getByTestId('hero-tagline');
    expect(tagline).toBeInTheDocument();
    expect(tagline).toHaveTextContent('SHORTEN. TRACK. GROW.');
  });

  // Test Case 3: Value proposition subtitle is visible
  it('displays the value proposition subtitle', () => {
    renderWithRouter(<HeroSection />);

    const valueProp = screen.getByTestId('hero-value-proposition');
    expect(valueProp).toBeInTheDocument();
    expect(valueProp.textContent).toContain('Transform long URLs');
    expect(valueProp.textContent).toContain('analytics');
  });

  // Test Case 4: Primary CTA button is visible and clickable
  it('displays the primary CTA button "Get Started Free"', () => {
    renderWithRouter(<HeroSection />);

    const ctaButton = screen.getByRole('button', { name: /get started free/i });
    expect(ctaButton).toBeInTheDocument();
    expect(ctaButton).toBeEnabled();
  });

  // Test Case 5: Clicking primary CTA navigates to /register
  it('navigates to /register when CTA button is clicked', () => {
    renderWithRouter(<HeroSection />);

    const ctaButton = screen.getByRole('button', { name: /get started free/i });
    fireEvent.click(ctaButton);

    expect(mockNavigate).toHaveBeenCalledWith('/register');
  });

  // Test Case 5 Alternative: Uses custom callback when provided
  it('calls onGetStarted callback when provided', () => {
    const onGetStarted = vi.fn();
    renderWithRouter(<HeroSection onGetStarted={onGetStarted} />);

    const ctaButton = screen.getByRole('button', { name: /get started free/i });
    fireEvent.click(ctaButton);

    expect(onGetStarted).toHaveBeenCalled();
    expect(mockNavigate).not.toHaveBeenCalled();
  });

  // Test Case 6: Login link is visible
  it('displays the login link with "Sign in" text', () => {
    renderWithRouter(<HeroSection />);

    const loginLink = screen.getByTestId('hero-login-link');
    expect(loginLink).toBeInTheDocument();
    expect(loginLink).toHaveTextContent('Sign in');
  });

  // Test Case 7: Clicking login link navigates to /login
  it('navigates to /login when login link is clicked', () => {
    renderWithRouter(<HeroSection />);

    const loginLink = screen.getByTestId('hero-login-link');
    fireEvent.click(loginLink);

    expect(mockNavigate).toHaveBeenCalledWith('/login');
  });

  // Test Case 7 Alternative: Uses custom callback when provided
  it('calls onLogin callback when provided', () => {
    const onLogin = vi.fn();
    renderWithRouter(<HeroSection onLogin={onLogin} />);

    const loginLink = screen.getByTestId('hero-login-link');
    fireEvent.click(loginLink);

    expect(onLogin).toHaveBeenCalled();
    expect(mockNavigate).not.toHaveBeenCalled();
  });

  // Test Case 8: BackgroundEffect is rendered (canvas element present)
  it('renders the BackgroundEffect component with canvas', () => {
    renderWithRouter(<HeroSection />);

    const backgroundEffect = screen.getByTestId('background-effect');
    expect(backgroundEffect).toBeInTheDocument();
  });

  // Additional tests for completeness
  it('has the correct typography classes for the heading', () => {
    renderWithRouter(<HeroSection />);

    const tagline = screen.getByTestId('hero-tagline');
    expect(tagline).toHaveClass('text-5xl');
    expect(tagline).toHaveClass('font-bold');
  });

  it('renders the "Already have an account?" text', () => {
    renderWithRouter(<HeroSection />);

    expect(screen.getByText(/already have an account/i)).toBeInTheDocument();
  });
});

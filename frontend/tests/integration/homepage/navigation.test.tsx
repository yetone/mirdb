/**
 * Navigation Integration Tests
 * Owner: Scenario 2 - Navigation & Routing (shared with Scenario 1 for CTA navigation test)
 *
 * Test cases from Scenario 1:
 * - Test case 4: Click primary CTA button -> navigates to /register route
 *
 * Test cases from Scenario 2:
 * - Test case 2: Click Login button -> navigates to /login route
 * - Test case 3: Click Sign Up button -> navigates to /register route
 * - Test case 4: Click logo -> navigates to / (homepage) route
 * - Test case 7: Scroll page down -> navigation has sticky positioning and shadow
 * - Test case 8: Route '/' renders HomePage component
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import HeroSection from '../../../src/components/homepage/HeroSection';
import PublicNavbar from '../../../src/components/homepage/PublicNavbar';
import HomePage from '../../../src/pages/HomePage';

describe('HeroSection Navigation Integration', () => {
  // Test Case 4: CTA button navigates to /register route
  it('navigates to /register when primary CTA is clicked', async () => {
    const TestApp = () => (
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route
            path="/"
            element={<HeroSection />}
          />
          <Route
            path="/register"
            element={<div data-testid="register-page">Register Page</div>}
          />
        </Routes>
      </MemoryRouter>
    );

    render(<TestApp />);

    // Verify we're on the hero section
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();

    // Click the primary CTA button
    const ctaButton = screen.getByTestId('hero-cta-primary');
    fireEvent.click(ctaButton);

    // Wait for navigation to /register
    await waitFor(() => {
      expect(screen.getByTestId('register-page')).toBeInTheDocument();
    });
  });
});

describe('PublicNavbar Navigation Integration', () => {
  // Test Case 2: Click Login button navigates to /login
  it('navigates to /login when Login button is clicked', async () => {
    const TestApp = () => (
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route
            path="/"
            element={<PublicNavbar />}
          />
          <Route
            path="/login"
            element={<div data-testid="login-page">Login Page</div>}
          />
        </Routes>
      </MemoryRouter>
    );

    render(<TestApp />);

    // Verify navbar is present
    expect(screen.getByTestId('public-navbar')).toBeInTheDocument();

    // Click the Login button
    const loginButton = screen.getByTestId('navbar-login-btn');
    fireEvent.click(loginButton);

    // Wait for navigation to /login
    await waitFor(() => {
      expect(screen.getByTestId('login-page')).toBeInTheDocument();
    });
  });

  // Test Case 3: Click Sign Up button navigates to /register
  it('navigates to /register when Sign Up button is clicked', async () => {
    const TestApp = () => (
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route
            path="/"
            element={<PublicNavbar />}
          />
          <Route
            path="/register"
            element={<div data-testid="register-page">Register Page</div>}
          />
        </Routes>
      </MemoryRouter>
    );

    render(<TestApp />);

    // Verify navbar is present
    expect(screen.getByTestId('public-navbar')).toBeInTheDocument();

    // Click the Sign Up button
    const signUpButton = screen.getByTestId('navbar-signup-btn');
    fireEvent.click(signUpButton);

    // Wait for navigation to /register
    await waitFor(() => {
      expect(screen.getByTestId('register-page')).toBeInTheDocument();
    });
  });

  // Test Case 4: Click logo navigates to homepage
  it('navigates to / when logo is clicked', async () => {
    const TestApp = () => (
      <MemoryRouter initialEntries={['/other']}>
        <Routes>
          <Route
            path="/other"
            element={<PublicNavbar />}
          />
          <Route
            path="/"
            element={<div data-testid="home-page">Home Page</div>}
          />
        </Routes>
      </MemoryRouter>
    );

    render(<TestApp />);

    // Verify navbar is present
    expect(screen.getByTestId('public-navbar')).toBeInTheDocument();

    // Click the logo
    const logo = screen.getByTestId('navbar-logo');
    fireEvent.click(logo);

    // Wait for navigation to /
    await waitFor(() => {
      expect(screen.getByTestId('home-page')).toBeInTheDocument();
    });
  });

  // Test Case 7: Scroll triggers sticky navigation with shadow
  it('applies shadow effect when page is scrolled', async () => {
    const TestApp = () => (
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route
            path="/"
            element={
              <div style={{ height: '2000px' }}>
                <PublicNavbar transparent={true} />
                <div style={{ height: '1000px' }}>Content</div>
              </div>
            }
          />
        </Routes>
      </MemoryRouter>
    );

    render(<TestApp />);

    const navbar = screen.getByTestId('public-navbar');

    // Initially, navbar should be transparent
    expect(navbar.className).toContain('bg-transparent');

    // Simulate scroll
    Object.defineProperty(window, 'scrollY', { value: 200, writable: true });
    fireEvent.scroll(window);

    // After scroll, navbar should have shadow
    await waitFor(() => {
      expect(navbar.className).toContain('shadow-md');
    });
  });
});

describe('HomePage Route Configuration', () => {
  // Test Case 8: Route '/' renders HomePage component
  it('renders HomePage component at / route', () => {
    const TestApp = () => (
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
          <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
        </Routes>
      </MemoryRouter>
    );

    render(<TestApp />);

    // HomePage should render with PublicNavbar and HeroSection
    expect(screen.getByTestId('public-navbar')).toBeInTheDocument();
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
  });

  it('renders login and register placeholders for unauthenticated routes', () => {
    // Test login route
    const { unmount } = render(
      <MemoryRouter initialEntries={['/login']}>
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
        </Routes>
      </MemoryRouter>
    );

    expect(screen.getByTestId('login-page')).toBeInTheDocument();
    unmount();

    // Test register route
    render(
      <MemoryRouter initialEntries={['/register']}>
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
        </Routes>
      </MemoryRouter>
    );

    expect(screen.getByTestId('register-page')).toBeInTheDocument();
  });
});

describe('Mobile Navigation Integration', () => {
  it('mobile login button navigates to /login', async () => {
    const TestApp = () => (
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route
            path="/"
            element={<PublicNavbar />}
          />
          <Route
            path="/login"
            element={<div data-testid="login-page">Login Page</div>}
          />
        </Routes>
      </MemoryRouter>
    );

    render(<TestApp />);

    // Open mobile menu
    const menuButton = screen.getByTestId('navbar-mobile-menu-btn');
    fireEvent.click(menuButton);

    // Click mobile login button
    const mobileLoginBtn = screen.getByTestId('mobile-login-btn');
    fireEvent.click(mobileLoginBtn);

    // Should navigate to login
    await waitFor(() => {
      expect(screen.getByTestId('login-page')).toBeInTheDocument();
    });
  });

  it('mobile sign up button navigates to /register', async () => {
    const TestApp = () => (
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route
            path="/"
            element={<PublicNavbar />}
          />
          <Route
            path="/register"
            element={<div data-testid="register-page">Register Page</div>}
          />
        </Routes>
      </MemoryRouter>
    );

    render(<TestApp />);

    // Open mobile menu
    const menuButton = screen.getByTestId('navbar-mobile-menu-btn');
    fireEvent.click(menuButton);

    // Click mobile sign up button
    const mobileSignUpBtn = screen.getByTestId('mobile-signup-btn');
    fireEvent.click(mobileSignUpBtn);

    // Should navigate to register
    await waitFor(() => {
      expect(screen.getByTestId('register-page')).toBeInTheDocument();
    });
  });
});

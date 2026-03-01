import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { render } from '../../utils/render';
import Home from '../../../src/pages/Home';

/**
 * Home Page Unit Tests
 * Owner: Scenario 1 - Hero Section Display and Content
 *
 * Tests for the main Home page component.
 *
 * Test coverage:
 * - Component renders without errors
 * - Hero section is present
 * - CTA buttons are rendered
 * - All sections are included
 */

describe('Home Page', () => {
  it('renders without errors', () => {
    render(<Home />);

    // The page should render the main element
    const mainElement = screen.getByRole('main');
    expect(mainElement).toBeInTheDocument();
  });

  it('displays the product name in the hero section', () => {
    render(<Home />);

    const productName = screen.getByRole('heading', { level: 1, name: /url shortener/i });
    expect(productName).toBeInTheDocument();
  });

  it('displays the tagline describing URL shortening service', () => {
    render(<Home />);

    const tagline = screen.getByTestId('hero-tagline');
    expect(tagline).toBeInTheDocument();
    expect(tagline).toHaveTextContent(/shorten, share, and track your links/i);
  });

  it('displays the Sign Up button', () => {
    render(<Home />);

    // There may be multiple Sign Up elements (navbar + hero)
    const signUpButtons = screen.getAllByRole('link', { name: /sign up/i });
    expect(signUpButtons.length).toBeGreaterThan(0);

    // At least one should link to /register
    const registerLinks = signUpButtons.filter(btn => btn.getAttribute('href') === '/register');
    expect(registerLinks.length).toBeGreaterThan(0);
  });

  it('displays the Log In button', () => {
    render(<Home />);

    // There may be multiple Log In elements (navbar + hero)
    const logInButtons = screen.getAllByRole('link', { name: /log in/i });
    expect(logInButtons.length).toBeGreaterThan(0);

    // At least one should link to /login
    const loginLinks = logInButtons.filter(btn => btn.getAttribute('href') === '/login');
    expect(loginLinks.length).toBeGreaterThan(0);
  });

  it('renders the navigation bar', () => {
    render(<Home />);

    const navbar = screen.getByRole('navigation');
    expect(navbar).toBeInTheDocument();
  });

  it('renders the hero section', () => {
    render(<Home />);

    const heroSection = screen.getByRole('region', { name: /hero section/i });
    expect(heroSection).toBeInTheDocument();
  });
});

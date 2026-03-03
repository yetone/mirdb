/**
 * Home Page Tests
 * Owner: Scenario 1 - Homepage Hero Section Rendering
 *
 * Integration tests for the Home page component.
 */

import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { describe, it, expect } from 'vitest';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { AuthProvider } from '../../src/contexts/AuthContext';
import { Home } from '../../src/pages/Home';

// Helper to render with all required providers
const renderWithProviders = (ui: React.ReactElement) => {
  return render(
    <ThemeProvider>
      <AuthProvider>
        <BrowserRouter>{ui}</BrowserRouter>
      </AuthProvider>
    </ThemeProvider>
  );
};

describe('Home Page', () => {
  it('renders without errors', () => {
    renderWithProviders(<Home />);

    const homePage = screen.getByTestId('home-page');
    expect(homePage).toBeInTheDocument();
  });

  it('includes the HeroSection component', () => {
    renderWithProviders(<Home />);

    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toBeInTheDocument();
  });

  it('displays the service name headline in HeroSection', () => {
    renderWithProviders(<Home />);

    const headline = screen.getByTestId('hero-headline');
    expect(headline).toBeInTheDocument();
    expect(headline).toHaveTextContent('URL Shortener');
  });

  it('displays the tagline in HeroSection', () => {
    renderWithProviders(<Home />);

    const tagline = screen.getByTestId('hero-tagline');
    expect(tagline).toBeInTheDocument();
    expect(tagline).toHaveTextContent('Shorten URLs, track clicks, analyze your audience');
  });

  it('renders Get Started CTA button', () => {
    renderWithProviders(<Home />);

    const getStartedButton = screen.getByTestId('get-started-button');
    expect(getStartedButton).toBeInTheDocument();
    expect(getStartedButton).toHaveTextContent('Get Started');
  });

  it('renders Sign In CTA button', () => {
    renderWithProviders(<Home />);

    const signInButton = screen.getByTestId('sign-in-button');
    expect(signInButton).toBeInTheDocument();
    expect(signInButton).toHaveTextContent('Sign In');
  });

  it('renders the Navbar component', () => {
    renderWithProviders(<Home />);

    // Check for navbar elements
    const navbar = screen.getByRole('link', { name: /URLShortener/i });
    expect(navbar).toBeInTheDocument();
  });
});

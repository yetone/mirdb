import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { render } from '../../utils/render';
import { HeroSection } from '../../../src/components/homepage';

/**
 * Hero Section Unit Tests
 * Owner: Scenario 1 - Hero Section Display and Content
 *
 * Tests for the HeroSection component
 */

describe('HeroSection', () => {
  it('renders the product name', () => {
    render(<HeroSection />);

    const productName = screen.getByRole('heading', { level: 1 });
    expect(productName).toBeInTheDocument();
    expect(productName).toHaveTextContent('URL Shortener');
  });

  it('renders the tagline describing URL shortening service', () => {
    render(<HeroSection />);

    const tagline = screen.getByTestId('hero-tagline');
    expect(tagline).toBeInTheDocument();
    expect(tagline).toHaveTextContent(/shorten, share, and track your links/i);
  });

  it('renders the value proposition', () => {
    render(<HeroSection />);

    const valueProp = screen.getByTestId('hero-value-prop');
    expect(valueProp).toBeInTheDocument();
    expect(valueProp).toHaveTextContent(/create concise, powerful links/i);
  });

  it('renders the Sign Up button with correct text', () => {
    render(<HeroSection />);

    const signUpButton = screen.getByRole('link', { name: /sign up/i });
    expect(signUpButton).toBeInTheDocument();
    expect(signUpButton).toHaveTextContent('Sign Up');
  });

  it('renders the Log In button with correct text', () => {
    render(<HeroSection />);

    const logInButton = screen.getByRole('link', { name: /log in/i });
    expect(logInButton).toBeInTheDocument();
    expect(logInButton).toHaveTextContent('Log In');
  });

  it('Sign Up button links to /register', () => {
    render(<HeroSection />);

    const signUpButton = screen.getByRole('link', { name: /sign up/i });
    expect(signUpButton).toHaveAttribute('href', '/register');
  });

  it('Log In button links to /login', () => {
    render(<HeroSection />);

    const logInButton = screen.getByRole('link', { name: /log in/i });
    expect(logInButton).toHaveAttribute('href', '/login');
  });

  it('has proper aria-label for accessibility', () => {
    render(<HeroSection />);

    const heroSection = screen.getByRole('region', { name: /hero section/i });
    expect(heroSection).toBeInTheDocument();
  });
});

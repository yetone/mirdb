/**
 * Home Page Unit Tests
 * Owner: Scenario 1 - Homepage Hero Section Rendering
 *
 * Tests for Home.tsx component rendering and composition.
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import Home from '../../src/pages/Home';

function renderWithRouter(ui: React.ReactElement) {
  return render(<MemoryRouter>{ui}</MemoryRouter>);
}

describe('Home', () => {
  it('renders homepage with hero section visible', () => {
    renderWithRouter(<Home />);
    expect(screen.getByTestId('home-page')).toBeInTheDocument();
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
  });

  it('renders hero section with headline element containing non-empty text', () => {
    renderWithRouter(<Home />);
    const headline = screen.getByTestId('hero-headline');
    expect(headline).toBeInTheDocument();
    expect(headline.textContent).toBeTruthy();
    expect(headline.textContent!.length).toBeGreaterThan(0);
  });

  it('renders hero section with subheading element containing descriptive benefit text', () => {
    renderWithRouter(<Home />);
    const subheading = screen.getByTestId('hero-subheading');
    expect(subheading).toBeInTheDocument();
    expect(subheading.textContent).toBeTruthy();
    expect(subheading.textContent!.length).toBeGreaterThan(20);
  });

  it('renders CTA button with href routing to /register page', () => {
    renderWithRouter(<Home />);
    const ctaButton = screen.getByTestId('hero-cta-button');
    expect(ctaButton).toBeInTheDocument();
    expect(ctaButton).toHaveAttribute('href', '/register');
  });

  it('renders URL shortener form inside hero section', () => {
    renderWithRouter(<Home />);
    expect(screen.getByTestId('hero-form-container')).toBeInTheDocument();
    expect(screen.getByTestId('url-input')).toBeInTheDocument();
    expect(screen.getByTestId('shorten-button')).toBeInTheDocument();
  });

  it('does not redirect or require authentication', () => {
    renderWithRouter(<Home />);
    expect(screen.getByTestId('home-page')).toBeInTheDocument();
    expect(screen.queryByText(/login/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/unauthorized/i)).not.toBeInTheDocument();
  });
});

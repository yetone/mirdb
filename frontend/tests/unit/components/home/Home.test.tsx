import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import Home from '../../../../src/pages/Home';

const renderWithRouter = (initialRoute = '/') => {
  return render(
    <MemoryRouter initialEntries={[initialRoute]}>
      <Home />
    </MemoryRouter>
  );
};

describe('Home Page', () => {
  it('renders the homepage without errors at root path', () => {
    renderWithRouter('/');

    expect(screen.getByTestId('homepage')).toBeInTheDocument();
  });

  it('renders the HeroSection component', () => {
    renderWithRouter('/');

    // Check for hero section content
    expect(screen.getByRole('heading', { name: /LinkSnip/i })).toBeInTheDocument();
    expect(screen.getByText('Shorten. Share. Track.')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /Get Started Free/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Learn More/i })).toBeInTheDocument();
  });

  it('renders the FeaturesSection component', () => {
    renderWithRouter('/');

    // Check for features section content
    expect(screen.getByRole('heading', { name: /Powerful Features/i })).toBeInTheDocument();
    expect(screen.getByText(/Lightning Fast URL Shortening/i)).toBeInTheDocument();
  });

  it('renders the HowItWorksSection component', () => {
    renderWithRouter('/');

    // Check for how-it-works section content
    expect(screen.getByRole('heading', { name: /How It Works/i })).toBeInTheDocument();
    expect(screen.getByText('Paste Your URL')).toBeInTheDocument();
  });

  it('renders the Footer component', () => {
    renderWithRouter('/');

    // Check for footer content
    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /Privacy Policy/i })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /Terms of Service/i })).toBeInTheDocument();
  });

  it('renders all sections in correct order within the homepage', () => {
    renderWithRouter('/');

    const homepage = screen.getByTestId('homepage');
    const main = homepage.querySelector('main');

    // Verify main content area exists
    expect(main).toBeInTheDocument();

    // Verify all sections are present (use getAllByText for elements appearing multiple times)
    expect(screen.getAllByText(/LinkSnip/).length).toBeGreaterThan(0);
    expect(screen.getByText(/Powerful Features/)).toBeInTheDocument();
    expect(screen.getByText(/How It Works/)).toBeInTheDocument();

    // Verify footer is present
    expect(screen.getByRole('contentinfo')).toBeInTheDocument();
  });

  it('renders placeholder for QuickShortenForm', () => {
    renderWithRouter('/');

    // Check for placeholder text (Scenario 2 owns the actual form)
    expect(screen.getByText(/Quick URL shortening form coming soon/i)).toBeInTheDocument();
  });
});

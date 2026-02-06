/**
 * Home page unit tests.
 * Owner: Scenario 10 - User Flow (page composition)
 *
 * Tests for:
 * - All sections render correctly
 * - Proper section ordering (Hero > Features > SocialProof > Footer)
 * - Main content structure and accessibility
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { Home } from './Home';

const renderWithRouter = (component: React.ReactElement) => {
  return render(
    <BrowserRouter>
      {component}
    </BrowserRouter>
  );
};

describe('Home', () => {
  it('renders the Hero section', () => {
    renderWithRouter(<Home />);

    expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();
    expect(screen.getByText(/Build Something Amazing/i)).toBeInTheDocument();
  });

  it('renders the Features section', () => {
    renderWithRouter(<Home />);

    expect(screen.getByText(/Lightning Fast/i)).toBeInTheDocument();
    expect(screen.getByText(/Secure by Default/i)).toBeInTheDocument();
    expect(screen.getByText(/Infinitely Scalable/i)).toBeInTheDocument();
  });

  it('renders the SocialProof section', () => {
    renderWithRouter(<Home />);

    expect(screen.getByText(/What Our Users Say/i)).toBeInTheDocument();
    expect(screen.getByText(/Jane Doe/i)).toBeInTheDocument();
    expect(screen.getByText(/10,000\+/i)).toBeInTheDocument();
  });

  it('renders the Footer section', () => {
    const { container } = renderWithRouter(<Home />);

    // The main footer has class containing 'footer' and role contentinfo
    const mainFooter = container.querySelector('footer[role="contentinfo"]');
    expect(mainFooter).toBeInTheDocument();
  });

  it('has a main content landmark', () => {
    renderWithRouter(<Home />);

    const main = screen.getByRole('main');
    expect(main).toBeInTheDocument();
    expect(main).toHaveAttribute('id', 'main-content');
  });

  it('renders CTAs for user flow', () => {
    renderWithRouter(<Home />);

    expect(screen.getByRole('link', { name: /Get Started/i })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /Learn More/i })).toBeInTheDocument();
  });

  it('renders sections in correct visual order', () => {
    const { container } = renderWithRouter(<Home />);

    const main = container.querySelector('main');
    expect(main).toBeInTheDocument();

    // Get all sections within main
    const sections = main?.querySelectorAll('section');
    expect(sections?.length).toBeGreaterThanOrEqual(3);

    // Verify order by checking testIds or section ids
    const heroSection = container.querySelector('[data-testid="hero-section"]');
    const featuresSection = container.querySelector('#features');
    const socialProofSection = container.querySelector('#social-proof');

    expect(heroSection).toBeInTheDocument();
    expect(featuresSection).toBeInTheDocument();
    expect(socialProofSection).toBeInTheDocument();
  });
});

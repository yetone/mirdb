/**
 * Footer Component Tests
 * Owner: Scenario 6 - Footer Navigation Links
 *
 * Tests for Footer component:
 * - Logo/product name display
 * - Navigation links (About, Features, Pricing, Contact)
 * - Copyright notice
 * - Social media icon placeholders
 *
 * Requirements: REQ-8
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { Footer } from '../../../src/components/homepage/Footer';

// Test wrapper with router context
function renderWithRouter(ui: React.ReactElement, { route = '/' } = {}) {
  return render(
    <MemoryRouter initialEntries={[route]}>
      {ui}
    </MemoryRouter>
  );
}

describe('Footer Component', () => {
  /**
   * Test Case 1: Footer contains logo/product name
   * Input: Render Footer component
   * Expected: Footer contains logo/product name
   */
  it('should render the footer with logo/product name', () => {
    renderWithRouter(<Footer />);

    const footer = screen.getByTestId('footer');
    expect(footer).toBeInTheDocument();

    const logo = screen.getByTestId('footer-logo');
    expect(logo).toBeInTheDocument();
    expect(logo).toHaveTextContent(/ShortLink/i);
  });

  it('should display footer branding section with logo and tagline', () => {
    renderWithRouter(<Footer />);

    const branding = screen.getByTestId('footer-branding');
    expect(branding).toBeInTheDocument();

    const tagline = screen.getByTestId('footer-tagline');
    expect(tagline).toBeInTheDocument();
    expect(tagline).toHaveTextContent(/shorten links/i);
  });

  /**
   * Test Case 2: Footer contains 'About' navigation link
   * Input: Render Footer component
   * Expected: Footer contains 'About' navigation link
   */
  it('should contain About navigation link', () => {
    renderWithRouter(<Footer />);

    const aboutLink = screen.getByTestId('footer-link-about');
    expect(aboutLink).toBeInTheDocument();
    expect(aboutLink).toHaveTextContent('About');
    expect(aboutLink).toHaveAttribute('href', '/about');
  });

  /**
   * Test Case 3: Footer contains 'Features' navigation link
   * Input: Render Footer component
   * Expected: Footer contains 'Features' navigation link
   */
  it('should contain Features navigation link', () => {
    renderWithRouter(<Footer />);

    const featuresLink = screen.getByTestId('footer-link-features');
    expect(featuresLink).toBeInTheDocument();
    expect(featuresLink).toHaveTextContent('Features');
    expect(featuresLink).toHaveAttribute('href', '/features');
  });

  /**
   * Test Case 4: Footer contains 'Pricing' navigation link
   * Input: Render Footer component
   * Expected: Footer contains 'Pricing' navigation link
   */
  it('should contain Pricing navigation link', () => {
    renderWithRouter(<Footer />);

    const pricingLink = screen.getByTestId('footer-link-pricing');
    expect(pricingLink).toBeInTheDocument();
    expect(pricingLink).toHaveTextContent('Pricing');
    expect(pricingLink).toHaveAttribute('href', '/pricing');
  });

  /**
   * Test Case 5: Footer contains 'Contact' navigation link
   * Input: Render Footer component
   * Expected: Footer contains 'Contact' navigation link
   */
  it('should contain Contact navigation link', () => {
    renderWithRouter(<Footer />);

    const contactLink = screen.getByTestId('footer-link-contact');
    expect(contactLink).toBeInTheDocument();
    expect(contactLink).toHaveTextContent('Contact');
    expect(contactLink).toHaveAttribute('href', '/contact');
  });

  /**
   * Test Case 6: Footer contains copyright notice
   * Input: Render Footer component
   * Expected: Footer contains copyright notice
   */
  it('should contain copyright notice', () => {
    renderWithRouter(<Footer />);

    const copyright = screen.getByTestId('footer-copyright');
    expect(copyright).toBeInTheDocument();
    expect(copyright).toHaveTextContent(/\u00a9/); // Unicode for copyright symbol
    expect(copyright).toHaveTextContent(/ShortLink/i);
    expect(copyright).toHaveTextContent(/All rights reserved/i);
  });

  it('should display current year in copyright notice', () => {
    renderWithRouter(<Footer />);

    const copyright = screen.getByTestId('footer-copyright');
    const currentYear = new Date().getFullYear().toString();
    expect(copyright).toHaveTextContent(currentYear);
  });

  /**
   * Test Case 7: Footer contains social media icon placeholders
   * Input: Render Footer component
   * Expected: Footer contains social media icon placeholders
   */
  it('should contain social media icon placeholders', () => {
    renderWithRouter(<Footer />);

    const socialSection = screen.getByTestId('footer-social');
    expect(socialSection).toBeInTheDocument();

    // Check for social media links
    const twitterLink = screen.getByTestId('social-twitter');
    expect(twitterLink).toBeInTheDocument();
    expect(twitterLink).toHaveAttribute('aria-label', 'Twitter');

    const githubLink = screen.getByTestId('social-github');
    expect(githubLink).toBeInTheDocument();
    expect(githubLink).toHaveAttribute('aria-label', 'GitHub');

    const linkedinLink = screen.getByTestId('social-linkedin');
    expect(linkedinLink).toBeInTheDocument();
    expect(linkedinLink).toHaveAttribute('aria-label', 'LinkedIn');
  });

  it('should render social media links with proper external link attributes', () => {
    renderWithRouter(<Footer />);

    const socialLinks = [
      screen.getByTestId('social-twitter'),
      screen.getByTestId('social-github'),
      screen.getByTestId('social-linkedin'),
    ];

    socialLinks.forEach((link) => {
      expect(link).toHaveAttribute('target', '_blank');
      expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  /**
   * Additional tests for navigation structure
   */
  it('should render footer navigation columns', () => {
    renderWithRouter(<Footer />);

    const navigation = screen.getByTestId('footer-navigation');
    expect(navigation).toBeInTheDocument();
  });

  it('should contain all required navigation sections', () => {
    renderWithRouter(<Footer />);

    // Check for section headings
    expect(screen.getByText('Product')).toBeInTheDocument();
    expect(screen.getByText('Company')).toBeInTheDocument();
    expect(screen.getByText('Resources')).toBeInTheDocument();
    expect(screen.getByText('Legal')).toBeInTheDocument();
  });

  /**
   * Accessibility tests
   */
  it('should render footer as a footer element', () => {
    renderWithRouter(<Footer />);

    const footer = screen.getByTestId('footer');
    expect(footer.tagName).toBe('FOOTER');
  });

  it('should have aria-label for site footer', () => {
    renderWithRouter(<Footer />);

    const footer = screen.getByTestId('footer');
    expect(footer).toHaveAttribute('aria-label', 'Site footer');
  });

  it('should have proper navigation landmarks for each section', () => {
    renderWithRouter(<Footer />);

    const navElements = screen.getAllByRole('navigation');
    // Should have navigation for Product, Company, Resources, Legal
    expect(navElements.length).toBeGreaterThanOrEqual(4);
  });

  /**
   * All navigation links test
   */
  it('should render all navigation links as proper link elements', () => {
    renderWithRouter(<Footer />);

    // Required links from REQ-8
    const requiredLinks = ['about', 'features', 'pricing', 'contact'];

    requiredLinks.forEach((linkName) => {
      const link = screen.getByTestId(`footer-link-${linkName}`);
      expect(link).toBeInTheDocument();
      expect(link.tagName).toBe('A');
    });
  });
});

/**
 * Footer Navigation Link Tests
 * Tests for link functionality and correct destinations
 */
describe('Footer Navigation Links', () => {
  it('should have all links with valid href attributes', () => {
    renderWithRouter(<Footer />);

    // Get all links in the footer navigation
    const links = screen.getAllByRole('link');

    links.forEach((link) => {
      const href = link.getAttribute('href');
      expect(href).toBeTruthy();
      // Internal links should start with /
      if (!href?.startsWith('http')) {
        expect(href).toMatch(/^\//);
      }
    });
  });

  it('should navigate About link to /about route', () => {
    renderWithRouter(<Footer />);

    const aboutLink = screen.getByTestId('footer-link-about');
    expect(aboutLink).toHaveAttribute('href', '/about');
  });

  it('should navigate Features link to /features route', () => {
    renderWithRouter(<Footer />);

    const featuresLink = screen.getByTestId('footer-link-features');
    expect(featuresLink).toHaveAttribute('href', '/features');
  });

  it('should navigate Pricing link to /pricing route', () => {
    renderWithRouter(<Footer />);

    const pricingLink = screen.getByTestId('footer-link-pricing');
    expect(pricingLink).toHaveAttribute('href', '/pricing');
  });

  it('should navigate Contact link to /contact route', () => {
    renderWithRouter(<Footer />);

    const contactLink = screen.getByTestId('footer-link-contact');
    expect(contactLink).toHaveAttribute('href', '/contact');
  });
});

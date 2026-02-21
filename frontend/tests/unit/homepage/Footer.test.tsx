/**
 * Footer Unit Tests
 * Owner: Scenario 5 - CTA & Footer
 *
 * Test cases:
 * 4. Service name and tagline are displayed
 * 5. Link to Terms of Service is present
 * 6. Link to Privacy Policy is present
 * 7. Contact link or information is present
 * 8. Copyright notice with current year (2026) is displayed
 * 9. All external links have rel='noopener noreferrer' attribute
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Footer from '../../../src/components/homepage/Footer';

describe('Footer', () => {
  // Test Case 4: Service name and tagline are displayed
  it('displays service name and tagline', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    );

    const footer = screen.getByTestId('footer');
    expect(footer).toBeInTheDocument();

    // Check for service name
    const serviceName = screen.getByTestId('footer-service-name');
    expect(serviceName).toBeInTheDocument();
    expect(serviceName.textContent).toBeTruthy();

    // Check for tagline
    const tagline = screen.getByTestId('footer-tagline');
    expect(tagline).toBeInTheDocument();
    expect(tagline.textContent).toBeTruthy();
  });

  // Test Case 5: Link to Terms of Service is present
  it('displays link to Terms of Service', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    );

    const termsLink = screen.getByRole('link', { name: /terms of service/i });
    expect(termsLink).toBeInTheDocument();
    expect(termsLink).toHaveAttribute('href');
  });

  // Test Case 6: Link to Privacy Policy is present
  it('displays link to Privacy Policy', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    );

    const privacyLink = screen.getByRole('link', { name: /privacy policy/i });
    expect(privacyLink).toBeInTheDocument();
    expect(privacyLink).toHaveAttribute('href');
  });

  // Test Case 7: Contact link or information is present
  it('displays contact link or information', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    );

    const contactLink = screen.getByRole('link', { name: /contact/i });
    expect(contactLink).toBeInTheDocument();
    expect(contactLink).toHaveAttribute('href');
  });

  // Test Case 8: Copyright notice with current year (2026) is displayed
  it('displays copyright notice with current year 2026', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    );

    const copyright = screen.getByTestId('footer-copyright');
    expect(copyright).toBeInTheDocument();
    expect(copyright.textContent).toMatch(/2026/);
    expect(copyright.textContent?.toLowerCase()).toMatch(/©|copyright/);
  });

  // Test Case 9: All external links have rel='noopener noreferrer' attribute
  it('ensures all external links have rel="noopener noreferrer"', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    );

    const footer = screen.getByTestId('footer');
    const allLinks = within(footer).getAllByRole('link');

    // Filter for external links (those starting with http or https)
    const externalLinks = allLinks.filter((link) => {
      const href = link.getAttribute('href');
      return href && (href.startsWith('http://') || href.startsWith('https://'));
    });

    // Ensure each external link has the security attributes
    externalLinks.forEach((link) => {
      const rel = link.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });
  });

  // Additional test: Footer uses semantic HTML
  it('uses semantic footer element', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    );

    const footer = screen.getByTestId('footer');
    expect(footer.tagName).toBe('FOOTER');
  });

  // Additional test: Footer link structure
  it('displays navigation links section', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    );

    const linksSection = screen.getByTestId('footer-links');
    expect(linksSection).toBeInTheDocument();

    // Verify we have multiple links
    const links = within(linksSection).getAllByRole('link');
    expect(links.length).toBeGreaterThanOrEqual(3);
  });
});

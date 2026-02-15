import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Footer from '../../../../src/components/home/Footer';

const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>);
};

describe('Footer', () => {
  it('renders the brand name', () => {
    renderWithRouter(<Footer />);

    expect(screen.getByText('LinkSnip')).toBeInTheDocument();
  });

  it('renders custom brand name when provided', () => {
    renderWithRouter(<Footer brandName="MyBrand" />);

    expect(screen.getByText('MyBrand')).toBeInTheDocument();
  });

  it('renders the brand logo icon', () => {
    renderWithRouter(<Footer />);

    // The Link2 icon should be rendered (check for SVG element)
    const footer = screen.getByRole('contentinfo');
    expect(footer.querySelector('svg')).toBeInTheDocument();
  });

  it('renders Privacy Policy link', () => {
    renderWithRouter(<Footer />);

    const privacyLink = screen.getByRole('link', { name: /Privacy Policy/i });
    expect(privacyLink).toBeInTheDocument();
    expect(privacyLink).toHaveAttribute('href', '/privacy');
  });

  it('renders Terms of Service link', () => {
    renderWithRouter(<Footer />);

    const termsLink = screen.getByRole('link', { name: /Terms of Service/i });
    expect(termsLink).toBeInTheDocument();
    expect(termsLink).toHaveAttribute('href', '/terms');
  });

  it('displays copyright notice with current year', () => {
    renderWithRouter(<Footer />);

    const currentYear = new Date().getFullYear();
    expect(screen.getByText(new RegExp(`Copyright.*${currentYear}`))).toBeInTheDocument();
  });

  it('displays brand description text', () => {
    renderWithRouter(<Footer />);

    expect(
      screen.getByText(/Shorten URLs, track clicks, and analyze your link performance/i)
    ).toBeInTheDocument();
  });
});

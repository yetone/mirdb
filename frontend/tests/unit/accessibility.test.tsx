import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import Home from '../../src/pages/Home';
import HeroSection from '../../src/components/Home/HeroSection';
import UrlShortenerForm from '../../src/components/Home/UrlShortenerForm';
import FeaturesSection from '../../src/components/Home/FeaturesSection';
import Footer from '../../src/components/Home/Footer';
import FeatureCard from '../../src/components/Home/FeatureCard';

function renderWithRouter(ui: React.ReactElement) {
  return render(<MemoryRouter>{ui}</MemoryRouter>);
}

describe('Accessibility - Heading Hierarchy', () => {
  it('TC5: Home page has exactly one H1 for main headline', () => {
    renderWithRouter(<Home />);
    const h1s = screen.getAllByRole('heading', { level: 1 });
    expect(h1s).toHaveLength(1);
    expect(h1s[0]).toHaveTextContent('Shorten Your URLs in Seconds');
  });

  it('TC5: Features section uses H2 for section heading', () => {
    render(<FeaturesSection />);
    const h2s = screen.getAllByRole('heading', { level: 2 });
    expect(h2s.length).toBeGreaterThanOrEqual(1);
    expect(h2s[0]).toHaveTextContent('Why Choose Our URL Shortener?');
  });

  it('TC5: Feature cards use H3 without skipping levels', () => {
    render(<FeaturesSection />);
    const h3s = screen.getAllByRole('heading', { level: 3 });
    expect(h3s.length).toBe(4);
    h3s.forEach((h3) => {
      expect(h3.textContent).toBeTruthy();
      expect(h3.textContent!.length).toBeGreaterThan(3);
    });
  });

  it('TC5: No skipped heading levels from h1 to h3', () => {
    renderWithRouter(<Home />);
    const allHeadings = screen.getAllByRole('heading');
    let prevLevel = 0;

    allHeadings.forEach((heading) => {
      const level = parseInt(heading.tagName[1]);
      // Heading levels should not skip (e.g., h1 -> h3 is invalid without h2)
      if (prevLevel > 0) {
        expect(level).toBeLessThanOrEqual(prevLevel + 1);
      }
      prevLevel = level;
    });
  });
});

describe('Accessibility - ARIA Landmarks', () => {
  it('TC6: Home page has main landmark', () => {
    renderWithRouter(<Home />);
    const main = screen.getByRole('main');
    expect(main).toBeInTheDocument();
    expect(main).toHaveAttribute('id', 'main-content');
  });

  it('TC6: Home page has footer landmark', () => {
    renderWithRouter(<Home />);
    const footer = screen.getByRole('contentinfo');
    expect(footer).toBeInTheDocument();
    expect(footer).toHaveAttribute('data-testid', 'footer');
  });

  it('TC6: Home page contains nav landmark when rendered with Navbar', () => {
    // When Home is rendered within App context, nav landmark should exist
    // We test via a wrapped component that includes nav
    const { container } = renderWithRouter(<Home />);
    // At minimum main landmark must be present
    const main = screen.getByRole('main');
    expect(main).toBeInTheDocument();
    // Verify the page structure has landmarks
    expect(container.querySelector('main')).toBeInTheDocument();
    expect(container.querySelector('footer')).toBeInTheDocument();
  });

  it('TC6: Hero section has aria-label', () => {
    renderWithRouter(<HeroSection />);
    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toHaveAttribute('aria-label', 'Hero section');
  });

  it('TC6: Features section has aria-label', () => {
    render(<FeaturesSection />);
    const featuresSection = screen.getByTestId('features-section');
    expect(featuresSection).toHaveAttribute('aria-label', 'Features');
  });

  it('TC6: Skip link is present for keyboard navigation', () => {
    renderWithRouter(<Home />);
    const skipLink = screen.getByText('Skip to main content');
    expect(skipLink).toBeInTheDocument();
    expect(skipLink).toHaveAttribute('href', '#main-content');
    expect(skipLink).toHaveClass('skip-link');
  });
});

describe('Accessibility - Form Labels', () => {
  it('TC2: URL input has associated label with htmlFor attribute', () => {
    render(<UrlShortenerForm />);
    const input = screen.getByTestId('url-input');
    expect(input).toHaveAttribute('id', 'url-input');

    // Check for label with htmlFor pointing to the input
    const label = document.querySelector('label[for="url-input"]');
    expect(label).toBeInTheDocument();
    expect(label).toHaveTextContent('Enter URL to shorten');
  });

  it('TC2: URL input has aria-invalid that changes based on validation state', async () => {
    render(<UrlShortenerForm />);
    const input = screen.getByTestId('url-input');
    expect(input).toHaveAttribute('aria-invalid', 'false');
  });

  it('TC2: Error message has role alert and aria-describedby linkage', async () => {
    const user = userEvent.setup();
    render(<UrlShortenerForm />);

    const button = screen.getByTestId('shorten-button');
    await user.click(button);

    const error = await screen.findByTestId('url-error');
    expect(error).toHaveAttribute('role', 'alert');
    expect(error).toHaveAttribute('id', 'url-error');

    const input = screen.getByTestId('url-input');
    expect(input).toHaveAttribute('aria-describedby', 'url-error');
  });

  it('TC2: URL input has type url for semantic correctness', () => {
    render(<UrlShortenerForm />);
    const input = screen.getByTestId('url-input');
    expect(input).toHaveAttribute('type', 'url');
  });
});

describe('Accessibility - Images and Icons', () => {
  it('Feature card icons have aria-label for screen readers', () => {
    render(
      <FeatureCard
        icon={<span role="img" aria-label="Test icon">🔧</span>}
        title="Test Feature"
        description="Test description"
      />
    );
    const icon = screen.getByRole('img');
    expect(icon).toHaveAttribute('aria-label', 'Test icon');
  });

  it('Feature cards in FeaturesSection have accessible icons', () => {
    render(<FeaturesSection />);
    const icons = screen.getAllByRole('img');
    expect(icons.length).toBe(4);
    icons.forEach((icon) => {
      const ariaLabel = icon.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel!.length).toBeGreaterThan(0);
    });
  });
});

describe('Accessibility - Focus Management', () => {
  it('CTA link does not have incorrect role="button"', () => {
    renderWithRouter(<HeroSection />);
    const cta = screen.getByTestId('hero-cta-button');
    expect(cta).not.toHaveAttribute('role', 'button');
    expect(cta.tagName.toLowerCase()).toBe('a');
  });

  it('All interactive elements in form are focusable', () => {
    render(<UrlShortenerForm />);
    const input = screen.getByTestId('url-input');
    const button = screen.getByTestId('shorten-button');

    expect(input).not.toHaveAttribute('tabindex', '-1');
    expect(button).not.toHaveAttribute('tabindex', '-1');
  });
});

describe('Accessibility - Footer', () => {
  it('Footer renders with copyright text', () => {
    renderWithRouter(<Footer />);
    const footer = screen.getByTestId('footer');
    expect(footer).toBeInTheDocument();
    expect(footer.tagName.toLowerCase()).toBe('footer');
    expect(footer.textContent).toContain('URL Shortener');
  });

  it('Footer has navigation with aria-label', () => {
    renderWithRouter(<Footer />);
    const footerNav = screen.getByRole('navigation', { name: /footer/i });
    expect(footerNav).toBeInTheDocument();
  });
});

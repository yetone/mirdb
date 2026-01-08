import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home from './Home';

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

const renderHome = () => {
  return render(
    <BrowserRouter>
      <Home />
    </BrowserRouter>
  );
};

// Navigation Tests (Scenario: Navigation Links Functionality - REQ-3)
describe('Home Page - Navigation Links', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Login button/link is visible from Home component with Navbar
  describe('Login navigation visibility', () => {
    it('displays Login button/link in the navigation on Home page', () => {
      renderHome();

      // Use aria-label to find the primary login link in the navbar (not mobile menu)
      const loginLink = screen.getByLabelText(/go to login page/i);
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toBeVisible();
      expect(loginLink).toHaveAttribute('href', '/login');
    });
  });

  // Test Case 2: Register/Sign Up button/link is visible from Home component with Navbar
  describe('Register navigation visibility', () => {
    it('displays Register/Sign Up button/link in the navigation on Home page', () => {
      renderHome();

      // Use aria-label to find the primary sign up link in the navbar
      const signUpLink = screen.getByLabelText(/go to registration page/i);
      expect(signUpLink).toBeInTheDocument();
      expect(signUpLink).toBeVisible();
      expect(signUpLink).toHaveAttribute('href', '/register');
    });
  });

  // Test Case 5: Logo/brand navigation
  describe('Logo navigation', () => {
    it('displays logo/brand name in navigation on Home page', () => {
      renderHome();

      const logoLink = screen.getByRole('link', { name: /go to homepage/i });
      expect(logoLink).toBeInTheDocument();
      expect(logoLink).toHaveTextContent('URLShort');
      expect(logoLink).toHaveAttribute('href', '/');
    });
  });

  // Additional Home page specific tests
  describe('Hero section', () => {
    it('displays main headline', () => {
      renderHome();

      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(/shorten, share, track/i);
    });
  });
});

// Feature Cards Tests (Scenario: Feature Cards Display)
describe('Home - Feature Cards Display', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Test Case 1: At least 3 GlassMorphismCard components are rendered', () => {
    it('renders at least 3 feature cards using GlassMorphismCard components', () => {
      renderHome();

      // Get all feature cards
      const featureCards = screen.getAllByTestId(/^feature-card-/);

      // Verify at least 3 cards are rendered
      expect(featureCards.length).toBeGreaterThanOrEqual(3);
    });

    it('renders the features section', () => {
      renderHome();

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toBeInTheDocument();
    });

    it('renders the features grid', () => {
      renderHome();

      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toBeInTheDocument();
    });
  });

  describe('Test Case 2: URL Shortening feature card with appropriate icon and description', () => {
    it('renders URL Shortening feature card', () => {
      renderHome();

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening');
      expect(urlShorteningCard).toBeInTheDocument();
    });

    it('URL Shortening card has correct title', () => {
      renderHome();

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening');
      const title = urlShorteningCard.querySelector('[data-testid="card-title"]');

      expect(title).toBeInTheDocument();
      expect(title?.textContent).toBe('URL Shortening');
    });

    it('URL Shortening card has description', () => {
      renderHome();

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening');
      const description = urlShorteningCard.querySelector('[data-testid="card-description"]');

      expect(description).toBeInTheDocument();
      expect(description?.textContent).toContain('short links');
    });

    it('URL Shortening card has icon', () => {
      renderHome();

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening');
      const icon = urlShorteningCard.querySelector('[data-testid="card-icon"]');

      expect(icon).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Analytics Dashboard feature card with chart/stats icon', () => {
    it('renders Analytics Dashboard feature card', () => {
      renderHome();

      const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard');
      expect(analyticsCard).toBeInTheDocument();
    });

    it('Analytics Dashboard card has correct title', () => {
      renderHome();

      const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard');
      const title = analyticsCard.querySelector('[data-testid="card-title"]');

      expect(title).toBeInTheDocument();
      expect(title?.textContent).toBe('Analytics Dashboard');
    });

    it('Analytics Dashboard card has description about tracking', () => {
      renderHome();

      const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard');
      const description = analyticsCard.querySelector('[data-testid="card-description"]');

      expect(description).toBeInTheDocument();
      expect(description?.textContent).toContain('Track');
    });

    it('Analytics Dashboard card has chart/stats icon', () => {
      renderHome();

      const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard');
      const icon = analyticsCard.querySelector('[data-testid="card-icon"]');

      expect(icon).toBeInTheDocument();
      // Icon should contain an SVG element (the BarChart3 icon)
      const svgElement = icon?.querySelector('svg');
      expect(svgElement).toBeInTheDocument();
    });
  });

  describe('Test Case 4: Geographic Insights feature card with location/globe icon', () => {
    it('renders Geographic Insights feature card', () => {
      renderHome();

      const geoCard = screen.getByTestId('feature-card-geographic-insights');
      expect(geoCard).toBeInTheDocument();
    });

    it('Geographic Insights card has correct title', () => {
      renderHome();

      const geoCard = screen.getByTestId('feature-card-geographic-insights');
      const title = geoCard.querySelector('[data-testid="card-title"]');

      expect(title).toBeInTheDocument();
      expect(title?.textContent).toBe('Geographic Insights');
    });

    it('Geographic Insights card has description about location/audience', () => {
      renderHome();

      const geoCard = screen.getByTestId('feature-card-geographic-insights');
      const description = geoCard.querySelector('[data-testid="card-description"]');

      expect(description).toBeInTheDocument();
      expect(description?.textContent).toContain('audience');
    });

    it('Geographic Insights card has globe/location icon', () => {
      renderHome();

      const geoCard = screen.getByTestId('feature-card-geographic-insights');
      const icon = geoCard.querySelector('[data-testid="card-icon"]');

      expect(icon).toBeInTheDocument();
      // Icon should contain an SVG element (the Globe icon)
      const svgElement = icon?.querySelector('svg');
      expect(svgElement).toBeInTheDocument();
    });
  });

  describe('Test Case 5: Each feature card has title, description, and visual icon element', () => {
    it('all feature cards have a title', () => {
      renderHome();

      const featureCards = screen.getAllByTestId(/^feature-card-/);

      featureCards.forEach((card) => {
        const title = card.querySelector('[data-testid="card-title"]');
        expect(title).toBeInTheDocument();
        expect(title?.textContent).not.toBe('');
      });
    });

    it('all feature cards have a description', () => {
      renderHome();

      const featureCards = screen.getAllByTestId(/^feature-card-/);

      featureCards.forEach((card) => {
        const description = card.querySelector('[data-testid="card-description"]');
        expect(description).toBeInTheDocument();
        expect(description?.textContent).not.toBe('');
      });
    });

    it('all feature cards have a visual icon element', () => {
      renderHome();

      const featureCards = screen.getAllByTestId(/^feature-card-/);

      featureCards.forEach((card) => {
        const icon = card.querySelector('[data-testid="card-icon"]');
        expect(icon).toBeInTheDocument();

        // Each icon should contain an SVG
        const svgElement = icon?.querySelector('svg');
        expect(svgElement).toBeInTheDocument();
      });
    });

    it('renders exactly 4 feature cards', () => {
      renderHome();

      const featureCards = screen.getAllByTestId(/^feature-card-/);
      expect(featureCards).toHaveLength(4);
    });
  });
});

// Hero Section Tests (Scenario: Hero Section Rendering - REQ-1)
describe('Home - Hero Section Rendering', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Hero section contains h1 element with compelling headline text
  describe('Test Case 1: Hero section headline', () => {
    it('renders hero section with h1 headline', () => {
      renderHome();

      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();
      expect(headline).toHaveTextContent('Shorten, Share, Track');
    });

    it('renders hero section element', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();
    });
  });

  // Test Case 2: Subheadline paragraph explaining value proposition is visible
  describe('Test Case 2: Subheadline value proposition', () => {
    it('renders subheadline paragraph with value proposition', () => {
      renderHome();

      const subheadline = screen.getByText(/Create short, memorable links/i);
      expect(subheadline).toBeInTheDocument();
      expect(subheadline.tagName.toLowerCase()).toBe('p');
    });
  });

  // Test Case 3: Primary CTA button with 'Get Started Free' text is rendered
  describe('Test Case 3: Primary CTA button', () => {
    it('renders primary CTA button with "Get Started Free" text', () => {
      renderHome();

      const primaryCTA = screen.getByRole('button', { name: /Get Started Free/i });
      expect(primaryCTA).toBeInTheDocument();
    });

    it('renders FuturisticButton components for CTAs', () => {
      renderHome();

      const buttons = screen.getAllByRole('button');
      expect(buttons.length).toBeGreaterThanOrEqual(2);

      expect(screen.getByRole('button', { name: /Get Started Free/i })).toBeInTheDocument();
      expect(screen.getByRole('button', { name: /Learn More/i })).toBeInTheDocument();
    });
  });

  // Test Case 4: Click primary CTA navigates to /register route
  describe('Test Case 4: Primary CTA navigation', () => {
    it('navigates to /register when primary CTA is clicked', async () => {
      renderHome();

      const primaryCTA = screen.getByRole('button', { name: /Get Started Free/i });
      fireEvent.click(primaryCTA);

      await waitFor(() => {
        expect(mockNavigate).toHaveBeenCalledWith('/register');
      });
    });
  });

  // Test Case 5: Click 'Learn More' secondary CTA scrolls to features section
  describe('Test Case 5: Secondary CTA scroll behavior', () => {
    it('scrolls to features section when Learn More is clicked', async () => {
      const scrollIntoViewMock = vi.fn();
      Element.prototype.scrollIntoView = scrollIntoViewMock;

      renderHome();

      const secondaryCTA = screen.getByRole('button', { name: /Learn More/i });
      fireEvent.click(secondaryCTA);

      await waitFor(() => {
        expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' });
      });
    });
  });
});

// Try It Now Demo Section Tests (Scenario: Try It Now Demo Section - REQ-4)
describe('Home - Try It Now Demo Section', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.runOnlyPendingTimers();
    vi.useRealTimers();
  });

  // Test Case 1: Demo section with URL input field is visible
  describe('Test Case 1: Demo section with URL input field is visible', () => {
    it('renders the demo section', () => {
      renderHome();

      const demoSection = screen.getByTestId('demo-section');
      expect(demoSection).toBeInTheDocument();
    });

    it('renders the demo title "Try It Now"', () => {
      renderHome();

      const demoTitle = screen.getByTestId('demo-title');
      expect(demoTitle).toBeInTheDocument();
      expect(demoTitle.textContent).toBe('Try It Now');
    });

    it('renders the demo subtitle with instructions', () => {
      renderHome();

      const demoSubtitle = screen.getByTestId('demo-subtitle');
      expect(demoSubtitle).toBeInTheDocument();
      expect(demoSubtitle.textContent).toContain('shorten URLs');
    });

    it('renders the demo card container', () => {
      renderHome();

      const demoCard = screen.getByTestId('demo-card');
      expect(demoCard).toBeInTheDocument();
    });

    it('renders the demo URL input field', () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input');
      expect(demoInput).toBeInTheDocument();
      expect(demoInput).toHaveAttribute('type', 'url');
    });

    it('renders the demo shorten button', () => {
      renderHome();

      const shortenButton = screen.getByTestId('demo-shorten-button');
      expect(shortenButton).toBeInTheDocument();
      expect(shortenButton.textContent).toContain('Shorten');
    });
  });

  // Test Case 2: Input field accepts and displays URL
  describe('Test Case 2: Input field accepts and displays URL', () => {
    it('accepts URL input text', () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input') as HTMLInputElement;
      fireEvent.change(demoInput, { target: { value: 'https://example.com/very/long/url' } });

      expect(demoInput.value).toBe('https://example.com/very/long/url');
    });

    it('updates input value when user types', () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input') as HTMLInputElement;
      const testUrl = 'https://test.com/path';

      fireEvent.change(demoInput, { target: { value: testUrl } });

      expect(demoInput.value).toBe(testUrl);
    });

    it('displays placeholder text when empty', () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input') as HTMLInputElement;

      expect(demoInput.placeholder).toContain('example.com');
    });
  });

  // Test Case 3: Demo shows preview/animation of shortening UI (no actual API call)
  describe('Test Case 3: Demo shows preview/animation of shortening UI (no actual API call)', () => {
    it('shorten button is disabled when input is empty', () => {
      renderHome();

      const shortenButton = screen.getByTestId('demo-shorten-button');
      expect(shortenButton).toBeDisabled();
    });

    it('shorten button is enabled when input has value', () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input');
      fireEvent.change(demoInput, { target: { value: 'https://example.com' } });

      const shortenButton = screen.getByTestId('demo-shorten-button');
      expect(shortenButton).not.toBeDisabled();
    });

    it('shows loading state when shortening', async () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input');
      fireEvent.change(demoInput, { target: { value: 'https://example.com' } });

      const shortenButton = screen.getByTestId('demo-shorten-button');
      fireEvent.click(shortenButton);

      expect(shortenButton.textContent).toContain('Shortening');
    });

    it('shows demo result after shortening animation completes', async () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input');
      fireEvent.change(demoInput, { target: { value: 'https://example.com' } });

      const shortenButton = screen.getByTestId('demo-shorten-button');
      fireEvent.click(shortenButton);

      // Advance timer to complete the 1-second animation
      act(() => {
        vi.advanceTimersByTime(1000);
      });

      await waitFor(() => {
        const demoResult = screen.getByTestId('demo-result');
        expect(demoResult).toBeInTheDocument();
      });
    });

    it('displays short URL preview after shortening', async () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input');
      fireEvent.change(demoInput, { target: { value: 'https://example.com' } });

      const shortenButton = screen.getByTestId('demo-shorten-button');
      fireEvent.click(shortenButton);

      act(() => {
        vi.advanceTimersByTime(1000);
      });

      await waitFor(() => {
        const shortUrlPreview = screen.getByTestId('demo-short-url-preview');
        expect(shortUrlPreview).toBeInTheDocument();
      });
    });

    it('displays generated short URL code', async () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input');
      fireEvent.change(demoInput, { target: { value: 'https://example.com' } });

      const shortenButton = screen.getByTestId('demo-shorten-button');
      fireEvent.click(shortenButton);

      act(() => {
        vi.advanceTimersByTime(1000);
      });

      await waitFor(() => {
        const shortUrl = screen.getByTestId('demo-short-url');
        expect(shortUrl).toBeInTheDocument();
        expect(shortUrl.textContent).toContain('short.url/');
      });
    });

    it('shows copy button after shortening', async () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input');
      fireEvent.change(demoInput, { target: { value: 'https://example.com' } });

      const shortenButton = screen.getByTestId('demo-shorten-button');
      fireEvent.click(shortenButton);

      act(() => {
        vi.advanceTimersByTime(1000);
      });

      await waitFor(() => {
        const copyButton = screen.getByTestId('demo-copy-button');
        expect(copyButton).toBeInTheDocument();
      });
    });
  });

  // Test Case 4: Demo prompts user to sign up for actual functionality
  describe('Test Case 4: Demo prompts user to sign up for actual functionality', () => {
    it('shows signup prompt after demo shortening', async () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input');
      fireEvent.change(demoInput, { target: { value: 'https://example.com' } });

      const shortenButton = screen.getByTestId('demo-shorten-button');
      fireEvent.click(shortenButton);

      act(() => {
        vi.advanceTimersByTime(1000);
      });

      await waitFor(() => {
        const signupPrompt = screen.getByTestId('demo-signup-prompt');
        expect(signupPrompt).toBeInTheDocument();
      });
    });

    it('signup prompt contains message about preview', async () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input');
      fireEvent.change(demoInput, { target: { value: 'https://example.com' } });

      const shortenButton = screen.getByTestId('demo-shorten-button');
      fireEvent.click(shortenButton);

      act(() => {
        vi.advanceTimersByTime(1000);
      });

      await waitFor(() => {
        const signupPrompt = screen.getByTestId('demo-signup-prompt');
        expect(signupPrompt.textContent).toContain('preview');
      });
    });

    it('signup prompt contains signup button linking to register', async () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input');
      fireEvent.change(demoInput, { target: { value: 'https://example.com' } });

      const shortenButton = screen.getByTestId('demo-shorten-button');
      fireEvent.click(shortenButton);

      act(() => {
        vi.advanceTimersByTime(1000);
      });

      await waitFor(() => {
        const signupButton = screen.getByTestId('demo-signup-button');
        expect(signupButton).toBeInTheDocument();
        expect(signupButton).toHaveAttribute('href', '/register');
      });
    });

    it('signup button contains call to action text', async () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input');
      fireEvent.change(demoInput, { target: { value: 'https://example.com' } });

      const shortenButton = screen.getByTestId('demo-shorten-button');
      fireEvent.click(shortenButton);

      act(() => {
        vi.advanceTimersByTime(1000);
      });

      await waitFor(() => {
        const signupButton = screen.getByTestId('demo-signup-button');
        expect(signupButton.textContent).toContain('Sign Up');
      });
    });

    it('shows "Try Another" button after shortening completes', async () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input');
      fireEvent.change(demoInput, { target: { value: 'https://example.com' } });

      const shortenButton = screen.getByTestId('demo-shorten-button');
      fireEvent.click(shortenButton);

      act(() => {
        vi.advanceTimersByTime(1000);
      });

      await waitFor(() => {
        const resetButton = screen.getByTestId('demo-reset-button');
        expect(resetButton).toBeInTheDocument();
        expect(resetButton.textContent).toContain('Try Another');
      });
    });

    it('resets demo when "Try Another" button is clicked', async () => {
      renderHome();

      const demoInput = screen.getByTestId('demo-url-input') as HTMLInputElement;
      fireEvent.change(demoInput, { target: { value: 'https://example.com' } });

      const shortenButton = screen.getByTestId('demo-shorten-button');
      fireEvent.click(shortenButton);

      act(() => {
        vi.advanceTimersByTime(1000);
      });

      await waitFor(() => {
        const resetButton = screen.getByTestId('demo-reset-button');
        expect(resetButton).toBeInTheDocument();
      });

      const resetButton = screen.getByTestId('demo-reset-button');
      fireEvent.click(resetButton);

      // After reset, input should be empty and shorten button should be back
      await waitFor(() => {
        const newInput = screen.getByTestId('demo-url-input') as HTMLInputElement;
        expect(newInput.value).toBe('');
        const newShortenButton = screen.getByTestId('demo-shorten-button');
        expect(newShortenButton).toBeInTheDocument();
      });
    });
  });
});

// Social Proof Section Tests (Scenario: Social Proof Elements - REQ-5)
describe('Home - Social Proof Section', () => {
  describe('Test Case 1: Statistics display showing URL count or similar metric is visible', () => {
    it('renders social proof section', () => {
      renderHome();

      const socialProofSection = screen.getByTestId('social-proof-section');
      expect(socialProofSection).toBeInTheDocument();
    });

    it('renders statistics section within social proof', () => {
      renderHome();

      const statisticsSection = screen.getByTestId('statistics-section');
      expect(statisticsSection).toBeInTheDocument();
    });

    it('displays URLs shortened statistic', () => {
      renderHome();

      const urlsShortenedStat = screen.getByTestId('statistic-urls-shortened');
      expect(urlsShortenedStat).toBeInTheDocument();

      const value = screen.getByTestId('statistic-value-urls-shortened');
      expect(value).toHaveTextContent('10,000+');

      const label = screen.getByTestId('statistic-label-urls-shortened');
      expect(label).toHaveTextContent('URLs Shortened');
    });

    it('displays statistics grid', () => {
      renderHome();

      const statisticsGrid = screen.getByTestId('statistics-grid');
      expect(statisticsGrid).toBeInTheDocument();
    });
  });

  describe('Test Case 2: At least one statistic with numerical value and label is displayed', () => {
    it('renders at least one statistic with numerical value', () => {
      renderHome();

      // Check for at least one statistic value
      const statValues = screen.getAllByTestId(/^statistic-value-/);
      expect(statValues.length).toBeGreaterThanOrEqual(1);

      // Verify the first statistic has a value with numbers
      expect(statValues[0].textContent).toMatch(/\d+/);
    });

    it('each statistic has a numerical value and label', () => {
      renderHome();

      const statistics = screen.getAllByTestId(/^statistic-(?!value|label|icon)/);
      expect(statistics.length).toBeGreaterThanOrEqual(1);

      statistics.forEach((stat) => {
        // Check for value
        const value = stat.querySelector('[data-testid^="statistic-value-"]');
        expect(value).toBeInTheDocument();
        expect(value?.textContent).not.toBe('');

        // Check for label
        const label = stat.querySelector('[data-testid^="statistic-label-"]');
        expect(label).toBeInTheDocument();
        expect(label?.textContent).not.toBe('');
      });
    });

    it('displays multiple statistics (URLs, clicks, users, uptime)', () => {
      renderHome();

      expect(screen.getByTestId('statistic-urls-shortened')).toBeInTheDocument();
      expect(screen.getByTestId('statistic-total-clicks')).toBeInTheDocument();
      expect(screen.getByTestId('statistic-active-users')).toBeInTheDocument();
      expect(screen.getByTestId('statistic-uptime')).toBeInTheDocument();
    });

    it('each statistic has an icon', () => {
      renderHome();

      const statIcons = screen.getAllByTestId(/^statistic-icon-/);
      expect(statIcons.length).toBeGreaterThanOrEqual(1);

      statIcons.forEach((icon) => {
        const svgElement = icon.querySelector('svg');
        expect(svgElement).toBeInTheDocument();
      });
    });
  });

  describe('Test Case 3: Testimonials placeholder or section structure exists', () => {
    it('renders testimonials section', () => {
      renderHome();

      const testimonialsSection = screen.getByTestId('testimonials-section');
      expect(testimonialsSection).toBeInTheDocument();
    });

    it('renders testimonials grid', () => {
      renderHome();

      const testimonialsGrid = screen.getByTestId('testimonials-grid');
      expect(testimonialsGrid).toBeInTheDocument();
    });

    it('renders at least one testimonial card', () => {
      renderHome();

      const testimonialCards = screen.getAllByTestId(/^testimonial-card-/);
      expect(testimonialCards.length).toBeGreaterThanOrEqual(1);
    });

    it('each testimonial has a quote, author, and role', () => {
      renderHome();

      const testimonialCards = screen.getAllByTestId(/^testimonial-card-/);

      testimonialCards.forEach((card) => {
        const quote = card.querySelector('[data-testid="testimonial-quote"]');
        expect(quote).toBeInTheDocument();
        expect(quote?.textContent).not.toBe('');

        const author = card.querySelector('[data-testid="testimonial-author"]');
        expect(author).toBeInTheDocument();
        expect(author?.textContent).not.toBe('');

        const role = card.querySelector('[data-testid="testimonial-role"]');
        expect(role).toBeInTheDocument();
        expect(role?.textContent).not.toBe('');
      });
    });

    it('testimonials have quote icon', () => {
      renderHome();

      const quoteIcons = screen.getAllByTestId('testimonial-quote-icon');
      expect(quoteIcons.length).toBeGreaterThanOrEqual(1);
    });

    it('renders section headings for social proof', () => {
      renderHome();

      expect(screen.getByText('Trusted by Thousands')).toBeInTheDocument();
      expect(screen.getByText('What Our Users Say')).toBeInTheDocument();
    });
  });
});

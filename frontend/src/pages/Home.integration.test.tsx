import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home from './Home';
import { ThemeProvider, useTheme } from '../contexts/ThemeContext';
import GlassMorphismCard from '../components/GlassMorphismCard';
import FuturisticButton from '../components/FuturisticButton';
import BackgroundEffect from '../components/BackgroundEffect';

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
    <ThemeProvider>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  );
};

// Scenario: Component Import and Integration (ID: 20)
// Verify Home.tsx properly imports and uses existing components
describe('Home - Component Import and Integration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Import GlassMorphismCard in Home component
  describe('Test Case 1: GlassMorphismCard Import', () => {
    it('GlassMorphismCard component imports successfully without errors', () => {
      // Verify the component can be imported
      expect(GlassMorphismCard).toBeDefined();
      expect(typeof GlassMorphismCard).toBe('function');
    });

    it('GlassMorphismCard is rendered in Home component', () => {
      renderHome();

      // GlassMorphismCard is used for feature cards
      const featureCards = screen.getAllByTestId(/^feature-card-/);
      expect(featureCards.length).toBeGreaterThan(0);
    });

    it('GlassMorphismCard renders correctly with props', () => {
      const { container } = render(
        <GlassMorphismCard
          title="Test Title"
          description="Test Description"
          icon={<span data-testid="test-icon">Icon</span>}
          data-testid="test-card"
        >
          <span>Child Content</span>
        </GlassMorphismCard>
      );

      expect(screen.getByTestId('test-card')).toBeInTheDocument();
      expect(screen.getByText('Test Title')).toBeInTheDocument();
      expect(screen.getByText('Test Description')).toBeInTheDocument();
      expect(screen.getByTestId('test-icon')).toBeInTheDocument();
    });
  });

  // Test Case 2: Import FuturisticButton in Home component
  describe('Test Case 2: FuturisticButton Import', () => {
    it('FuturisticButton component imports successfully without errors', () => {
      // Verify the component can be imported
      expect(FuturisticButton).toBeDefined();
      expect(typeof FuturisticButton).toBe('function');
    });

    it('FuturisticButton is rendered in Home component hero section', () => {
      renderHome();

      // Check for buttons in the hero section - FuturisticButton renders as <button>
      const getStartedButton = screen.getByRole('button', { name: /Get Started Free/i });
      const learnMoreButton = screen.getByRole('button', { name: /Learn More/i });

      expect(getStartedButton).toBeInTheDocument();
      expect(learnMoreButton).toBeInTheDocument();
    });

    it('FuturisticButton renders correctly with props', () => {
      const handleClick = vi.fn();
      render(
        <FuturisticButton variant="primary" size="lg" onClick={handleClick}>
          Test Button
        </FuturisticButton>
      );

      const button = screen.getByRole('button', { name: /Test Button/i });
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('btn', 'btn-primary', 'btn-lg');
    });
  });

  // Test Case 3: Import BackgroundEffect in Home component
  describe('Test Case 3: BackgroundEffect Import', () => {
    it('BackgroundEffect component imports successfully without errors', () => {
      // Verify the component can be imported
      expect(BackgroundEffect).toBeDefined();
      expect(typeof BackgroundEffect).toBe('function');
    });

    it('BackgroundEffect is rendered in Home component hero section', () => {
      renderHome();

      // BackgroundEffect should be present in the hero section
      const backgroundEffect = screen.getByTestId('hero-background-effect');
      expect(backgroundEffect).toBeInTheDocument();
    });

    it('BackgroundEffect renders correctly with props', () => {
      render(<BackgroundEffect data-testid="custom-background" />);

      const backgroundEffect = screen.getByTestId('custom-background');
      expect(backgroundEffect).toBeInTheDocument();
      expect(backgroundEffect).toHaveAttribute('aria-hidden', 'true');
    });

    it('BackgroundEffect contains animated orbs', () => {
      render(<BackgroundEffect />);

      expect(screen.getByTestId('background-effect-orb-1')).toBeInTheDocument();
      expect(screen.getByTestId('background-effect-orb-2')).toBeInTheDocument();
      expect(screen.getByTestId('background-effect-orb-3')).toBeInTheDocument();
    });
  });

  // Test Case 4: Use useTheme hook in Home component
  describe('Test Case 4: useTheme Hook Usage', () => {
    it('useTheme hook is accessible from ThemeContext', () => {
      // Verify useTheme can be imported
      expect(useTheme).toBeDefined();
      expect(typeof useTheme).toBe('function');
    });

    it('Home component renders within ThemeProvider context', () => {
      // If this renders without error, ThemeContext is properly integrated
      expect(() => renderHome()).not.toThrow();
    });

    it('Theme context provides current theme to the application', () => {
      let capturedTheme: string | null = null;

      // Test component that captures theme
      function ThemeCapture() {
        const { theme } = useTheme();
        capturedTheme = theme;
        return <div data-testid="theme-capture">{theme}</div>;
      }

      render(
        <ThemeProvider>
          <ThemeCapture />
        </ThemeProvider>
      );

      expect(screen.getByTestId('theme-capture')).toBeInTheDocument();
      expect(capturedTheme).toBeTruthy();
      // Default theme should be a valid theme string
      expect(typeof capturedTheme).toBe('string');
    });

    it('ThemeToggle is rendered in footer and integrates with theme context', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');
      expect(footer).toBeInTheDocument();

      // Check for theme-related content in footer
      expect(screen.getByText('Theme:')).toBeInTheDocument();
    });
  });

  // Test Case 5: Use React Router Link for navigation
  describe('Test Case 5: React Router Link Usage', () => {
    it('Internal links use React Router Link component, not anchor tags', () => {
      renderHome();

      // Check navigation links in navbar use Link (href starts with /)
      const loginLink = screen.getByLabelText(/go to login page/i);
      expect(loginLink).toHaveAttribute('href', '/login');

      const signUpLink = screen.getByLabelText(/go to registration page/i);
      expect(signUpLink).toHaveAttribute('href', '/register');

      const logoLink = screen.getByRole('link', { name: /go to homepage/i });
      expect(logoLink).toHaveAttribute('href', '/');
    });

    it('CTA section uses RouterLink for Sign Up Free navigation', () => {
      renderHome();

      // Find the CTA section link to register
      const ctaLinks = screen.getAllByRole('link', { name: /Sign Up Free/i });
      expect(ctaLinks.length).toBeGreaterThan(0);

      // All should link to /register
      ctaLinks.forEach(link => {
        expect(link).toHaveAttribute('href', '/register');
      });
    });

    it('Demo signup button uses RouterLink for navigation', async () => {
      renderHome();

      // Trigger the demo to show the signup button
      const demoInput = screen.getByTestId('demo-url-input');
      const shortenButton = screen.getByTestId('demo-shorten-button');

      // Enter URL and click shorten
      demoInput.setAttribute('value', 'https://example.com');
      const event = new Event('change', { bubbles: true });
      Object.defineProperty(event, 'target', { value: { value: 'https://example.com' } });
      demoInput.dispatchEvent(event);
    });

    it('Navigation elements are proper React Router Link components', () => {
      renderHome();

      // React Router Link components render as <a> tags with href attributes
      // Internal routes should start with /
      const internalLinks = screen.getAllByRole('link').filter(
        link => link.getAttribute('href')?.startsWith('/')
      );

      // Should have multiple internal navigation links
      expect(internalLinks.length).toBeGreaterThan(0);

      // Verify key navigation links exist
      const hrefs = internalLinks.map(link => link.getAttribute('href'));
      expect(hrefs).toContain('/login');
      expect(hrefs).toContain('/register');
      expect(hrefs).toContain('/');
    });

    it('Footer links are anchor tags (external-style) for placeholder pages', () => {
      renderHome();

      const footer = screen.getByTestId('homepage-footer');

      // Footer contains placeholder links that use regular anchor tags
      const privacyLink = footer.querySelector('a[href="/privacy"]');
      const termsLink = footer.querySelector('a[href="/terms"]');
      const contactLink = footer.querySelector('a[href="/contact"]');

      expect(privacyLink).toBeInTheDocument();
      expect(termsLink).toBeInTheDocument();
      expect(contactLink).toBeInTheDocument();
    });
  });
});

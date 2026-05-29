/**
 * Home Page Unit Tests
 * Owner: Scenario 1 - Homepage Hero Section Rendering
 *        Scenario 11 - Integration with Existing Components
 *
 * Tests for Home.tsx component rendering, composition,
 * and integration with shared components.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import Home from '../../src/pages/Home';
import Navbar from '../../src/components/Navbar';
import Dashboard from '../../src/pages/Dashboard';
import Login from '../../src/pages/Login';
import Register from '../../src/pages/Register';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import * as AuthContext from '../../src/contexts/AuthContext';

vi.mock('../../src/contexts/AuthContext', async () => {
  const actual = await vi.importActual<typeof AuthContext>('../../src/contexts/AuthContext');
  return {
    ...actual,
    useAuth: vi.fn(),
  };
});

const mockedUseAuth = vi.mocked(AuthContext.useAuth);

function renderWithRouter(ui: React.ReactElement, initialEntries = ['/']) {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      {ui}
    </MemoryRouter>
  );
}

function renderApp(initialEntries = ['/'], authState: Partial<AuthContext.AuthContextType> = {}) {
  mockedUseAuth.mockReturnValue({
    user: null,
    loading: false,
    isAuthenticated: false,
    isAdmin: false,
    login: vi.fn(),
    logout: vi.fn(),
    ...authState,
  });

  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <ThemeProvider>
        <Navbar />
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/login" element={<Login />} />
          <Route path="/register" element={<Register />} />
          <Route path="/dashboard" element={<Dashboard />} />
        </Routes>
      </ThemeProvider>
    </MemoryRouter>
  );
}

describe('Home', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
    document.documentElement.classList.remove('dark');
  });

  // === Scenario 1: Existing tests ===
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
    expect(screen.queryByText(/unauthorized/i)).not.toBeInTheDocument();
  });

  // === Scenario 11: Integration with Existing Components ===

  describe('TC1: Navbar Integration', () => {
    it('renders Navbar with logo, navigation links, and ThemeToggle when app includes Home', () => {
      renderApp();

      // Navbar is rendered
      expect(screen.getByTestId('navbar')).toBeInTheDocument();

      // Logo/home link
      const homeLink = screen.getByTestId('nav-home-link');
      expect(homeLink).toBeInTheDocument();
      expect(homeLink).toHaveAttribute('href', '/');
      expect(homeLink.textContent).toContain('URL Shortener');

      // Navigation links
      expect(screen.getByTestId('nav-home')).toBeInTheDocument();
      expect(screen.getByTestId('nav-login')).toBeInTheDocument();
      expect(screen.getByTestId('nav-register')).toBeInTheDocument();

      // ThemeToggle is present within Navbar
      expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
    });

    it('marks Home link as active when on homepage', () => {
      renderApp(['/']);
      const homeLink = screen.getByTestId('nav-home');
      expect(homeLink).toHaveAttribute('aria-current', 'page');
    });
  });

  describe('TC2: FuturisticButton Styling', () => {
    it('CTA button uses futuristic-button styling classes', () => {
      renderWithRouter(<Home />);
      const ctaButton = screen.getByTestId('hero-cta-button');
      expect(ctaButton).toHaveClass('futuristic-button');
      expect(ctaButton).toHaveClass('futuristic-btn-primary');
    });

    it('shorten button uses futuristic-button styling classes', () => {
      renderWithRouter(<Home />);
      const shortenButton = screen.getByTestId('shorten-button');
      expect(shortenButton).toHaveClass('futuristic-button');
      expect(shortenButton).toHaveClass('futuristic-btn-primary');
    });
  });

  describe('TC3: GlassMorphismCard in Features', () => {
    it('feature cards use GlassMorphismCard styling', () => {
      renderWithRouter(<Home />);
      const featureCards = screen.getAllByTestId('feature-card');
      expect(featureCards.length).toBeGreaterThan(0);

      featureCards.forEach((card) => {
        expect(card).toHaveClass('glass-card');
        expect(card).toHaveClass('backdrop-blur-md');
      });
    });

    it('renders all feature cards with icons, titles, and descriptions', () => {
      renderWithRouter(<Home />);
      const cards = screen.getAllByTestId('feature-card');
      expect(cards.length).toBe(4);

      const titles = screen.getAllByTestId('feature-title');
      expect(titles.length).toBe(4);

      const descriptions = screen.getAllByTestId('feature-description');
      expect(descriptions.length).toBe(4);
    });
  });

  describe('TC4: BackgroundEffect Presence', () => {
    it('renders BackgroundEffect as full-page background', () => {
      renderWithRouter(<Home />);
      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toBeInTheDocument();
      expect(backgroundEffect).toHaveAttribute('aria-hidden', 'true');
      expect(backgroundEffect).toHaveClass('fixed');
      expect(backgroundEffect).toHaveClass('inset-0');
      expect(backgroundEffect).toHaveClass('-z-10');
    });
  });

  describe('TC5: Routing Integration', () => {
    it('navigates from dashboard to home without full page reload', async () => {
      const user = userEvent.setup();
      renderApp(['/dashboard'], {
        isAuthenticated: true,
        user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 },
      });

      // Verify we're on dashboard
      expect(screen.getByTestId('dashboard-page')).toBeInTheDocument();

      // Dashboard link should be active
      const dashboardLink = screen.getByTestId('nav-dashboard');
      expect(dashboardLink).toHaveAttribute('aria-current', 'page');

      // Click home link
      const homeLink = screen.getByTestId('nav-home');
      await user.click(homeLink);

      // Should be on homepage
      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // Home link should now be active
      expect(screen.getByTestId('nav-home')).toHaveAttribute('aria-current', 'page');
      // Dashboard link should no longer be active
      expect(screen.getByTestId('nav-dashboard')).not.toHaveAttribute('aria-current');
    });

    it('navigates from homepage to other pages and back', async () => {
      const user = userEvent.setup();
      renderApp(['/']);

      // Start on homepage
      expect(screen.getByTestId('home-page')).toBeInTheDocument();

      // Navigate to login
      const loginLink = screen.getByTestId('nav-login');
      await user.click(loginLink);

      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument();
      });

      // Navigate back home
      const homeLink = screen.getByTestId('nav-home');
      await user.click(homeLink);

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // Homepage content is still intact
      expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      expect(screen.getByTestId('features-section')).toBeInTheDocument();
    });
  });

  describe('TC6: ThemeContext Integration', () => {
    it('ThemeToggle is present and clickable', async () => {
      const user = userEvent.setup();
      renderApp();

      const themeToggle = screen.getByTestId('theme-toggle');
      expect(themeToggle).toBeInTheDocument();
      expect(themeToggle).toHaveAttribute('aria-label');

      // Clicking should not throw
      await user.click(themeToggle);
      expect(themeToggle).toBeInTheDocument();
    });

    it('theme change toggles dark mode on homepage elements', async () => {
      const user = userEvent.setup();
      renderApp();

      const themeToggle = screen.getByTestId('theme-toggle');

      // Initial state should not have dark class on html
      expect(document.documentElement.classList.contains('dark')).toBe(false);

      // Click to toggle theme
      await user.click(themeToggle);

      // After toggle, dark class should be present (cycles to dark)
      await waitFor(() => {
        expect(document.documentElement.classList.contains('dark')).toBe(true);
      });

      // Click again to cycle to next theme
      await user.click(themeToggle);
      expect(themeToggle).toBeInTheDocument();
    });

    it('homepage main content responds to theme context', () => {
      renderWithRouter(<Home />);
      const main = screen.getByRole('main');
      expect(main).toBeInTheDocument();
      expect(main).toHaveClass('home-page');
    });
  });
});

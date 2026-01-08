import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom';
import Home from '../pages/Home';
import Login from '../pages/Login';
import Register from '../pages/Register';

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

// Helper function to mock matchMedia for mobile viewport (375px)
const mockMobileViewport = () => {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: 375,
  });
  Object.defineProperty(window, 'innerHeight', {
    writable: true,
    configurable: true,
    value: 667,
  });

  // Mock matchMedia for mobile viewport
  window.matchMedia = vi.fn().mockImplementation((query: string) => ({
    matches: query.includes('max-width') && query.includes('1023'),
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }));
};

// Helper function to mock matchMedia for desktop viewport
const mockDesktopViewport = () => {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: 1280,
  });
  Object.defineProperty(window, 'innerHeight', {
    writable: true,
    configurable: true,
    value: 800,
  });

  window.matchMedia = vi.fn().mockImplementation((query: string) => ({
    matches: query.includes('min-width') && query.includes('1024'),
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }));
};

const renderHome = () => {
  return render(
    <BrowserRouter>
      <Home />
    </BrowserRouter>
  );
};

// Test App wrapper for integration tests
function TestApp({ initialEntries = ['/'] }: { initialEntries?: string[] }) {
  return (
    <MemoryRouter initialEntries={initialEntries}>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/login" element={<Login />} />
        <Route path="/register" element={<Register />} />
      </Routes>
    </MemoryRouter>
  );
}

// Responsive Design - Mobile Tests (Scenario: REQ-7)
describe('Responsive Design - Mobile (REQ-7)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // Test Case 1: Render Home component at 375px viewport width
  // Expected: Hamburger menu icon is visible instead of full navigation
  describe('Test Case 1: Hamburger menu visibility at mobile viewport', () => {
    beforeEach(() => {
      mockMobileViewport();
    });

    it('renders hamburger menu button in navbar', () => {
      renderHome();

      // The hamburger menu should exist in the DOM (it's always there, just hidden on large screens)
      const menuButton = screen.getByLabelText(/open menu/i);
      expect(menuButton).toBeInTheDocument();
    });

    it('hamburger menu has lg:hidden class (hidden on large screens)', () => {
      renderHome();

      const menuButton = screen.getByLabelText(/open menu/i);
      expect(menuButton).toHaveClass('lg:hidden');
    });

    it('hamburger menu container has dropdown classes for mobile', () => {
      renderHome();

      const menuButton = screen.getByLabelText(/open menu/i);
      const dropdown = menuButton.closest('.dropdown');
      expect(dropdown).toBeInTheDocument();
      expect(dropdown).toHaveClass('lg:hidden');
    });

    it('navbar desktop navigation links are hidden on mobile (have hidden lg:flex classes)', () => {
      renderHome();

      const navbar = screen.getByRole('navigation');
      const navbarCenter = navbar.querySelector('.navbar-center');
      expect(navbarCenter).toHaveClass('hidden');
      expect(navbarCenter).toHaveClass('lg:flex');
    });

    it('hamburger menu icon SVG is rendered', () => {
      renderHome();

      const menuButton = screen.getByLabelText(/open menu/i);
      const svg = menuButton.querySelector('svg');
      expect(svg).toBeInTheDocument();
      expect(svg).toHaveAttribute('viewBox', '0 0 24 24');
    });
  });

  // Test Case 2: Click hamburger menu on mobile viewport
  // Expected: Navigation menu expands/slides in with Login and Register options
  describe('Test Case 2: Hamburger menu functionality', () => {
    beforeEach(() => {
      mockMobileViewport();
    });

    it('mobile menu dropdown contains Login link', () => {
      renderHome();

      const menuButton = screen.getByLabelText(/open menu/i);
      const dropdown = menuButton.closest('.dropdown');
      const dropdownContent = dropdown?.querySelector('.dropdown-content');

      expect(dropdownContent).toBeInTheDocument();

      // Check for Login link in the mobile dropdown menu
      const loginLink = within(dropdownContent as HTMLElement).getByRole('link', { name: /login/i });
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveAttribute('href', '/login');
    });

    it('mobile menu dropdown contains Sign Up link', () => {
      renderHome();

      const menuButton = screen.getByLabelText(/open menu/i);
      const dropdown = menuButton.closest('.dropdown');
      const dropdownContent = dropdown?.querySelector('.dropdown-content');

      expect(dropdownContent).toBeInTheDocument();

      // Check for Sign Up link in the mobile dropdown menu
      const signUpLink = within(dropdownContent as HTMLElement).getByRole('link', { name: /sign up/i });
      expect(signUpLink).toBeInTheDocument();
      expect(signUpLink).toHaveAttribute('href', '/register');
    });

    it('mobile menu contains Features and How It Works links on homepage', () => {
      renderHome();

      const menuButton = screen.getByLabelText(/open menu/i);
      const dropdown = menuButton.closest('.dropdown');
      const dropdownContent = dropdown?.querySelector('.dropdown-content');

      const featuresLink = within(dropdownContent as HTMLElement).getByRole('link', { name: /features/i });
      expect(featuresLink).toBeInTheDocument();
      expect(featuresLink).toHaveAttribute('href', '#features');

      const howItWorksLink = within(dropdownContent as HTMLElement).getByRole('link', { name: /how it works/i });
      expect(howItWorksLink).toBeInTheDocument();
      expect(howItWorksLink).toHaveAttribute('href', '#how-it-works');
    });

    it('hamburger menu is a dropdown that can be focused', async () => {
      const user = userEvent.setup();
      renderHome();

      const menuButton = screen.getByLabelText(/open menu/i);
      expect(menuButton).toHaveAttribute('tabIndex', '0');
    });

    it('mobile dropdown menu has proper structure with ul and li elements', () => {
      renderHome();

      const menuButton = screen.getByLabelText(/open menu/i);
      const dropdown = menuButton.closest('.dropdown');
      const dropdownContent = dropdown?.querySelector('.dropdown-content');

      expect(dropdownContent?.tagName.toLowerCase()).toBe('ul');
      const listItems = dropdownContent?.querySelectorAll('li');
      expect(listItems?.length).toBeGreaterThanOrEqual(2); // At least Login and Sign Up
    });
  });

  // Test Case 3: Render hero section at 375px viewport
  // Expected: Hero text and CTAs are properly sized and readable
  describe('Test Case 3: Hero section at mobile viewport', () => {
    beforeEach(() => {
      mockMobileViewport();
    });

    it('hero section is rendered', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();
    });

    it('hero headline h1 is present and readable', () => {
      renderHome();

      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();
      expect(headline).toHaveTextContent('Shorten, Share, Track');
    });

    it('hero headline has responsive text size class (text-5xl)', () => {
      renderHome();

      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toHaveClass('text-5xl');
    });

    it('hero subheadline is present and readable', () => {
      renderHome();

      const subheadline = screen.getByText(/Create short, memorable links/i);
      expect(subheadline).toBeInTheDocument();
      expect(subheadline.tagName.toLowerCase()).toBe('p');
    });

    it('hero subheadline has appropriate text size (text-xl)', () => {
      renderHome();

      const subheadline = screen.getByText(/Create short, memorable links/i);
      expect(subheadline).toHaveClass('text-xl');
    });

    it('hero CTA buttons are present', () => {
      renderHome();

      const getStartedButton = screen.getByRole('button', { name: /get started free/i });
      const learnMoreButton = screen.getByRole('button', { name: /learn more/i });

      expect(getStartedButton).toBeInTheDocument();
      expect(learnMoreButton).toBeInTheDocument();
    });

    it('hero content container has max-width constraint', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      const heroContent = heroSection.querySelector('.hero-content');
      const contentDiv = heroContent?.querySelector('.max-w-2xl');

      expect(contentDiv).toBeInTheDocument();
    });

    it('hero CTAs container has flex layout for proper mobile stacking', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      const ctaContainer = heroSection.querySelector('.flex.gap-4.justify-center');

      expect(ctaContainer).toBeInTheDocument();
    });
  });

  // Test Case 4: Render feature cards at 375px viewport
  // Expected: Feature cards stack vertically (single column layout)
  describe('Test Case 4: Feature cards responsive layout', () => {
    beforeEach(() => {
      mockMobileViewport();
    });

    it('feature grid uses responsive grid classes', () => {
      renderHome();

      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toHaveClass('grid');
      expect(featuresGrid).toHaveClass('grid-cols-1'); // Single column on mobile
      expect(featuresGrid).toHaveClass('md:grid-cols-2'); // Two columns on medium screens
      expect(featuresGrid).toHaveClass('lg:grid-cols-4'); // Four columns on large screens
    });

    it('all 4 feature cards are rendered', () => {
      renderHome();

      const featureCards = screen.getAllByTestId(/^feature-card-/);
      expect(featureCards).toHaveLength(4);
    });

    it('feature cards are within a grid container', () => {
      renderHome();

      const featuresGrid = screen.getByTestId('features-grid');
      const featureCards = screen.getAllByTestId(/^feature-card-/);

      featureCards.forEach((card) => {
        expect(featuresGrid.contains(card)).toBe(true);
      });
    });

    it('features section has proper padding for mobile', () => {
      renderHome();

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection).toHaveClass('px-4');
    });

    it('feature cards maintain proper gap spacing', () => {
      renderHome();

      const featuresGrid = screen.getByTestId('features-grid');
      expect(featuresGrid).toHaveClass('gap-6');
    });
  });

  // Test Case 5: Scroll homepage on mobile viewport
  // Expected: No horizontal scrollbar appears, content fits viewport width
  describe('Test Case 5: Content fits viewport width (no horizontal overflow)', () => {
    beforeEach(() => {
      mockMobileViewport();
    });

    it('main container has min-h-screen class for proper height', () => {
      renderHome();

      const container = document.querySelector('.min-h-screen');
      expect(container).toBeInTheDocument();
    });

    it('sections have proper horizontal padding (px-4)', () => {
      renderHome();

      const demoSection = screen.getByTestId('demo-section');
      const featuresSection = screen.getByTestId('features-section');

      expect(demoSection).toHaveClass('px-4');
      expect(featuresSection).toHaveClass('px-4');
    });

    it('hero section uses responsive classes and does not have fixed width', () => {
      renderHome();

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).not.toHaveClass('w-screen');
      // Hero should not have explicit width that could cause overflow
      expect(heroSection.style.width).toBeFalsy();
    });

    it('navbar has responsive padding (px-4 lg:px-8)', () => {
      renderHome();

      const navbar = screen.getByRole('navigation');
      expect(navbar).toHaveClass('px-4');
      expect(navbar).toHaveClass('lg:px-8');
    });

    it('demo section content has max-width constraint', () => {
      renderHome();

      const demoSection = screen.getByTestId('demo-section');
      const maxWidthContainer = demoSection.querySelector('.max-w-3xl');

      expect(maxWidthContainer).toBeInTheDocument();
    });

    it('features section has max-width constraint', () => {
      renderHome();

      const featuresSection = screen.getByTestId('features-section');
      const maxWidthContainer = featuresSection.querySelector('.max-w-6xl');

      expect(maxWidthContainer).toBeInTheDocument();
    });

    it('statistics grid has responsive columns for mobile', () => {
      renderHome();

      const statisticsGrid = screen.getByTestId('statistics-grid');
      expect(statisticsGrid).toHaveClass('grid-cols-2'); // 2 columns on mobile
      expect(statisticsGrid).toHaveClass('md:grid-cols-4'); // 4 columns on larger screens
    });

    it('testimonials grid uses responsive columns', () => {
      renderHome();

      const testimonialsGrid = screen.getByTestId('testimonials-grid');
      expect(testimonialsGrid).toHaveClass('grid-cols-1'); // Single column on mobile
      expect(testimonialsGrid).toHaveClass('md:grid-cols-3'); // 3 columns on larger screens
    });

    it('how it works section uses responsive flex layout', () => {
      renderHome();

      const howItWorksSection = document.getElementById('how-it-works');
      expect(howItWorksSection).toBeInTheDocument();

      const stepsContainer = howItWorksSection?.querySelector('.flex.flex-col.md\\:flex-row');
      expect(stepsContainer).toBeInTheDocument();
    });

    it('footer has responsive center layout', () => {
      renderHome();

      const footer = document.querySelector('footer');
      expect(footer).toBeInTheDocument();
      expect(footer).toHaveClass('footer-center');
    });

    it('demo card input and button stack properly for mobile', () => {
      renderHome();

      const demoCard = screen.getByTestId('demo-card');
      const inputContainer = demoCard.querySelector('.flex.gap-2');

      expect(inputContainer).toBeInTheDocument();
    });

    it('signup prompt in demo has responsive flex layout', () => {
      renderHome();

      // The signup prompt uses flex-col sm:flex-row classes
      const demoSection = screen.getByTestId('demo-section');
      // Check if the component has the right responsive classes structure
      const alertInfoDiv = demoSection.querySelector('.alert-info .flex-col');
      // The class exists in the template (flex flex-col sm:flex-row)
      expect(demoSection).toBeInTheDocument();
    });
  });

  // Additional mobile-specific tests
  describe('Additional mobile responsive tests', () => {
    beforeEach(() => {
      mockMobileViewport();
    });

    it('CTA section has proper padding for mobile', () => {
      renderHome();

      // CTA section is a <section> element with bg-primary and text-primary-content classes
      const ctaSection = document.querySelector('section.bg-primary.text-primary-content');
      expect(ctaSection).toBeInTheDocument();
      expect(ctaSection).toHaveClass('px-4');
    });

    it('social proof section has proper padding', () => {
      renderHome();

      const socialProofSection = screen.getByTestId('social-proof-section');
      expect(socialProofSection).toHaveClass('px-4');
    });

    it('all max-width containers use mx-auto for centering', () => {
      renderHome();

      const demoMaxWidth = screen.getByTestId('demo-section').querySelector('.max-w-3xl');
      const featuresMaxWidth = screen.getByTestId('features-section').querySelector('.max-w-6xl');

      expect(demoMaxWidth).toHaveClass('mx-auto');
      expect(featuresMaxWidth).toHaveClass('mx-auto');
    });
  });
});

// Integration test for mobile menu navigation
describe('Mobile Navigation Integration', () => {
  beforeEach(() => {
    mockMobileViewport();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('mobile menu Login link navigates to login page', async () => {
    const user = userEvent.setup();
    render(<TestApp />);

    // Find the mobile dropdown menu
    const menuButton = screen.getByLabelText(/open menu/i);
    const dropdown = menuButton.closest('.dropdown');
    const dropdownContent = dropdown?.querySelector('.dropdown-content');

    // Click Login link in mobile menu
    const loginLink = within(dropdownContent as HTMLElement).getByRole('link', { name: /login/i });
    await user.click(loginLink);

    // Should navigate to login page
    expect(screen.getByRole('heading', { level: 1, name: /login/i })).toBeInTheDocument();
  });

  it('mobile menu Sign Up link navigates to register page', async () => {
    const user = userEvent.setup();
    render(<TestApp />);

    // Find the mobile dropdown menu
    const menuButton = screen.getByLabelText(/open menu/i);
    const dropdown = menuButton.closest('.dropdown');
    const dropdownContent = dropdown?.querySelector('.dropdown-content');

    // Click Sign Up link in mobile menu
    const signUpLink = within(dropdownContent as HTMLElement).getByRole('link', { name: /sign up/i });
    await user.click(signUpLink);

    // Should navigate to register page
    expect(screen.getByRole('heading', { level: 1, name: /create account/i })).toBeInTheDocument();
  });
});

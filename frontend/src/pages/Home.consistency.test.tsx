import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home from './Home';
import Login from './Login';
import { ThemeProvider } from '../contexts/ThemeContext';

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

// Helper to render Home with providers
const renderHome = () => {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  );
};

// Helper to render Login with providers
const renderLogin = () => {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        <Login />
      </BrowserRouter>
    </ThemeProvider>
  );
};

// Scenario: DaisyUI Component Consistency (NFR-5)
// Test Case 1: FuturisticButton component is used for primary CTAs
describe('Home - DaisyUI Component Consistency - Test Case 1: FuturisticButton Usage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('uses FuturisticButton for "Get Started Free" primary CTA', () => {
    renderHome();

    const getStartedButton = screen.getByRole('button', { name: /Get Started Free/i });
    expect(getStartedButton).toBeInTheDocument();

    // FuturisticButton applies DaisyUI 'btn' class and 'btn-primary' for primary variant
    expect(getStartedButton).toHaveClass('btn');
    expect(getStartedButton).toHaveClass('btn-primary');
  });

  it('uses FuturisticButton for "Learn More" secondary CTA', () => {
    renderHome();

    const learnMoreButton = screen.getByRole('button', { name: /Learn More/i });
    expect(learnMoreButton).toBeInTheDocument();

    // FuturisticButton with outline variant uses DaisyUI 'btn-outline'
    expect(learnMoreButton).toHaveClass('btn');
    expect(learnMoreButton).toHaveClass('btn-outline');
  });

  it('hero section CTA buttons have consistent DaisyUI size classes', () => {
    renderHome();

    const getStartedButton = screen.getByRole('button', { name: /Get Started Free/i });
    const learnMoreButton = screen.getByRole('button', { name: /Learn More/i });

    // Both buttons should use 'btn-lg' size for hero CTAs
    expect(getStartedButton).toHaveClass('btn-lg');
    expect(learnMoreButton).toHaveClass('btn-lg');
  });

  it('FuturisticButton applies transition classes for hover effects', () => {
    renderHome();

    const getStartedButton = screen.getByRole('button', { name: /Get Started Free/i });

    // FuturisticButton base classes include transition
    expect(getStartedButton).toHaveClass('transition-all');
    expect(getStartedButton).toHaveClass('duration-300');
  });

  it('all primary CTA buttons use FuturisticButton component pattern', () => {
    renderHome();

    const heroButtons = screen.getAllByRole('button').filter(btn =>
      btn.textContent?.includes('Get Started Free') ||
      btn.textContent?.includes('Learn More')
    );

    expect(heroButtons.length).toBe(2);

    heroButtons.forEach(button => {
      // All should have base 'btn' class from DaisyUI via FuturisticButton
      expect(button).toHaveClass('btn');
      // All should have font-semibold (FuturisticButton base class)
      expect(button).toHaveClass('font-semibold');
    });
  });
});

// Test Case 2: GlassMorphismCard component is used for feature cards
describe('Home - DaisyUI Component Consistency - Test Case 2: GlassMorphismCard Usage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('feature cards use GlassMorphismCard component', () => {
    renderHome();

    const featureCards = screen.getAllByTestId(/^feature-card-/);
    expect(featureCards.length).toBe(4);

    featureCards.forEach(card => {
      // GlassMorphismCard applies glassmorphism styles
      expect(card).toHaveClass('backdrop-blur-lg');
      expect(card).toHaveClass('rounded-2xl');
    });
  });

  it('GlassMorphismCard applies consistent border styling', () => {
    renderHome();

    const featureCards = screen.getAllByTestId(/^feature-card-/);

    featureCards.forEach(card => {
      // GlassMorphismCard border classes
      expect(card).toHaveClass('border');
    });
  });

  it('GlassMorphismCard applies shadow and hover transitions', () => {
    renderHome();

    const featureCards = screen.getAllByTestId(/^feature-card-/);

    featureCards.forEach(card => {
      expect(card).toHaveClass('shadow-xl');
      expect(card).toHaveClass('transition-all');
      expect(card).toHaveClass('duration-300');
    });
  });

  it('GlassMorphismCard applies consistent padding', () => {
    renderHome();

    const featureCards = screen.getAllByTestId(/^feature-card-/);

    featureCards.forEach(card => {
      // Default padding from GlassMorphismCard
      expect(card).toHaveClass('p-6');
    });
  });

  it('GlassMorphismCard renders title with consistent typography', () => {
    renderHome();

    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening');
    const title = urlShorteningCard.querySelector('[data-testid="card-title"]');

    expect(title).toBeInTheDocument();
    expect(title).toHaveClass('text-xl');
    expect(title).toHaveClass('font-bold');
    expect(title).toHaveClass('mb-2');
  });

  it('GlassMorphismCard icon container uses DaisyUI primary color', () => {
    renderHome();

    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening');
    const iconContainer = urlShorteningCard.querySelector('[data-testid="card-icon"]');

    expect(iconContainer).toBeInTheDocument();
    expect(iconContainer).toHaveClass('text-primary');
  });

  it('testimonial cards also use GlassMorphismCard component', () => {
    renderHome();

    const testimonialCards = screen.getAllByTestId(/^testimonial-card-/);
    expect(testimonialCards.length).toBe(3);

    testimonialCards.forEach(card => {
      // Same glassmorphism classes as feature cards
      expect(card).toHaveClass('backdrop-blur-lg');
      expect(card).toHaveClass('rounded-2xl');
      expect(card).toHaveClass('shadow-xl');
    });
  });

  it('demo section uses GlassMorphismCard for URL input area', () => {
    renderHome();

    const demoCard = screen.getByTestId('demo-card');
    expect(demoCard).toBeInTheDocument();
    expect(demoCard).toHaveClass('backdrop-blur-lg');
    expect(demoCard).toHaveClass('rounded-2xl');
  });
});

// Test Case 3: Visual styling consistency between homepage and login page
describe('Home - DaisyUI Component Consistency - Test Case 3: Cross-Page Styling Consistency', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('homepage and login page use same base background color class', () => {
    const { container: homeContainer } = renderHome();
    const homeRoot = homeContainer.firstElementChild;

    expect(homeRoot).toHaveClass('bg-base-200');
  });

  it('login page uses same base background color class', () => {
    const { container: loginContainer } = renderLogin();
    const loginRoot = loginContainer.firstElementChild;

    expect(loginRoot).toHaveClass('bg-base-200');
  });

  it('both pages use Navbar component for consistent navigation', () => {
    renderHome();

    // Homepage uses Navbar
    const homeNavbar = screen.getByRole('navigation');
    expect(homeNavbar).toBeInTheDocument();
  });

  it('login page also uses Navbar component', () => {
    renderLogin();

    // Login page uses Navbar
    const loginNavbar = screen.getByRole('navigation');
    expect(loginNavbar).toBeInTheDocument();
  });

  it('both pages use min-h-screen for full viewport height', () => {
    const { container: homeContainer } = renderHome();
    const homeRoot = homeContainer.firstElementChild;

    expect(homeRoot).toHaveClass('min-h-screen');
  });

  it('login page uses min-h-screen class', () => {
    const { container: loginContainer } = renderLogin();
    const loginRoot = loginContainer.firstElementChild;

    expect(loginRoot).toHaveClass('min-h-screen');
  });

  it('login page uses DaisyUI card component for form container', () => {
    renderLogin();

    const card = screen.getByRole('heading', { name: /login/i }).closest('.card');
    expect(card).toBeInTheDocument();
    expect(card).toHaveClass('card');
    expect(card).toHaveClass('bg-base-100');
    expect(card).toHaveClass('shadow-xl');
  });

  it('login page inputs use DaisyUI input classes', () => {
    renderLogin();

    const emailInput = screen.getByPlaceholderText(/your@email.com/i);
    expect(emailInput).toHaveClass('input');
    expect(emailInput).toHaveClass('input-bordered');
  });

  it('login page button uses DaisyUI btn-primary class', () => {
    renderLogin();

    const loginButton = screen.getByRole('button', { name: /login/i });
    expect(loginButton).toHaveClass('btn');
    expect(loginButton).toHaveClass('btn-primary');
    expect(loginButton).toHaveClass('w-full');
  });

  it('homepage and login share DaisyUI form-control pattern', () => {
    renderHome();

    // Demo section input uses form-control
    const demoSection = screen.getByTestId('demo-section');
    const formControl = demoSection.querySelector('.form-control');
    expect(formControl).toBeInTheDocument();
  });

  it('login page uses form-control for inputs', () => {
    renderLogin();

    const formControls = document.querySelectorAll('.form-control');
    expect(formControls.length).toBeGreaterThanOrEqual(2); // email and password
  });

  it('both pages use DaisyUI link styling for navigation', () => {
    renderLogin();

    // Find the inline text link (not the navbar button)
    const signUpLinks = screen.getAllByRole('link', { name: /sign up/i });
    // The link in the card body uses 'link link-primary' class
    const inlineLink = signUpLinks.find(link => link.classList.contains('link'));

    expect(inlineLink).toBeInTheDocument();
    expect(inlineLink).toHaveClass('link');
    expect(inlineLink).toHaveClass('link-primary');
  });

  it('homepage footer uses consistent DaisyUI footer classes', () => {
    renderHome();

    const footer = screen.getByTestId('homepage-footer');
    expect(footer).toHaveClass('footer');
    expect(footer).toHaveClass('footer-center');
    expect(footer).toHaveClass('bg-base-300');
  });
});

// Test Case 4: Consistent Tailwind utility class patterns
describe('Home - DaisyUI Component Consistency - Test Case 4: Tailwind Utility Patterns', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('hero section uses gradient background with DaisyUI color variables', () => {
    renderHome();

    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toHaveClass('hero');
    expect(heroSection).toHaveClass('bg-gradient-to-br');
  });

  it('sections use consistent max-width container patterns', () => {
    renderHome();

    const featuresSection = screen.getByTestId('features-section');
    const maxWidthContainer = featuresSection.querySelector('.max-w-6xl');
    expect(maxWidthContainer).toBeInTheDocument();
  });

  it('sections use consistent vertical padding pattern', () => {
    renderHome();

    const demoSection = screen.getByTestId('demo-section');
    expect(demoSection).toHaveClass('py-20');
    expect(demoSection).toHaveClass('px-4');
  });

  it('features section uses same padding pattern', () => {
    renderHome();

    const featuresSection = screen.getByTestId('features-section');
    expect(featuresSection).toHaveClass('py-20');
    expect(featuresSection).toHaveClass('px-4');
  });

  it('how-it-works section uses same padding pattern', () => {
    renderHome();

    const howItWorksSection = screen.getByTestId('how-it-works-section');
    expect(howItWorksSection).toHaveClass('py-20');
    expect(howItWorksSection).toHaveClass('px-4');
  });

  it('feature cards grid uses responsive column classes', () => {
    renderHome();

    const featuresGrid = screen.getByTestId('features-grid');
    expect(featuresGrid).toHaveClass('grid');
    expect(featuresGrid).toHaveClass('grid-cols-1');
    expect(featuresGrid).toHaveClass('md:grid-cols-2');
    expect(featuresGrid).toHaveClass('lg:grid-cols-4');
    expect(featuresGrid).toHaveClass('gap-6');
  });

  it('step indicators use DaisyUI primary color variables', () => {
    renderHome();

    const step1Indicator = screen.getByTestId('how-it-works-step-1-indicator');
    expect(step1Indicator).toHaveClass('bg-primary');
    expect(step1Indicator).toHaveClass('text-primary-content');
  });

  it('text uses DaisyUI base-content color for readability', () => {
    renderHome();

    const step1Description = screen.getByTestId('how-it-works-step-1-description');
    expect(step1Description.className).toContain('text-base-content');
  });

  it('demo section input uses DaisyUI input component classes', () => {
    renderHome();

    const demoInput = screen.getByTestId('demo-url-input');
    expect(demoInput).toHaveClass('input');
    expect(demoInput).toHaveClass('input-bordered');
  });

  it('demo shorten button uses DaisyUI btn classes', () => {
    renderHome();

    const shortenButton = screen.getByTestId('demo-shorten-button');
    expect(shortenButton).toHaveClass('btn');
    expect(shortenButton).toHaveClass('btn-primary');
  });

  it('statistics section uses consistent background with opacity', () => {
    renderHome();

    const socialProofSection = screen.getByTestId('social-proof-section');
    expect(socialProofSection).toHaveClass('bg-gradient-to-br');
  });

  it('statistics grid uses responsive column layout', () => {
    renderHome();

    const statisticsGrid = screen.getByTestId('statistics-grid');
    expect(statisticsGrid).toHaveClass('grid');
    expect(statisticsGrid).toHaveClass('grid-cols-2');
    expect(statisticsGrid).toHaveClass('md:grid-cols-4');
    expect(statisticsGrid).toHaveClass('gap-8');
  });

  it('testimonials grid uses responsive layout', () => {
    renderHome();

    const testimonialsGrid = screen.getByTestId('testimonials-grid');
    expect(testimonialsGrid).toHaveClass('grid');
    expect(testimonialsGrid).toHaveClass('grid-cols-1');
    expect(testimonialsGrid).toHaveClass('md:grid-cols-3');
    expect(testimonialsGrid).toHaveClass('gap-6');
  });

  it('how-it-works steps container uses flex with responsive direction', () => {
    renderHome();

    const stepsContainer = screen.getByTestId('how-it-works-steps-container');
    expect(stepsContainer).toHaveClass('flex');
    expect(stepsContainer).toHaveClass('flex-col');
    expect(stepsContainer).toHaveClass('md:flex-row');
    expect(stepsContainer).toHaveClass('gap-8');
  });

  it('CTA section uses DaisyUI primary background', () => {
    renderHome();

    // Find CTA section by its content
    const ctaHeading = screen.getByRole('heading', { name: /Ready to get started/i });
    const ctaSection = ctaHeading.closest('section');

    expect(ctaSection).toHaveClass('bg-primary');
    expect(ctaSection).toHaveClass('text-primary-content');
  });

  it('CTA section sign up button uses DaisyUI btn-secondary', () => {
    renderHome();

    // The CTA section has a RouterLink styled as button
    const ctaButton = screen.getByRole('link', { name: /Sign Up Free/i });
    expect(ctaButton).toHaveClass('btn');
    expect(ctaButton).toHaveClass('btn-secondary');
    expect(ctaButton).toHaveClass('btn-lg');
  });

  it('footer links use consistent DaisyUI link-hover class', () => {
    renderHome();

    const footer = screen.getByTestId('homepage-footer');
    const privacyLink = within(footer).getByRole('link', { name: /Privacy Policy/i });

    expect(privacyLink).toHaveClass('link');
    expect(privacyLink).toHaveClass('link-hover');
  });
});

/**
 * CTASection Component Tests
 * Owner: Scenario 4 - CTA Section Implementation
 *
 * Tests for:
 * - Component rendering with CTA content visible
 * - Value reinforcement text
 * - Primary CTA button with FuturisticButton
 * - Navigation to /register on button click
 * - Sign-in link visibility
 * - Navigation to /login on sign-in click
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import CTASection from '../../components/homepage/CTASection';

// Mock useNavigate
const mockNavigate = vi.fn();

vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

// Mock FuturisticButton to verify it's being used
// The mock adds data-futuristic-button attribute to verify the component is used
vi.mock('../../components/FuturisticButton', () => ({
  default: ({
    children,
    onClick,
    variant,
    ...props
  }: {
    children: React.ReactNode;
    onClick?: () => void;
    variant?: string;
  }) => (
    <button
      onClick={onClick}
      data-futuristic-button="true"
      data-variant={variant}
      {...props}
    >
      {children}
    </button>
  ),
  FuturisticButton: ({
    children,
    onClick,
    variant,
    ...props
  }: {
    children: React.ReactNode;
    onClick?: () => void;
    variant?: string;
  }) => (
    <button
      onClick={onClick}
      data-futuristic-button="true"
      data-variant={variant}
      {...props}
    >
      {children}
    </button>
  ),
}));

const renderWithRouter = (component: React.ReactElement) => {
  return render(<MemoryRouter>{component}</MemoryRouter>);
};

describe('CTASection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Component renders with CTA content visible
  it('renders with CTA content visible', () => {
    renderWithRouter(<CTASection />);

    const ctaSection = screen.getByTestId('cta-section');
    expect(ctaSection).toBeInTheDocument();
  });

  // Test Case 2: Value reinforcement text is visible
  it('displays compelling text encouraging registration', () => {
    renderWithRouter(<CTASection />);

    // Check for value reinforcement text (e.g., "Ready to supercharge your links?")
    const valueText = screen.getByTestId('cta-value-text');
    expect(valueText).toBeInTheDocument();
    // The text should contain encouraging language about registration/getting started
    expect(valueText.textContent).toMatch(
      /ready|supercharge|start|get started|transform|begin/i
    );
  });

  // Test Case 3: Primary CTA button is visible with appropriate text
  it('displays button with Create Account or Sign Up text', () => {
    renderWithRouter(<CTASection />);

    // Look for button with "Create Account" or "Sign Up" text
    const ctaButton = screen.getByRole('button', {
      name: /create.*account|sign.*up|create free account/i,
    });
    expect(ctaButton).toBeInTheDocument();
    expect(ctaButton).toBeEnabled();
  });

  // Test Case 4: Clicking primary CTA button navigates to /register
  it('navigates to /register when CTA button is clicked', () => {
    renderWithRouter(<CTASection />);

    const ctaButton = screen.getByRole('button', {
      name: /create.*account|sign.*up|create free account/i,
    });
    fireEvent.click(ctaButton);

    expect(mockNavigate).toHaveBeenCalledWith('/register');
  });

  // Test Case 4 Alternative: Uses custom callback when provided
  it('calls onCreateAccount callback when provided', () => {
    const onCreateAccount = vi.fn();
    renderWithRouter(<CTASection onCreateAccount={onCreateAccount} />);

    const ctaButton = screen.getByRole('button', {
      name: /create.*account|sign.*up|create free account/i,
    });
    fireEvent.click(ctaButton);

    expect(onCreateAccount).toHaveBeenCalled();
    expect(mockNavigate).not.toHaveBeenCalled();
  });

  // Test Case 5: Sign-in link is visible
  it('displays link with Sign In text', () => {
    renderWithRouter(<CTASection />);

    const signInLink = screen.getByTestId('cta-signin-link');
    expect(signInLink).toBeInTheDocument();
    expect(signInLink).toHaveTextContent(/sign in/i);
  });

  // Test Case 6: Clicking sign-in link navigates to /login
  it('navigates to /login when sign-in link is clicked', () => {
    renderWithRouter(<CTASection />);

    const signInLink = screen.getByTestId('cta-signin-link');
    fireEvent.click(signInLink);

    expect(mockNavigate).toHaveBeenCalledWith('/login');
  });

  // Test Case 6 Alternative: Uses custom callback when provided
  it('calls onSignIn callback when provided', () => {
    const onSignIn = vi.fn();
    renderWithRouter(<CTASection onSignIn={onSignIn} />);

    const signInLink = screen.getByTestId('cta-signin-link');
    fireEvent.click(signInLink);

    expect(onSignIn).toHaveBeenCalled();
    expect(mockNavigate).not.toHaveBeenCalled();
  });

  // Test Case 7: Primary CTA uses FuturisticButton component
  it('uses FuturisticButton component for primary CTA', () => {
    renderWithRouter(<CTASection />);

    // The mock adds data-futuristic-button attribute to verify FuturisticButton is used
    const primaryButton = screen.getByTestId('cta-primary-button');
    expect(primaryButton).toBeInTheDocument();
    // Verify it's using FuturisticButton (mock adds this attribute)
    expect(primaryButton).toHaveAttribute('data-futuristic-button', 'true');
    // Verify it's the primary variant
    expect(primaryButton).toHaveAttribute('data-variant', 'primary');
  });

  // Additional tests for styling and layout
  it('has centered layout', () => {
    renderWithRouter(<CTASection />);

    const ctaSection = screen.getByTestId('cta-section');
    expect(ctaSection).toHaveClass('text-center');
  });

  it('renders the "Already have an account?" text', () => {
    renderWithRouter(<CTASection />);

    expect(screen.getByText(/already have an account/i)).toBeInTheDocument();
  });
});

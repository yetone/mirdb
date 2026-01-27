/**
 * Unit Tests for CTASection Component
 * Owner: Scenario 10 - Call-to-Action Section
 *
 * Tests:
 * 1. CTA section renders with conversion prompt text before footer
 * 2. Registration CTA button exists with "Create Free Account" or "Get Started" text
 * 3. Reassurance text indicating free tier or no commitment is present
 * 4. Clicking CTA button navigates to /register page (integration)
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from './setup';
import CTASection from '../../../src/components/landing/CTASection';

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

describe('CTASection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Section with conversion prompt text is present
  it('renders CTA section with conversion prompt text', () => {
    render(<CTASection />);

    // Check that the CTA section is present
    const ctaSection = screen.getByTestId('cta-section');
    expect(ctaSection).toBeInTheDocument();

    // Check for conversion prompt heading
    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toBeInTheDocument();
    expect(heading).toHaveTextContent(/ready to start/i);
  });

  // Test Case 2: Registration CTA button exists
  it('renders registration CTA button with appropriate text', () => {
    render(<CTASection />);

    // Find the CTA button
    const ctaButton = screen.getByRole('button', { name: /create.*account|get started/i });
    expect(ctaButton).toBeInTheDocument();
    expect(ctaButton).toHaveTextContent(/create free account/i);
  });

  // Test Case 3: Reassurance text is present
  it('renders reassurance text indicating free tier and no commitment', () => {
    render(<CTASection />);

    // Check for reassurance text
    const reassuranceText = screen.getByTestId('reassurance-text');
    expect(reassuranceText).toBeInTheDocument();
    expect(reassuranceText).toHaveTextContent(/free to get started/i);
    expect(reassuranceText).toHaveTextContent(/no credit card/i);
  });

  // Test Case 4 (Integration): Clicking CTA navigates to /register
  it('navigates to /register when CTA button is clicked', () => {
    render(<CTASection />);

    const ctaButton = screen.getByRole('button', { name: /create.*account|get started/i });
    fireEvent.click(ctaButton);

    expect(mockNavigate).toHaveBeenCalledWith('/register');
  });

  // Additional test: Uses custom onRegister callback if provided
  it('calls onRegister callback when provided instead of navigating', () => {
    const onRegister = vi.fn();
    render(<CTASection onRegister={onRegister} />);

    const ctaButton = screen.getByRole('button', { name: /create.*account|get started/i });
    fireEvent.click(ctaButton);

    expect(onRegister).toHaveBeenCalled();
    expect(mockNavigate).not.toHaveBeenCalled();
  });

  // Test accessibility: Section has proper aria-labelledby
  it('has proper accessibility attributes', () => {
    render(<CTASection />);

    const section = screen.getByTestId('cta-section');
    expect(section).toHaveAttribute('aria-labelledby', 'cta-heading');

    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toHaveAttribute('id', 'cta-heading');
  });

  // Test: CTA button has proper aria-label
  it('CTA button has proper aria-label for accessibility', () => {
    render(<CTASection />);

    const ctaButton = screen.getByTestId('cta-register-button');
    expect(ctaButton).toHaveAttribute('aria-label', 'Create a free account');
  });

  // Test: Section contains supporting text
  it('renders supporting text explaining the value', () => {
    render(<CTASection />);

    const supportingText = screen.getByText(/join thousands of users/i);
    expect(supportingText).toBeInTheDocument();
  });
});

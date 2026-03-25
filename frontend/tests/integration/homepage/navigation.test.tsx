/**
 * Navigation Integration Tests
 * Owner: Scenario 2 - Get Started CTA Navigation (partial)
 *        Scenario 3 - Sign In CTA Navigation (partial)
 *
 * Tests CTA button navigation from homepage to registration/login pages.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';
import Home from '../../../src/pages/Home';
import { HeroSection } from '../../../src/components/homepage/HeroSection';

// Mock navigate function
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

interface TestWrapperProps {
  children: React.ReactNode;
  isAuthenticated?: boolean;
  initialRoute?: string;
}

const TestWrapper: React.FC<TestWrapperProps> = ({
  children,
  isAuthenticated = false,
  initialRoute = '/',
}) => {
  return (
    <MemoryRouter initialEntries={[initialRoute]}>
      <AuthProvider initialAuth={isAuthenticated}>
        <ThemeProvider initialTheme="dark">
          {children}
        </ThemeProvider>
      </AuthProvider>
    </MemoryRouter>
  );
};

describe('Scenario 2: Get Started CTA Navigation', () => {
  beforeEach(() => {
    mockNavigate.mockClear();
  });

  describe('Test Case 1: Get Started button visibility', () => {
    it('renders Get Started button in hero section for unauthenticated users', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <Home />
        </TestWrapper>
      );

      const getStartedButton = screen.getByRole('button', { name: /get started/i });
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton).toBeVisible();
    });

    it('displays Get Started button prominently in hero section', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <HeroSection isAuthenticated={false} />
        </TestWrapper>
      );

      // Hero section is labeled by the heading
      const heroSection = screen.getByRole('region', { name: /shorten your urls/i });
      expect(heroSection).toBeInTheDocument();

      const getStartedButton = screen.getByRole('button', { name: /get started/i });
      expect(heroSection).toContainElement(getStartedButton);
    });
  });

  describe('Test Case 2: Navigation to /register', () => {
    it('navigates to /register when Get Started button is clicked', async () => {
      const user = userEvent.setup();

      render(
        <TestWrapper isAuthenticated={false}>
          <Home />
        </TestWrapper>
      );

      const getStartedButton = screen.getByRole('button', { name: /get started/i });
      await user.click(getStartedButton);

      expect(mockNavigate).toHaveBeenCalledWith('/register');
    });

    it('triggers navigation on click event', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <HeroSection isAuthenticated={false} />
        </TestWrapper>
      );

      const getStartedButton = screen.getByRole('button', { name: /get started/i });
      fireEvent.click(getStartedButton);

      expect(mockNavigate).toHaveBeenCalledTimes(1);
      expect(mockNavigate).toHaveBeenCalledWith('/register');
    });
  });

  describe('Test Case 3: FuturisticButton component usage', () => {
    it('renders CTA button with proper styling classes', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <HeroSection isAuthenticated={false} />
        </TestWrapper>
      );

      const getStartedButton = screen.getByRole('button', { name: /get started/i });

      // Check for FuturisticButton styling characteristics
      expect(getStartedButton).toHaveClass('bg-gradient-to-r');
      expect(getStartedButton).toHaveClass('rounded-lg');
      expect(getStartedButton).toHaveClass('font-semibold');
    });

    it('has hover effect transition classes', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <HeroSection isAuthenticated={false} />
        </TestWrapper>
      );

      const getStartedButton = screen.getByRole('button', { name: /get started/i });

      // FuturisticButton should have transition and transform classes
      expect(getStartedButton).toHaveClass('transition-all');
      expect(getStartedButton).toHaveClass('hover:scale-105');
    });
  });

  describe('Test Case 4: Accessibility', () => {
    it('has proper ARIA attributes', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <HeroSection isAuthenticated={false} />
        </TestWrapper>
      );

      const getStartedButton = screen.getByRole('button', { name: /get started/i });

      // Button should have accessible name via aria-label
      expect(getStartedButton).toHaveAttribute('aria-label');
      expect(getStartedButton.getAttribute('aria-label')).toContain('Get started');
    });

    it('is keyboard accessible with proper focus styling', () => {
      render(
        <TestWrapper isAuthenticated={false}>
          <HeroSection isAuthenticated={false} />
        </TestWrapper>
      );

      const getStartedButton = screen.getByRole('button', { name: /get started/i });

      // Button should be focusable
      getStartedButton.focus();
      expect(getStartedButton).toHaveFocus();

      // Should have focus ring classes
      expect(getStartedButton).toHaveClass('focus:outline-none');
      expect(getStartedButton).toHaveClass('focus:ring-2');
    });

    it('can be activated via keyboard Enter key', async () => {
      const user = userEvent.setup();

      render(
        <TestWrapper isAuthenticated={false}>
          <HeroSection isAuthenticated={false} />
        </TestWrapper>
      );

      const getStartedButton = screen.getByRole('button', { name: /get started/i });

      getStartedButton.focus();
      await user.keyboard('{Enter}');

      expect(mockNavigate).toHaveBeenCalledWith('/register');
    });

    it('can be activated via keyboard Space key', async () => {
      const user = userEvent.setup();

      render(
        <TestWrapper isAuthenticated={false}>
          <HeroSection isAuthenticated={false} />
        </TestWrapper>
      );

      const getStartedButton = screen.getByRole('button', { name: /get started/i });

      getStartedButton.focus();
      await user.keyboard(' ');

      expect(mockNavigate).toHaveBeenCalledWith('/register');
    });
  });

  describe('Authenticated user experience', () => {
    it('shows Go to Dashboard instead of Get Started for authenticated users', () => {
      render(
        <TestWrapper isAuthenticated={true}>
          <Home />
        </TestWrapper>
      );

      expect(screen.queryByRole('button', { name: /get started/i })).not.toBeInTheDocument();
      expect(screen.getByRole('button', { name: /go to your dashboard/i })).toBeInTheDocument();
    });

    it('navigates to /dashboard when authenticated user clicks CTA', async () => {
      const user = userEvent.setup();

      render(
        <TestWrapper isAuthenticated={true}>
          <HeroSection isAuthenticated={true} />
        </TestWrapper>
      );

      const dashboardButton = screen.getByRole('button', { name: /go to your dashboard/i });
      await user.click(dashboardButton);

      expect(mockNavigate).toHaveBeenCalledWith('/dashboard');
    });
  });
});

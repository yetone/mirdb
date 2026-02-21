/**
 * CTASection Unit Tests
 * Owner: Scenario 5 - CTA & Footer
 *
 * Test cases:
 * 1. CTA banner with sign-up prompt text is displayed
 * 2. CTA button linking to registration is present
 * 3. Navigation to /register route occurs on button click
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom';
import CTASection from '../../../src/components/homepage/CTASection';

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
    mockNavigate.mockClear();
  });

  // Test Case 1: CTA banner with sign-up prompt text is displayed
  it('renders CTA banner with sign-up prompt text', () => {
    render(
      <BrowserRouter>
        <CTASection />
      </BrowserRouter>
    );

    const ctaSection = screen.getByTestId('cta-section');
    expect(ctaSection).toBeInTheDocument();

    // Check for sign-up prompt text (should contain call-to-action messaging)
    const promptText = screen.getByTestId('cta-prompt');
    expect(promptText).toBeInTheDocument();
    expect(promptText.textContent?.toLowerCase()).toMatch(/sign up|get started|join|start|ready|create/);
  });

  // Test Case 2: CTA button linking to registration is present
  it('displays CTA button that links to registration', () => {
    render(
      <BrowserRouter>
        <CTASection />
      </BrowserRouter>
    );

    const ctaButton = screen.getByTestId('cta-button');
    expect(ctaButton).toBeInTheDocument();
    expect(ctaButton).toBeVisible();
    expect(ctaButton).toHaveClass('btn');
  });

  // Test Case 3: Navigation to /register route occurs on button click
  it('navigates to /register when CTA button is clicked', () => {
    render(
      <BrowserRouter>
        <CTASection />
      </BrowserRouter>
    );

    const ctaButton = screen.getByTestId('cta-button');
    fireEvent.click(ctaButton);

    expect(mockNavigate).toHaveBeenCalledWith('/register');
  });

  // Additional test: CTA section has visually distinct background
  it('has visually distinct background styling', () => {
    render(
      <BrowserRouter>
        <CTASection />
      </BrowserRouter>
    );

    const ctaSection = screen.getByTestId('cta-section');
    // Check for background classes that create visual distinction
    expect(ctaSection.className).toMatch(/bg-|background/);
  });

  // Additional test: CTA content is center-aligned
  it('has center-aligned content layout', () => {
    render(
      <BrowserRouter>
        <CTASection />
      </BrowserRouter>
    );

    const ctaSection = screen.getByTestId('cta-section');
    const ctaContent = screen.getByTestId('cta-content');

    expect(ctaContent).toHaveClass('text-center');
  });
});

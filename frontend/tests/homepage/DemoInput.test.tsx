/**
 * Demo URL Input Tests
 * Owner: Scenario 6 - Demo URL Input (Teaser)
 *
 * Test cases:
 * 1. Input field exists with placeholder text for URL entry
 * 2. Submit button exists to trigger demo action
 * 3. User is redirected to /register when valid URL is submitted
 * 4. Invalid URL shows visual error indication or message
 * 5. URL shortening does not actually occur - only prompts registration
 * 6. Input has associated label or aria-label for screen readers
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { DemoInput } from '../../src/components/homepage/DemoInput';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    button: ({
      children,
      ...props
    }: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
      <button {...props}>{children}</button>
    ),
  },
  HTMLMotionProps: {},
}));

function renderWithRouter(
  component: React.ReactElement,
  { route = '/' } = {}
) {
  return {
    ...render(
      <MemoryRouter initialEntries={[route]}>
        <Routes>
          <Route path="/" element={component} />
          <Route
            path="/register"
            element={<div data-testid="register-page">Register Page</div>}
          />
        </Routes>
      </MemoryRouter>
    ),
  };
}

describe('DemoInput', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Input field exists with placeholder text for URL entry
  it('displays URL input field with placeholder text', () => {
    renderWithRouter(<DemoInput />);

    const input = screen.getByTestId('demo-url-input');
    expect(input).toBeInTheDocument();
    expect(input).toHaveAttribute('placeholder');
    expect(input.getAttribute('placeholder')).toMatch(/url/i);
  });

  // Test Case 2: Submit button exists to trigger demo action
  it('displays Shorten submit button', () => {
    renderWithRouter(<DemoInput />);

    const button = screen.getByTestId('demo-shorten-button');
    expect(button).toBeInTheDocument();
    expect(button).toBeVisible();
    expect(button).toHaveTextContent(/shorten/i);
  });

  // Test Case 3: User is redirected to /register when valid URL is submitted
  it('redirects to /register when valid URL is entered and submitted', async () => {
    const user = userEvent.setup();
    renderWithRouter(<DemoInput />);

    const input = screen.getByTestId('demo-url-input');
    const button = screen.getByTestId('demo-shorten-button');

    await user.type(input, 'https://example.com');
    await user.click(button);

    expect(await screen.findByTestId('register-page')).toBeInTheDocument();
  });

  // Test Case 4: Invalid URL shows visual error indication or message
  it('shows error message for invalid URL', async () => {
    const user = userEvent.setup();
    renderWithRouter(<DemoInput />);

    const input = screen.getByTestId('demo-url-input');
    const button = screen.getByTestId('demo-shorten-button');

    await user.type(input, 'not-a-valid-url');
    await user.click(button);

    const errorElement = await screen.findByTestId('demo-url-error');
    expect(errorElement).toBeInTheDocument();
    expect(errorElement).toHaveTextContent(/valid url/i);
  });

  // Test Case 4b: Input has error styling when invalid
  it('applies error styling to input when URL is invalid', async () => {
    const user = userEvent.setup();
    renderWithRouter(<DemoInput />);

    const input = screen.getByTestId('demo-url-input');
    const button = screen.getByTestId('demo-shorten-button');

    await user.type(input, 'invalid');
    await user.click(button);

    await waitFor(() => {
      expect(input).toHaveAttribute('aria-invalid', 'true');
    });
  });

  // Test Case 5: URL shortening does not actually occur - only prompts registration
  it('does not perform actual URL shortening - only redirects to registration', async () => {
    const user = userEvent.setup();
    const onSubmitMock = vi.fn();
    renderWithRouter(<DemoInput onSubmit={onSubmitMock} />);

    const input = screen.getByTestId('demo-url-input');
    const button = screen.getByTestId('demo-shorten-button');

    await user.type(input, 'https://example.com');
    await user.click(button);

    // The onSubmit callback is called but no actual shortening happens
    expect(onSubmitMock).toHaveBeenCalledWith('https://example.com');

    // User is redirected to registration
    expect(await screen.findByTestId('register-page')).toBeInTheDocument();
  });

  // Test Case 6: Input has associated label or aria-label for screen readers
  it('has accessible label for screen readers', () => {
    renderWithRouter(<DemoInput />);

    const input = screen.getByTestId('demo-url-input');

    // Check for aria-label attribute
    expect(input).toHaveAttribute('aria-label');
    const ariaLabel = input.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel?.toLowerCase()).toMatch(/url/i);

    // Also verify the input can be found by its label
    const inputByLabel = screen.getByLabelText(/url/i);
    expect(inputByLabel).toBeInTheDocument();
  });

  // Additional test: Empty URL shows validation error
  it('shows error message when submitting empty URL', async () => {
    const user = userEvent.setup();
    renderWithRouter(<DemoInput />);

    const button = screen.getByTestId('demo-shorten-button');
    await user.click(button);

    const errorElement = await screen.findByTestId('demo-url-error');
    expect(errorElement).toBeInTheDocument();
    expect(errorElement).toHaveTextContent(/enter a url/i);
  });

  // Additional test: Error clears when valid URL is entered after error
  it('clears error when valid URL is entered after validation error', async () => {
    const user = userEvent.setup();
    renderWithRouter(<DemoInput />);

    const input = screen.getByTestId('demo-url-input');
    const button = screen.getByTestId('demo-shorten-button');

    // First, trigger an error
    await user.type(input, 'invalid');
    await user.click(button);

    expect(await screen.findByTestId('demo-url-error')).toBeInTheDocument();

    // Clear and enter valid URL
    await user.clear(input);
    await user.type(input, 'https://example.com');

    await waitFor(() => {
      expect(screen.queryByTestId('demo-url-error')).not.toBeInTheDocument();
    });
  });

  // Additional test: Section has proper test id
  it('renders demo input section with proper structure', () => {
    renderWithRouter(<DemoInput />);

    const section = screen.getByTestId('demo-input-section');
    expect(section).toBeInTheDocument();
    expect(section.tagName).toBe('SECTION');
  });

  // Additional test: Form submission with Enter key
  it('submits form when Enter key is pressed in input', async () => {
    const user = userEvent.setup();
    renderWithRouter(<DemoInput />);

    const input = screen.getByTestId('demo-url-input');

    await user.type(input, 'https://example.com{enter}');

    expect(await screen.findByTestId('register-page')).toBeInTheDocument();
  });

  // Additional test: Input has proper type attribute
  it('has input type of url', () => {
    renderWithRouter(<DemoInput />);

    const input = screen.getByTestId('demo-url-input');
    expect(input).toHaveAttribute('type', 'url');
  });

  // Additional test: Error has proper aria role
  it('error message has alert role for accessibility', async () => {
    const user = userEvent.setup();
    renderWithRouter(<DemoInput />);

    const input = screen.getByTestId('demo-url-input');
    const button = screen.getByTestId('demo-shorten-button');

    await user.type(input, 'invalid');
    await user.click(button);

    const errorElement = await screen.findByTestId('demo-url-error');
    expect(errorElement).toHaveAttribute('role', 'alert');
  });
});

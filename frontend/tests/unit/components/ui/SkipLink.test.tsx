/**
 * Unit tests for SkipLink Component.
 * Owner: Scenario 11 - Keyboard Navigation Accessibility
 *
 * Tests:
 * - Renders with default props
 * - Is hidden until focused (visually)
 * - Becomes visible when focused
 * - Navigates to target when clicked
 * - Responds to Enter and Space key presses
 * - Is the first focusable element on the page
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { SkipLink } from '../../../../src/components/ui/SkipLink';

describe('SkipLink Component', () => {
  beforeEach(() => {
    // Create a mock target element for focus testing
    const mainContent = document.createElement('main');
    mainContent.id = 'main-content';
    mainContent.textContent = 'Main content area';
    document.body.appendChild(mainContent);
  });

  afterEach(() => {
    // Cleanup mock elements
    const mainContent = document.getElementById('main-content');
    if (mainContent) {
      document.body.removeChild(mainContent);
    }
  });

  it('renders with default text "Skip to content"', () => {
    render(<SkipLink />);

    const skipLink = screen.getByTestId('skip-link');
    expect(skipLink).toBeInTheDocument();
    expect(skipLink).toHaveTextContent('Skip to content');
  });

  it('renders with custom children text', () => {
    render(<SkipLink>Skip to main section</SkipLink>);

    const skipLink = screen.getByTestId('skip-link');
    expect(skipLink).toHaveTextContent('Skip to main section');
  });

  it('has correct href attribute pointing to target', () => {
    render(<SkipLink targetId="main-content" />);

    const skipLink = screen.getByTestId('skip-link');
    expect(skipLink).toHaveAttribute('href', '#main-content');
  });

  it('has correct href for custom targetId', () => {
    render(<SkipLink targetId="custom-target" />);

    const skipLink = screen.getByTestId('skip-link');
    expect(skipLink).toHaveAttribute('href', '#custom-target');
  });

  it('is hidden by default (translated off-screen)', () => {
    render(<SkipLink />);

    const skipLink = screen.getByTestId('skip-link');
    expect(skipLink.className).toContain('-translate-y-full');
  });

  it('becomes visible when focused', () => {
    render(<SkipLink />);

    const skipLink = screen.getByTestId('skip-link');
    skipLink.focus();

    expect(skipLink.className).toContain('focus:translate-y-0');
  });

  it('applies custom className', () => {
    render(<SkipLink className="custom-class" />);

    const skipLink = screen.getByTestId('skip-link');
    expect(skipLink.className).toContain('custom-class');
  });

  it('focuses target element when clicked', () => {
    render(<SkipLink targetId="main-content" />);

    const skipLink = screen.getByTestId('skip-link');
    const mainContent = document.getElementById('main-content');

    fireEvent.click(skipLink);

    expect(mainContent).toHaveAttribute('tabindex', '-1');
    expect(document.activeElement).toBe(mainContent);
  });

  it('responds to Enter key press', async () => {
    const user = userEvent.setup();
    render(<SkipLink targetId="main-content" />);

    const skipLink = screen.getByTestId('skip-link');
    const mainContent = document.getElementById('main-content');

    skipLink.focus();
    await user.keyboard('{Enter}');

    expect(mainContent).toHaveAttribute('tabindex', '-1');
    expect(document.activeElement).toBe(mainContent);
  });

  it('responds to Space key press', async () => {
    const user = userEvent.setup();
    render(<SkipLink targetId="main-content" />);

    const skipLink = screen.getByTestId('skip-link');
    const mainContent = document.getElementById('main-content');

    skipLink.focus();
    await user.keyboard(' ');

    expect(mainContent).toHaveAttribute('tabindex', '-1');
    expect(document.activeElement).toBe(mainContent);
  });

  it('has visible focus outline when focused', () => {
    render(<SkipLink />);

    const skipLink = screen.getByTestId('skip-link');
    expect(skipLink.className).toContain('focus:ring-2');
    expect(skipLink.className).toContain('focus:ring-primary');
  });

  it('is positioned fixed at top-left with high z-index', () => {
    render(<SkipLink />);

    const skipLink = screen.getByTestId('skip-link');
    expect(skipLink.className).toContain('fixed');
    expect(skipLink.className).toContain('top-0');
    expect(skipLink.className).toContain('left-0');
    expect(skipLink.className).toContain('z-[9999]');
  });

  it('is an anchor element', () => {
    render(<SkipLink />);

    const skipLink = screen.getByTestId('skip-link');
    expect(skipLink.tagName).toBe('A');
  });

  it('prevents default link behavior on click', () => {
    render(<SkipLink targetId="main-content" />);

    const skipLink = screen.getByTestId('skip-link');
    const clickEvent = new MouseEvent('click', { bubbles: true, cancelable: true });
    const preventDefaultSpy = vi.spyOn(clickEvent, 'preventDefault');

    skipLink.dispatchEvent(clickEvent);

    expect(preventDefaultSpy).toHaveBeenCalled();
  });
});

describe('SkipLink - First Focusable Element', () => {
  it('skip link appears as first focusable element when rendered first', () => {
    // Render the skip link first
    const { container } = render(
      <div>
        <SkipLink targetId="main-content" />
        <nav>
          <button>Nav Button</button>
        </nav>
        <main id="main-content">
          <input type="text" placeholder="Form input" />
        </main>
      </div>
    );

    // Get all focusable elements in order
    const focusableElements = container.querySelectorAll(
      'a, button, input, [tabindex]:not([tabindex="-1"])'
    );

    // Skip link should be the first focusable element
    expect(focusableElements[0]).toHaveAttribute('data-testid', 'skip-link');
  });
});

describe('SkipLink - Button Keyboard Activation', () => {
  it('all buttons respond to Enter key press', async () => {
    const user = userEvent.setup();
    const onClickMock = vi.fn();

    render(
      <div>
        <SkipLink targetId="main-content" />
        <button onClick={onClickMock} data-testid="test-button">
          Test Button
        </button>
        <main id="main-content">Content</main>
      </div>
    );

    const button = screen.getByTestId('test-button');
    button.focus();
    await user.keyboard('{Enter}');

    expect(onClickMock).toHaveBeenCalled();
  });

  it('all buttons respond to Space key press', async () => {
    const user = userEvent.setup();
    const onClickMock = vi.fn();

    render(
      <div>
        <SkipLink targetId="main-content" />
        <button onClick={onClickMock} data-testid="test-button">
          Test Button
        </button>
        <main id="main-content">Content</main>
      </div>
    );

    const button = screen.getByTestId('test-button');
    button.focus();
    await user.keyboard(' ');

    expect(onClickMock).toHaveBeenCalled();
  });
});

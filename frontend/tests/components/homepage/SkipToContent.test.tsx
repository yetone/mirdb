import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import SkipToContent from '../../../src/components/homepage/SkipToContent';

/**
 * Tests for the SkipToContent link.
 * Owner: Scenario 7 - Accessibility and Keyboard Navigation.
 *
 * The skip link is the first focusable element on the page. It is visually
 * hidden until focused, then revealed at the top of the viewport so keyboard
 * users can bypass the navigation and jump straight to the main region.
 * Required by WCAG 2.4.1 "Bypass Blocks".
 */
describe('SkipToContent', () => {
  it('renders an anchor element with the default label', () => {
    render(<SkipToContent />);

    const link = screen.getByRole('link', { name: /skip to main content/i });
    expect(link).toBeInTheDocument();
    expect(link.tagName.toLowerCase()).toBe('a');
  });

  it('points at the #main-content anchor by default', () => {
    render(<SkipToContent />);

    const link = screen.getByTestId('skip-to-content');
    expect(link).toHaveAttribute('href', '#main-content');
  });

  it('honours a custom target id', () => {
    render(<SkipToContent targetId="content" label="Skip to content" />);

    const link = screen.getByRole('link', { name: /skip to content/i });
    expect(link).toHaveAttribute('href', '#content');
  });

  it('is hidden by default (sr-only) but revealed when focused', () => {
    render(<SkipToContent />);

    const link = screen.getByTestId('skip-to-content');
    // It has the sr-only class so it is visually hidden until focus.
    expect(link.className).toMatch(/sr-only/);
    // And it has a focus-visible class that removes sr-only.
    expect(link.className).toMatch(/focus:not-sr-only|focus-visible:not-sr-only/);
  });

  it('moves focus to the target main region when activated', async () => {
    const user = userEvent.setup();

    render(
      <>
        <SkipToContent />
        <main id="main-content" tabIndex={-1} data-testid="page-main">
          Main content
        </main>
      </>
    );

    const link = screen.getByTestId('skip-to-content');
    await user.click(link);

    const main = screen.getByTestId('page-main');
    expect(document.activeElement).toBe(main);
  });

  it('adds tabindex=-1 to the target if missing so it can receive focus', async () => {
    const user = userEvent.setup();

    render(
      <>
        <SkipToContent />
        <main id="main-content" data-testid="page-main">
          Main content
        </main>
      </>
    );

    const link = screen.getByTestId('skip-to-content');
    await user.click(link);

    const main = screen.getByTestId('page-main');
    expect(main).toHaveAttribute('tabindex', '-1');
    expect(document.activeElement).toBe(main);
  });

  it('does not throw when activated while the target is missing', async () => {
    const user = userEvent.setup();

    render(<SkipToContent targetId="does-not-exist" />);

    const link = screen.getByTestId('skip-to-content');
    await expect(user.click(link)).resolves.not.toThrow();
  });

  it('is the first focusable element when placed at the top of the DOM', async () => {
    const user = userEvent.setup();

    render(
      <>
        <SkipToContent />
        <button type="button">Other action</button>
      </>
    );

    expect(document.body).toBe(document.activeElement);
    await user.tab();
    const link = screen.getByTestId('skip-to-content');
    expect(document.activeElement).toBe(link);
  });
});

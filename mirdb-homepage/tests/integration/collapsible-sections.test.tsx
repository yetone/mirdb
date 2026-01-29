/**
 * Integration tests for collapsible sections functionality.
 * Owner: Scenario 5 - Configuration Reference Section
 *
 * Tests:
 * - Multiple collapsible sections work independently
 * - Expand/collapse animations and transitions
 * - Focus management and keyboard navigation
 * - Content visibility after expand/collapse
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { CollapsibleSection } from '../../src/components/Configuration/CollapsibleSection';

describe('Collapsible Sections Integration', () => {
  it('multiple sections can be expanded independently', async () => {
    const user = userEvent.setup();

    render(
      <div>
        <CollapsibleSection title="Section A" testId="section-a">
          <div data-testid="content-a">Content A</div>
        </CollapsibleSection>
        <CollapsibleSection title="Section B" testId="section-b">
          <div data-testid="content-b">Content B</div>
        </CollapsibleSection>
        <CollapsibleSection title="Section C" testId="section-c">
          <div data-testid="content-c">Content C</div>
        </CollapsibleSection>
      </div>
    );

    const toggleA = screen.getByTestId('section-a-toggle');
    const toggleB = screen.getByTestId('section-b-toggle');
    const toggleC = screen.getByTestId('section-c-toggle');

    // All start collapsed
    expect(toggleA).toHaveAttribute('aria-expanded', 'false');
    expect(toggleB).toHaveAttribute('aria-expanded', 'false');
    expect(toggleC).toHaveAttribute('aria-expanded', 'false');

    // Expand section A
    await user.click(toggleA);
    expect(toggleA).toHaveAttribute('aria-expanded', 'true');
    expect(toggleB).toHaveAttribute('aria-expanded', 'false');
    expect(toggleC).toHaveAttribute('aria-expanded', 'false');

    // Expand section B as well
    await user.click(toggleB);
    expect(toggleA).toHaveAttribute('aria-expanded', 'true');
    expect(toggleB).toHaveAttribute('aria-expanded', 'true');
    expect(toggleC).toHaveAttribute('aria-expanded', 'false');

    // Collapse section A, B stays expanded
    await user.click(toggleA);
    expect(toggleA).toHaveAttribute('aria-expanded', 'false');
    expect(toggleB).toHaveAttribute('aria-expanded', 'true');
    expect(toggleC).toHaveAttribute('aria-expanded', 'false');
  });

  it('content is visible when section is expanded', async () => {
    const user = userEvent.setup();

    render(
      <CollapsibleSection title="Visibility Test" testId="visibility-section">
        <div data-testid="hidden-content">
          <p>This content should be visible when expanded</p>
          <ul>
            <li>Item 1</li>
            <li>Item 2</li>
          </ul>
        </div>
      </CollapsibleSection>
    );

    const toggle = screen.getByTestId('visibility-section-toggle');

    // Initially collapsed
    expect(toggle).toHaveAttribute('aria-expanded', 'false');

    // Content container exists but has max-height: 0
    const contentContainer = screen.getByTestId('visibility-section-content');
    expect(contentContainer).toHaveStyle({ maxHeight: '0px' });

    // Expand section
    await user.click(toggle);

    // Verify aria-expanded is now true (content is accessible)
    expect(toggle).toHaveAttribute('aria-expanded', 'true');

    // Content should be in the document and accessible
    expect(screen.getByTestId('hidden-content')).toBeInTheDocument();
    expect(screen.getByText('This content should be visible when expanded')).toBeInTheDocument();
  });

  it('handles rapid toggle clicks gracefully', async () => {
    const user = userEvent.setup();

    render(
      <CollapsibleSection title="Rapid Toggle" testId="rapid-section">
        <div>Content</div>
      </CollapsibleSection>
    );

    const toggle = screen.getByTestId('rapid-section-toggle');

    // Rapid clicks
    await user.click(toggle); // expand
    await user.click(toggle); // collapse
    await user.click(toggle); // expand
    await user.click(toggle); // collapse
    await user.click(toggle); // expand

    // Should end up expanded
    expect(toggle).toHaveAttribute('aria-expanded', 'true');
  });

  it('keyboard navigation works correctly', async () => {
    const user = userEvent.setup();

    render(
      <div>
        <CollapsibleSection title="First" testId="first-section">
          <button data-testid="first-button">First Button</button>
        </CollapsibleSection>
        <CollapsibleSection title="Second" testId="second-section">
          <button data-testid="second-button">Second Button</button>
        </CollapsibleSection>
      </div>
    );

    const firstToggle = screen.getByTestId('first-section-toggle');
    const secondToggle = screen.getByTestId('second-section-toggle');

    // Focus first toggle
    firstToggle.focus();
    expect(document.activeElement).toBe(firstToggle);

    // Press Enter to expand
    await user.keyboard('{Enter}');
    expect(firstToggle).toHaveAttribute('aria-expanded', 'true');

    // Tab to second toggle
    await user.tab();
    await user.tab();
    expect(document.activeElement).toBe(secondToggle);

    // Press Space to expand second
    await user.keyboard(' ');
    expect(secondToggle).toHaveAttribute('aria-expanded', 'true');
  });

  it('preserves focus after toggle', async () => {
    const user = userEvent.setup();

    render(
      <CollapsibleSection title="Focus Test" testId="focus-section">
        <div>Content</div>
      </CollapsibleSection>
    );

    const toggle = screen.getByTestId('focus-section-toggle');

    // Click and verify focus stays on button
    await user.click(toggle);
    expect(document.activeElement).toBe(toggle);

    // Click again
    await user.click(toggle);
    expect(document.activeElement).toBe(toggle);
  });

  it('renders with proper border and rounded corners', () => {
    render(
      <CollapsibleSection title="Styled Section" testId="styled-section">
        <div>Content</div>
      </CollapsibleSection>
    );

    // testId prop is used directly for outer div
    const section = screen.getByTestId('styled-section');
    expect(section).toHaveClass('border');
    expect(section).toHaveClass('rounded-lg');
  });

  it('toggle button has surface background and hover state', () => {
    render(
      <CollapsibleSection title="Hover Test" testId="hover-section">
        <div>Content</div>
      </CollapsibleSection>
    );

    const toggle = screen.getByTestId('hover-section-toggle');
    expect(toggle).toHaveClass('bg-surface');
    expect(toggle).toHaveClass('hover:bg-opacity-80');
  });

  it('chevron icon rotates when expanded', async () => {
    const user = userEvent.setup();

    render(
      <CollapsibleSection title="Chevron Test" testId="chevron-section">
        <div>Content</div>
      </CollapsibleSection>
    );

    const toggle = screen.getByTestId('chevron-section-toggle');

    // Find chevron span (contains the SVG)
    const chevronSpan = toggle.querySelector('span[aria-hidden="true"]');
    expect(chevronSpan).toHaveClass('rotate-0');

    // Expand
    await user.click(toggle);

    // Chevron should rotate
    expect(chevronSpan).toHaveClass('rotate-180');
  });

  it('content container has smooth transition', () => {
    render(
      <CollapsibleSection title="Transition Test" testId="transition-section">
        <div>Content</div>
      </CollapsibleSection>
    );

    const contentContainer = screen.getByTestId('transition-section-content');
    expect(contentContainer).toHaveClass('transition-[max-height]');
    expect(contentContainer).toHaveClass('duration-300');
    expect(contentContainer).toHaveClass('ease-in-out');
  });
});

describe('Configuration Categories Collapsible Integration', () => {
  it('configuration sections render with correct structure', () => {
    render(
      <div data-testid="config-categories">
        <CollapsibleSection title="Network" defaultOpen={true} testId="config-network">
          <table data-testid="network-options">
            <tbody>
              <tr data-testid="option-addr">
                <td>addr</td>
                <td data-testid="default-addr">0.0.0.0:12333</td>
              </tr>
            </tbody>
          </table>
        </CollapsibleSection>
        <CollapsibleSection title="Storage" testId="config-storage">
          <table data-testid="storage-options">
            <tbody>
              <tr data-testid="option-max_level">
                <td>max_level</td>
                <td data-testid="default-max_level">7</td>
              </tr>
            </tbody>
          </table>
        </CollapsibleSection>
      </div>
    );

    // Network section should be open by default
    expect(screen.getByTestId('config-network-toggle')).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByTestId('network-options')).toBeInTheDocument();
    expect(screen.getByTestId('default-addr')).toHaveTextContent('0.0.0.0:12333');

    // Storage section should be collapsed
    expect(screen.getByTestId('config-storage-toggle')).toHaveAttribute('aria-expanded', 'false');
  });

  it('expands storage section to show max_level option', async () => {
    const user = userEvent.setup();

    render(
      <CollapsibleSection title="Storage" testId="config-storage">
        <table data-testid="storage-options">
          <tbody>
            <tr data-testid="option-max_level">
              <td>max_level</td>
              <td data-testid="default-max_level">7</td>
              <td>Maximum number of LSM tree levels</td>
            </tr>
          </tbody>
        </table>
      </CollapsibleSection>
    );

    const toggle = screen.getByTestId('config-storage-toggle');

    // Click to expand
    await user.click(toggle);

    // Verify content is visible
    expect(toggle).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByTestId('option-max_level')).toBeInTheDocument();
    expect(screen.getByTestId('default-max_level')).toHaveTextContent('7');
    expect(screen.getByText(/LSM tree levels/)).toBeInTheDocument();
  });
});

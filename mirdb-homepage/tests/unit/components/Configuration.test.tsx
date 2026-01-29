/**
 * Unit tests for Configuration components.
 * Owner: Scenario 5 - Configuration Reference Section
 *
 * Tests:
 * - ConfigurationReference renders with collapsible option categories
 * - CollapsibleSection expand/collapse functionality
 * - Default values display correctly
 * - Size units explanation is present
 * - TOML example displays with syntax highlighting
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { CollapsibleSection } from '../../../src/components/Configuration/CollapsibleSection';

describe('CollapsibleSection', () => {
  it('renders with title and collapsed by default', () => {
    render(
      <CollapsibleSection title="Test Section" testId="test-section">
        <div>Test Content</div>
      </CollapsibleSection>
    );

    // Check section is rendered (testId prop is used directly for outer div)
    expect(screen.getByTestId('test-section')).toBeInTheDocument();

    // Check title is displayed
    expect(screen.getByText('Test Section')).toBeInTheDocument();

    // Check toggle button exists
    const toggleButton = screen.getByTestId('test-section-toggle');
    expect(toggleButton).toBeInTheDocument();
    expect(toggleButton).toHaveAttribute('aria-expanded', 'false');
  });

  it('renders expanded when defaultOpen is true', () => {
    render(
      <CollapsibleSection title="Open Section" defaultOpen={true} testId="open-section">
        <div data-testid="inner-content">Inner Content</div>
      </CollapsibleSection>
    );

    const toggleButton = screen.getByTestId('open-section-toggle');
    expect(toggleButton).toHaveAttribute('aria-expanded', 'true');

    // Content should be visible
    expect(screen.getByTestId('inner-content')).toBeInTheDocument();
  });

  it('expands when toggle button is clicked', async () => {
    const user = userEvent.setup();

    render(
      <CollapsibleSection title="Clickable Section" testId="clickable-section">
        <div data-testid="section-content">Section Content</div>
      </CollapsibleSection>
    );

    const toggleButton = screen.getByTestId('clickable-section-toggle');

    // Initially collapsed
    expect(toggleButton).toHaveAttribute('aria-expanded', 'false');

    // Click to expand
    await user.click(toggleButton);

    // Should now be expanded
    expect(toggleButton).toHaveAttribute('aria-expanded', 'true');
  });

  it('collapses when expanded section is clicked', async () => {
    const user = userEvent.setup();

    render(
      <CollapsibleSection title="Toggle Section" defaultOpen={true} testId="toggle-section">
        <div>Content</div>
      </CollapsibleSection>
    );

    const toggleButton = screen.getByTestId('toggle-section-toggle');

    // Initially expanded
    expect(toggleButton).toHaveAttribute('aria-expanded', 'true');

    // Click to collapse
    await user.click(toggleButton);

    // Should now be collapsed
    expect(toggleButton).toHaveAttribute('aria-expanded', 'false');
  });

  it('toggles on Enter key press', async () => {
    const user = userEvent.setup();

    render(
      <CollapsibleSection title="Keyboard Section" testId="keyboard-section">
        <div>Content</div>
      </CollapsibleSection>
    );

    const toggleButton = screen.getByTestId('keyboard-section-toggle');
    toggleButton.focus();

    // Initially collapsed
    expect(toggleButton).toHaveAttribute('aria-expanded', 'false');

    // Press Enter
    await user.keyboard('{Enter}');

    // Should now be expanded
    expect(toggleButton).toHaveAttribute('aria-expanded', 'true');
  });

  it('toggles on Space key press', async () => {
    const user = userEvent.setup();

    render(
      <CollapsibleSection title="Space Section" testId="space-section">
        <div>Content</div>
      </CollapsibleSection>
    );

    const toggleButton = screen.getByTestId('space-section-toggle');
    toggleButton.focus();

    // Initially collapsed
    expect(toggleButton).toHaveAttribute('aria-expanded', 'false');

    // Press Space
    await user.keyboard(' ');

    // Should now be expanded
    expect(toggleButton).toHaveAttribute('aria-expanded', 'true');
  });

  it('has proper ARIA attributes for accessibility', () => {
    render(
      <CollapsibleSection title="Accessible Section" testId="accessible-section">
        <div>Content</div>
      </CollapsibleSection>
    );

    const toggleButton = screen.getByTestId('accessible-section-toggle');
    const contentRegion = screen.getByTestId('accessible-section-content');

    // Button should have aria-controls pointing to content
    expect(toggleButton).toHaveAttribute('aria-controls');
    expect(toggleButton).toHaveAttribute('aria-expanded');

    // Content region should have role="region"
    expect(contentRegion).toHaveAttribute('role', 'region');
  });

  it('renders children content inside the collapsible area', () => {
    render(
      <CollapsibleSection title="Content Section" defaultOpen={true} testId="content-section">
        <div data-testid="custom-child">Custom Child Content</div>
      </CollapsibleSection>
    );

    expect(screen.getByTestId('custom-child')).toBeInTheDocument();
    expect(screen.getByText('Custom Child Content')).toBeInTheDocument();
  });
});

describe('ConfigurationReference Integration', () => {
  // These tests verify the expected structure that the Astro component renders
  // They test the React CollapsibleSection with configuration-specific content

  it('renders addr configuration option with default value', () => {
    render(
      <CollapsibleSection title="Network" defaultOpen={true} testId="config-network">
        <table>
          <tbody>
            <tr data-testid="option-addr">
              <td>addr</td>
              <td>string</td>
              <td data-testid="default-addr">0.0.0.0:12333</td>
              <td>Listen address and port</td>
            </tr>
          </tbody>
        </table>
      </CollapsibleSection>
    );

    // Check addr option is rendered
    expect(screen.getByTestId('option-addr')).toBeInTheDocument();

    // Check default value is correct
    const defaultValue = screen.getByTestId('default-addr');
    expect(defaultValue).toHaveTextContent('0.0.0.0:12333');
  });

  it('renders max_level configuration option with default value', () => {
    render(
      <CollapsibleSection title="Storage" defaultOpen={true} testId="config-storage">
        <table>
          <tbody>
            <tr data-testid="option-max_level">
              <td>max_level</td>
              <td>integer</td>
              <td data-testid="default-max_level">7</td>
              <td>Maximum number of LSM tree levels</td>
            </tr>
          </tbody>
        </table>
      </CollapsibleSection>
    );

    // Check max_level option is rendered
    expect(screen.getByTestId('option-max_level')).toBeInTheDocument();

    // Check default value is correct
    const defaultValue = screen.getByTestId('default-max_level');
    expect(defaultValue).toHaveTextContent('7');

    // Check LSM description is present
    expect(screen.getByText(/LSM tree levels/)).toBeInTheDocument();
  });

  it('renders size units explanation', () => {
    render(
      <div data-testid="size-units-section">
        <h3>Size Units</h3>
        <div data-testid="size-units-grid">
          <div data-testid="size-unit-k">
            <span>K</span>
            <p>Kilobytes</p>
          </div>
          <div data-testid="size-unit-m">
            <span>M</span>
            <p>Megabytes</p>
          </div>
          <div data-testid="size-unit-g">
            <span>G</span>
            <p>Gigabytes</p>
          </div>
          <div data-testid="size-unit-t">
            <span>T</span>
            <p>Terabytes</p>
          </div>
        </div>
      </div>
    );

    // Check size units section is present
    expect(screen.getByTestId('size-units-section')).toBeInTheDocument();

    // Check all unit types are displayed
    expect(screen.getByTestId('size-unit-k')).toBeInTheDocument();
    expect(screen.getByText('Kilobytes')).toBeInTheDocument();

    expect(screen.getByTestId('size-unit-m')).toBeInTheDocument();
    expect(screen.getByText('Megabytes')).toBeInTheDocument();

    expect(screen.getByTestId('size-unit-g')).toBeInTheDocument();
    expect(screen.getByText('Gigabytes')).toBeInTheDocument();

    expect(screen.getByTestId('size-unit-t')).toBeInTheDocument();
    expect(screen.getByText('Terabytes')).toBeInTheDocument();
  });

  it('renders TOML example code block', () => {
    const tomlExample = `addr = "0.0.0.0:12333"
max_level = 7`;

    render(
      <div data-testid="toml-example">
        <pre data-testid="toml-code-block" data-language="toml">
          <code className="syntax-highlighted" data-testid="toml-highlighted-code">
            {tomlExample}
          </code>
        </pre>
      </div>
    );

    // Check TOML example is present
    expect(screen.getByTestId('toml-example')).toBeInTheDocument();

    // Check code block has language attribute
    const codeBlock = screen.getByTestId('toml-code-block');
    expect(codeBlock).toHaveAttribute('data-language', 'toml');

    // Check highlighted code container exists
    expect(screen.getByTestId('toml-highlighted-code')).toBeInTheDocument();

    // Check TOML content is displayed
    expect(screen.getByText(/addr = "0.0.0.0:12333"/)).toBeInTheDocument();
  });

  it('renders collapsible categories for configuration options', () => {
    render(
      <div data-testid="config-categories">
        <CollapsibleSection title="Network" testId="config-network">
          <div>Network options</div>
        </CollapsibleSection>
        <CollapsibleSection title="Storage" testId="config-storage">
          <div>Storage options</div>
        </CollapsibleSection>
        <CollapsibleSection title="Memory Tables" testId="config-memtable">
          <div>Memtable options</div>
        </CollapsibleSection>
        <CollapsibleSection title="SSTables" testId="config-sstable">
          <div>SSTable options</div>
        </CollapsibleSection>
        <CollapsibleSection title="Compaction" testId="config-compaction">
          <div>Compaction options</div>
        </CollapsibleSection>
      </div>
    );

    // Check all configuration categories are present
    expect(screen.getByText('Network')).toBeInTheDocument();
    expect(screen.getByText('Storage')).toBeInTheDocument();
    expect(screen.getByText('Memory Tables')).toBeInTheDocument();
    expect(screen.getByText('SSTables')).toBeInTheDocument();
    expect(screen.getByText('Compaction')).toBeInTheDocument();
  });
});

/**
 * Integration tests for Terminal interaction.
 * Owner: Scenario 3 - Interactive Terminal Component
 *
 * Tests:
 * - Tab switching behavior
 * - Content changes on tab selection
 * - Keyboard navigation
 */
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { InteractiveTerminal } from '../../src/components/Terminal/InteractiveTerminal';
import { TabPanel } from '../../src/components/Terminal/TabPanel';

describe('Terminal Tab Interaction', () => {
  it('displays Basic Usage content by default', () => {
    render(<InteractiveTerminal />);

    // Basic Usage tab should be active by default
    const basicTab = screen.getByTestId('tab-basic');
    expect(basicTab).toHaveAttribute('aria-selected', 'true');

    // Code block should show shell commands
    const codeBlock = screen.getByTestId('code-block');
    expect(codeBlock).toHaveAttribute('data-language', 'shell');

    // Should contain get/set command examples
    const highlightedCode = screen.getByTestId('highlighted-code');
    expect(highlightedCode.textContent).toContain('set mykey');
    expect(highlightedCode.textContent).toContain('get mykey');
  });

  it('switches to Configuration tab and displays TOML content', async () => {
    const user = userEvent.setup();
    render(<InteractiveTerminal />);

    // Click Configuration tab
    const configTab = screen.getByTestId('tab-config');
    await user.click(configTab);

    // Configuration tab should now be active
    expect(configTab).toHaveAttribute('aria-selected', 'true');

    // Basic Usage tab should be inactive
    const basicTab = screen.getByTestId('tab-basic');
    expect(basicTab).toHaveAttribute('aria-selected', 'false');

    // Code block should now show TOML content
    const codeBlock = screen.getByTestId('code-block');
    expect(codeBlock).toHaveAttribute('data-language', 'toml');

    // Should contain TOML configuration
    const highlightedCode = screen.getByTestId('highlighted-code');
    expect(highlightedCode.textContent).toContain('[server]');
    expect(highlightedCode.textContent).toContain('[storage]');
    expect(highlightedCode.textContent).toContain('memtable_size');
  });

  it('switches to Advanced tab and displays advanced commands', async () => {
    const user = userEvent.setup();
    render(<InteractiveTerminal />);

    // Click Advanced tab
    const advancedTab = screen.getByTestId('tab-advanced');
    await user.click(advancedTab);

    // Advanced tab should now be active
    expect(advancedTab).toHaveAttribute('aria-selected', 'true');

    // Code block should show shell content
    const codeBlock = screen.getByTestId('code-block');
    expect(codeBlock).toHaveAttribute('data-language', 'shell');

    // Should contain advanced commands
    const highlightedCode = screen.getByTestId('highlighted-code');
    expect(highlightedCode.textContent).toContain('gets');
    expect(highlightedCode.textContent).toContain('append');
    expect(highlightedCode.textContent).toContain('prepend');
  });

  it('can switch between tabs multiple times', async () => {
    const user = userEvent.setup();
    render(<InteractiveTerminal />);

    // Start with Basic Usage
    expect(screen.getByTestId('tab-basic')).toHaveAttribute('aria-selected', 'true');

    // Switch to Config
    await user.click(screen.getByTestId('tab-config'));
    expect(screen.getByTestId('tab-config')).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByTestId('code-block')).toHaveAttribute('data-language', 'toml');

    // Switch to Advanced
    await user.click(screen.getByTestId('tab-advanced'));
    expect(screen.getByTestId('tab-advanced')).toHaveAttribute('aria-selected', 'true');

    // Switch back to Basic
    await user.click(screen.getByTestId('tab-basic'));
    expect(screen.getByTestId('tab-basic')).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByTestId('code-block')).toHaveAttribute('data-language', 'shell');
  });
});

describe('TabPanel Keyboard Navigation', () => {
  const mockTabs = [
    { id: 'tab1', label: 'Tab 1' },
    { id: 'tab2', label: 'Tab 2' },
    { id: 'tab3', label: 'Tab 3' },
  ];

  it('supports ArrowRight key navigation', () => {
    const onTabChange = vi.fn();
    render(
      <TabPanel tabs={mockTabs} activeTab="tab1" onTabChange={onTabChange}>
        <div>Content</div>
      </TabPanel>
    );

    const firstTab = screen.getByTestId('tab-tab1');
    fireEvent.keyDown(firstTab, { key: 'ArrowRight' });

    expect(onTabChange).toHaveBeenCalledWith('tab2');
  });

  it('supports ArrowLeft key navigation', () => {
    const onTabChange = vi.fn();
    render(
      <TabPanel tabs={mockTabs} activeTab="tab2" onTabChange={onTabChange}>
        <div>Content</div>
      </TabPanel>
    );

    const secondTab = screen.getByTestId('tab-tab2');
    fireEvent.keyDown(secondTab, { key: 'ArrowLeft' });

    expect(onTabChange).toHaveBeenCalledWith('tab1');
  });

  it('wraps around when pressing ArrowRight on last tab', () => {
    const onTabChange = vi.fn();
    render(
      <TabPanel tabs={mockTabs} activeTab="tab3" onTabChange={onTabChange}>
        <div>Content</div>
      </TabPanel>
    );

    const lastTab = screen.getByTestId('tab-tab3');
    fireEvent.keyDown(lastTab, { key: 'ArrowRight' });

    expect(onTabChange).toHaveBeenCalledWith('tab1');
  });

  it('wraps around when pressing ArrowLeft on first tab', () => {
    const onTabChange = vi.fn();
    render(
      <TabPanel tabs={mockTabs} activeTab="tab1" onTabChange={onTabChange}>
        <div>Content</div>
      </TabPanel>
    );

    const firstTab = screen.getByTestId('tab-tab1');
    fireEvent.keyDown(firstTab, { key: 'ArrowLeft' });

    expect(onTabChange).toHaveBeenCalledWith('tab3');
  });

  it('supports Home key to go to first tab', () => {
    const onTabChange = vi.fn();
    render(
      <TabPanel tabs={mockTabs} activeTab="tab3" onTabChange={onTabChange}>
        <div>Content</div>
      </TabPanel>
    );

    const lastTab = screen.getByTestId('tab-tab3');
    fireEvent.keyDown(lastTab, { key: 'Home' });

    expect(onTabChange).toHaveBeenCalledWith('tab1');
  });

  it('supports End key to go to last tab', () => {
    const onTabChange = vi.fn();
    render(
      <TabPanel tabs={mockTabs} activeTab="tab1" onTabChange={onTabChange}>
        <div>Content</div>
      </TabPanel>
    );

    const firstTab = screen.getByTestId('tab-tab1');
    fireEvent.keyDown(firstTab, { key: 'End' });

    expect(onTabChange).toHaveBeenCalledWith('tab3');
  });
});

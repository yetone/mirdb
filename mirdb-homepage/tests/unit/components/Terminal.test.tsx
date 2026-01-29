/**
 * Unit tests for Terminal components.
 * Owner: Scenario 3 - Interactive Terminal Component
 *
 * Tests:
 * - InteractiveTerminal renders with all tabs
 * - TabPanel renders tabs and content
 * - CopyButton renders and handles clipboard
 * - Syntax highlighting is applied
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { InteractiveTerminal } from '../../../src/components/Terminal/InteractiveTerminal';
import { TabPanel } from '../../../src/components/Terminal/TabPanel';
import { CopyButton } from '../../../src/components/Terminal/CopyButton';

describe('InteractiveTerminal', () => {
  it('renders with tabs for Basic Usage, Configuration, and Advanced', () => {
    render(<InteractiveTerminal />);

    // Check that terminal container is rendered
    expect(screen.getByTestId('interactive-terminal')).toBeInTheDocument();

    // Check all three tabs are present
    expect(screen.getByTestId('tab-basic')).toBeInTheDocument();
    expect(screen.getByTestId('tab-config')).toBeInTheDocument();
    expect(screen.getByTestId('tab-advanced')).toBeInTheDocument();

    // Verify tab labels
    expect(screen.getByText('Basic Usage')).toBeInTheDocument();
    expect(screen.getByText('Configuration')).toBeInTheDocument();
    expect(screen.getByText('Advanced')).toBeInTheDocument();
  });

  it('renders code block with syntax highlighting', () => {
    render(<InteractiveTerminal />);

    // Check code block is rendered
    const codeBlock = screen.getByTestId('code-block');
    expect(codeBlock).toBeInTheDocument();
    expect(codeBlock).toHaveAttribute('data-language', 'shell');

    // Check highlighted code is present
    const highlightedCode = screen.getByTestId('highlighted-code');
    expect(highlightedCode).toBeInTheDocument();
    expect(highlightedCode.className).toContain('syntax-highlighted');
  });

  it('renders terminal header with decorative dots', () => {
    render(<InteractiveTerminal />);

    // Terminal header should show mirdb terminal text
    expect(screen.getByText('mirdb terminal')).toBeInTheDocument();
  });

  it('has copy button rendered', () => {
    render(<InteractiveTerminal />);

    // Copy button should be present
    expect(screen.getByTestId('copy-button')).toBeInTheDocument();
  });
});

describe('TabPanel', () => {
  const mockTabs = [
    { id: 'tab1', label: 'Tab 1' },
    { id: 'tab2', label: 'Tab 2' },
    { id: 'tab3', label: 'Tab 3' },
  ];

  it('renders all tabs', () => {
    const onTabChange = vi.fn();
    render(
      <TabPanel tabs={mockTabs} activeTab="tab1" onTabChange={onTabChange}>
        <div>Content</div>
      </TabPanel>
    );

    expect(screen.getByText('Tab 1')).toBeInTheDocument();
    expect(screen.getByText('Tab 2')).toBeInTheDocument();
    expect(screen.getByText('Tab 3')).toBeInTheDocument();
  });

  it('marks the active tab correctly', () => {
    const onTabChange = vi.fn();
    render(
      <TabPanel tabs={mockTabs} activeTab="tab2" onTabChange={onTabChange}>
        <div>Content</div>
      </TabPanel>
    );

    const activeTab = screen.getByTestId('tab-tab2');
    expect(activeTab).toHaveAttribute('aria-selected', 'true');
    expect(activeTab).toHaveAttribute('tabindex', '0');

    const inactiveTab = screen.getByTestId('tab-tab1');
    expect(inactiveTab).toHaveAttribute('aria-selected', 'false');
    expect(inactiveTab).toHaveAttribute('tabindex', '-1');
  });

  it('renders children content', () => {
    const onTabChange = vi.fn();
    render(
      <TabPanel tabs={mockTabs} activeTab="tab1" onTabChange={onTabChange}>
        <div data-testid="panel-content">Test Content</div>
      </TabPanel>
    );

    expect(screen.getByTestId('panel-content')).toBeInTheDocument();
    expect(screen.getByText('Test Content')).toBeInTheDocument();
  });

  it('has proper ARIA attributes for accessibility', () => {
    const onTabChange = vi.fn();
    render(
      <TabPanel tabs={mockTabs} activeTab="tab1" onTabChange={onTabChange}>
        <div>Content</div>
      </TabPanel>
    );

    // Check tablist role
    const tabList = screen.getByRole('tablist');
    expect(tabList).toHaveAttribute('aria-label', 'Terminal examples');

    // Check tab roles
    const tabs = screen.getAllByRole('tab');
    expect(tabs).toHaveLength(3);

    // Check tabpanel role
    const tabPanel = screen.getByRole('tabpanel');
    expect(tabPanel).toHaveAttribute('aria-labelledby', 'tab-tab1');
  });
});

describe('CopyButton', () => {
  let originalClipboard: Clipboard;
  let mockWriteText: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockWriteText = vi.fn().mockResolvedValue(undefined);
    originalClipboard = navigator.clipboard;

    Object.defineProperty(navigator, 'clipboard', {
      value: {
        writeText: mockWriteText,
      },
      writable: true,
      configurable: true,
    });
  });

  afterEach(() => {
    Object.defineProperty(navigator, 'clipboard', {
      value: originalClipboard,
      writable: true,
      configurable: true,
    });
  });

  it('renders copy button with correct label', () => {
    render(<CopyButton text="test code" />);

    const button = screen.getByTestId('copy-button');
    expect(button).toBeInTheDocument();
    expect(button).toHaveAttribute('aria-label', 'Copy to clipboard');
    expect(screen.getByText('Copy')).toBeInTheDocument();
  });

  it('has proper button type attribute', () => {
    render(<CopyButton text="test code" />);

    const button = screen.getByTestId('copy-button');
    expect(button).toHaveAttribute('type', 'button');
  });
});

describe('Syntax Highlighting', () => {
  it('applies shell syntax highlighting', () => {
    render(<InteractiveTerminal />);

    // The highlighted code should contain span elements for syntax highlighting
    const highlightedCode = screen.getByTestId('highlighted-code');
    expect(highlightedCode.innerHTML).toContain('<span');
    expect(highlightedCode.innerHTML).toContain('class=');
  });

  it('code block has language data attribute', () => {
    render(<InteractiveTerminal />);

    const codeBlock = screen.getByTestId('code-block');
    expect(codeBlock).toHaveAttribute('data-language');
  });
});

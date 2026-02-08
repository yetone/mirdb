/**
 * Installation Component Unit Tests
 * Owner: Scenario 3 - Installation Instructions
 *
 * Test cases:
 * 1. Component renders pre/code elements with installation commands
 * 2. Copy-to-clipboard button is present for each code block
 * 5. Code snippets contain recognizable installation commands (cargo, git clone, etc.)
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { Installation } from './Installation';
import { INSTALLATION_STEPS } from '../../utils/constants';

describe('Installation Component', () => {
  beforeEach(() => {
    // Reset clipboard mock before each test
    vi.clearAllMocks();
  });

  // Test Case 1: Component renders pre/code elements with installation commands
  it('renders code blocks with installation commands', () => {
    const { container } = render(<Installation />);

    // Check for pre elements using querySelector
    const preElements = container.querySelectorAll('pre');
    expect(preElements.length).toBeGreaterThan(0);

    // Check for code elements inside pre
    const codeElements = container.querySelectorAll('pre code');
    expect(codeElements.length).toBeGreaterThan(0);

    // Check that each installation step command is displayed
    INSTALLATION_STEPS.forEach((step) => {
      expect(screen.getByText(step.command)).toBeInTheDocument();
    });
  });

  // Test Case 2: Copy-to-clipboard button is present for each code block
  it('renders a copy button for each code block', () => {
    render(<Installation />);

    // Find all copy buttons
    const copyButtons = screen.getAllByRole('button', { name: /copy/i });

    // Should have one copy button per installation step
    expect(copyButtons.length).toBe(INSTALLATION_STEPS.length);
  });

  // Test Case 5: Code snippets contain recognizable installation commands
  it('displays valid installation commands including git clone and cargo', () => {
    render(<Installation />);

    // Check for git clone command
    expect(screen.getByText(/git clone/)).toBeInTheDocument();

    // Check for cargo build command
    expect(screen.getByText(/cargo build/)).toBeInTheDocument();
  });

  it('renders the Installation section with proper heading', () => {
    render(<Installation />);

    // Check for section heading
    expect(screen.getByRole('heading', { name: /installation/i })).toBeInTheDocument();
  });

  it('renders step numbers for each installation step', () => {
    render(<Installation />);

    // Check for step numbers
    expect(screen.getByText('1')).toBeInTheDocument();
    expect(screen.getByText('2')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();
  });

  it('displays step labels for each installation step', () => {
    render(<Installation />);

    INSTALLATION_STEPS.forEach((step) => {
      expect(screen.getByText(step.label)).toBeInTheDocument();
    });
  });

  it('shows "Copied!" feedback when copy button is clicked', async () => {
    render(<Installation />);

    // Get the first copy button
    const copyButtons = screen.getAllByRole('button', { name: /copy/i });
    const firstCopyButton = copyButtons[0];

    // Click the copy button
    fireEvent.click(firstCopyButton);

    // Wait for the "Copied!" text to appear
    await waitFor(() => {
      expect(screen.getByText('Copied!')).toBeInTheDocument();
    });
  });

  it('has accessible aria-label on copy buttons', () => {
    render(<Installation />);

    const copyButtons = screen.getAllByRole('button', { name: /copy to clipboard/i });
    expect(copyButtons.length).toBe(INSTALLATION_STEPS.length);
  });

  it('renders installation section with proper id for navigation', () => {
    render(<Installation />);

    const section = document.getElementById('installation');
    expect(section).toBeInTheDocument();
  });
});

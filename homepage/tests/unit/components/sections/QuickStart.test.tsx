/**
 * Unit tests for QuickStart component.
 * Owner: Scenario 5 - Quick Start Section with Code Examples
 */

import React from 'react';
import { render, screen, within } from '@testing-library/react';
import { QuickStart } from '@/components/sections/QuickStart';

// Mock the clipboard API
const mockWriteText = jest.fn();
Object.assign(navigator, {
  clipboard: {
    writeText: mockWriteText,
  },
});

// Helper function to check if text content contains a substring
// (handles text broken up by syntax highlighting spans)
const hasTextContent = (container: HTMLElement, text: string | RegExp): boolean => {
  const textContent = container.textContent || '';
  if (typeof text === 'string') {
    return textContent.includes(text);
  }
  return text.test(textContent);
};

describe('QuickStart Component', () => {
  beforeEach(() => {
    mockWriteText.mockReset();
    mockWriteText.mockResolvedValue(undefined);
  });

  // Test Case 1: Prerequisites section displays 'Rust toolchain' requirement
  describe('TC1: Prerequisites section displays Rust toolchain requirement', () => {
    it('should display "Rust toolchain" in prerequisites list', () => {
      render(<QuickStart />);

      expect(screen.getByText('Rust toolchain')).toBeInTheDocument();
    });

    it('should render prerequisites section', () => {
      render(<QuickStart />);

      const prerequisitesSection = screen.getByTestId('prerequisites-section');
      expect(prerequisitesSection).toBeInTheDocument();
    });

    it('should display prerequisites heading', () => {
      render(<QuickStart />);

      expect(screen.getByText('Prerequisites')).toBeInTheDocument();
    });

    it('should display Git as a prerequisite', () => {
      render(<QuickStart />);

      expect(screen.getByText('Git')).toBeInTheDocument();
    });

    it('should display prerequisite items with checkmarks', () => {
      render(<QuickStart />);

      const prerequisiteItems = screen.getAllByTestId('prerequisite-item');
      expect(prerequisiteItems.length).toBeGreaterThanOrEqual(2);
    });

    it('should have a link to rustup.rs for Rust toolchain', () => {
      render(<QuickStart />);

      const rustupLink = screen.getByRole('link', { name: /rustup\.rs/i });
      expect(rustupLink).toHaveAttribute('href', 'https://rustup.rs/');
    });
  });

  // Test Case 2: Git clone command display
  describe('TC2: Git clone command is displayed', () => {
    it('should display the git clone command', () => {
      render(<QuickStart />);

      const stepClone = screen.getByTestId('step-clone');
      expect(hasTextContent(stepClone, 'git clone https://github.com/yetone/mirdb.git')).toBe(true);
    });

    it('should render installation section', () => {
      render(<QuickStart />);

      const installationSection = screen.getByTestId('installation-section');
      expect(installationSection).toBeInTheDocument();
    });

    it('should display step 1 description', () => {
      render(<QuickStart />);

      expect(screen.getByText(/1\. Clone the repository/i)).toBeInTheDocument();
    });
  });

  // Test Case 3: Cargo run command is displayed
  describe('TC3: Cargo run command is displayed', () => {
    it('should display the cargo run command', () => {
      render(<QuickStart />);

      const stepRun = screen.getByTestId('step-run');
      expect(hasTextContent(stepRun, 'cargo run --release')).toBe(true);
    });

    it('should display step 3 description for building', () => {
      render(<QuickStart />);

      expect(screen.getByText(/3\. Build and run MirDB/i)).toBeInTheDocument();
    });
  });

  // Test Case 5: Memcached client connection example
  describe('TC5: Memcached client connection example', () => {
    it('should display memcached connection example', () => {
      render(<QuickStart />);

      const usageSection = screen.getByTestId('usage-section');
      expect(hasTextContent(usageSection, 'telnet localhost 11211')).toBe(true);
    });

    it('should display usage section', () => {
      render(<QuickStart />);

      const usageSection = screen.getByTestId('usage-section');
      expect(usageSection).toBeInTheDocument();
    });

    it('should display heading about memcached protocol', () => {
      render(<QuickStart />);

      expect(screen.getByText(/Connect Using Memcached Protocol/i)).toBeInTheDocument();
    });

    it('should display set command example', () => {
      render(<QuickStart />);

      const usageSection = screen.getByTestId('usage-section');
      expect(hasTextContent(usageSection, 'set mykey')).toBe(true);
    });

    it('should display get command example', () => {
      render(<QuickStart />);

      const usageSection = screen.getByTestId('usage-section');
      expect(hasTextContent(usageSection, 'get mykey')).toBe(true);
    });

    it('should explain memcached compatibility', () => {
      render(<QuickStart />);

      expect(
        screen.getByText(/MirDB is compatible with standard memcached clients/i)
      ).toBeInTheDocument();
    });
  });

  // Test for section structure and accessibility
  describe('Section structure and accessibility', () => {
    it('should render a section with id="quick-start"', () => {
      render(<QuickStart />);

      const section = document.getElementById('quick-start');
      expect(section).toBeInTheDocument();
    });

    it('should have proper aria-labelledby on the section', () => {
      render(<QuickStart />);

      const section = document.getElementById('quick-start');
      expect(section).toHaveAttribute('aria-labelledby', 'quick-start-heading');
    });

    it('should display Quick Start as section heading', () => {
      render(<QuickStart />);

      const heading = screen.getByRole('heading', { name: 'Quick Start', level: 2 });
      expect(heading).toBeInTheDocument();
    });

    it('should display section description', () => {
      render(<QuickStart />);

      expect(
        screen.getByText(/Get MirDB up and running in minutes/i)
      ).toBeInTheDocument();
    });

    it('should have prerequisites list with proper role', () => {
      render(<QuickStart />);

      const list = screen.getByRole('list', { name: /Prerequisites list/i });
      expect(list).toBeInTheDocument();
    });
  });

  // Test for code blocks
  describe('Code blocks', () => {
    it('should render multiple code blocks', () => {
      render(<QuickStart />);

      const codeBlocks = screen.getAllByTestId('code-block');
      expect(codeBlocks.length).toBeGreaterThanOrEqual(3);
    });

    it('should render copy buttons for code blocks', () => {
      render(<QuickStart />);

      const copyButtons = screen.getAllByTestId('copy-button');
      expect(copyButtons.length).toBeGreaterThanOrEqual(3);
    });
  });

  // Test for installation steps
  describe('Installation steps', () => {
    it('should display cd command', () => {
      render(<QuickStart />);

      const stepCd = screen.getByTestId('step-cd');
      expect(hasTextContent(stepCd, 'cd mirdb')).toBe(true);
    });

    it('should display step 2 description', () => {
      render(<QuickStart />);

      expect(
        screen.getByText(/2\. Navigate to the project directory/i)
      ).toBeInTheDocument();
    });

    it('should render step clone element', () => {
      render(<QuickStart />);

      expect(screen.getByTestId('step-clone')).toBeInTheDocument();
    });

    it('should render step cd element', () => {
      render(<QuickStart />);

      expect(screen.getByTestId('step-cd')).toBeInTheDocument();
    });

    it('should render step run element', () => {
      render(<QuickStart />);

      expect(screen.getByTestId('step-run')).toBeInTheDocument();
    });
  });
});

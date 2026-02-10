/**
 * Unit tests for UsageExample component
 * Owner: Scenario 3 - Usage Example Section
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { UsageExample } from '../../../../src/components/sections/UsageExample';

describe('UsageExample Component', () => {
  describe('Test Case 1: Terminal-style display or GIF is rendered', () => {
    it('should render the UsageExample section', () => {
      render(<UsageExample />);

      const section = screen.getByTestId('usage-example-section');
      expect(section).toBeInTheDocument();
    });

    it('should render a terminal-style display', () => {
      render(<UsageExample />);

      const terminal = screen.getByTestId('terminal-display');
      expect(terminal).toBeInTheDocument();
    });

    it('should have proper ARIA label for accessibility', () => {
      render(<UsageExample />);

      const terminal = screen.getByTestId('terminal-display');
      expect(terminal).toHaveAttribute('role', 'img');
      expect(terminal).toHaveAttribute('aria-label', 'Terminal demonstration of SET and GET commands');
    });
  });

  describe('Test Case 2: SET command example is visible', () => {
    it('should display the SET command in the terminal', () => {
      render(<UsageExample />);

      const setCommand = screen.getByTestId('set-command');
      expect(setCommand).toBeInTheDocument();
      expect(setCommand).toHaveTextContent('set mykey 0 0 5');
    });

    it('should show STORED response after SET command', () => {
      render(<UsageExample />);

      expect(screen.getByText('STORED')).toBeInTheDocument();
    });
  });

  describe('Test Case 3: GET command example is visible', () => {
    it('should display the GET command in the terminal', () => {
      render(<UsageExample />);

      const getCommand = screen.getByTestId('get-command');
      expect(getCommand).toBeInTheDocument();
      expect(getCommand).toHaveTextContent('get mykey');
    });

    it('should show VALUE response after GET command', () => {
      render(<UsageExample />);

      expect(screen.getByText(/VALUE mykey/)).toBeInTheDocument();
    });

    it('should show END marker after GET response', () => {
      render(<UsageExample />);

      expect(screen.getByText('END')).toBeInTheDocument();
    });
  });

  describe('Test Case 4: Usage GIF container is present', () => {
    it('should render the usage GIF container', () => {
      render(<UsageExample />);

      const gifContainer = screen.getByTestId('usage-gif-container');
      expect(gifContainer).toBeInTheDocument();
    });

    it('should render the usage GIF image element', () => {
      render(<UsageExample />);

      const gifImage = screen.getByTestId('usage-gif');
      expect(gifImage).toBeInTheDocument();
      expect(gifImage).toHaveAttribute('src', '/assets/images/usage.gif');
      expect(gifImage).toHaveAttribute('alt', 'MirDB usage demonstration showing SET and GET commands');
    });

    it('should have lazy loading enabled for the GIF', () => {
      render(<UsageExample />);

      const gifImage = screen.getByTestId('usage-gif');
      expect(gifImage).toHaveAttribute('loading', 'lazy');
    });
  });

  describe('Section structure and content', () => {
    it('should have the correct section id for navigation', () => {
      render(<UsageExample />);

      const section = document.getElementById('usage');
      expect(section).toBeInTheDocument();
    });

    it('should display the section title', () => {
      render(<UsageExample />);

      expect(screen.getByText('Usage Example')).toBeInTheDocument();
    });

    it('should display the section description', () => {
      render(<UsageExample />);

      expect(screen.getByText(/MirDB uses the Memcached protocol/)).toBeInTheDocument();
    });

    it('should have proper ARIA labeling for the section', () => {
      render(<UsageExample />);

      const section = screen.getByRole('region', { name: /usage example/i });
      expect(section).toBeInTheDocument();
    });
  });

  describe('Terminal styling elements', () => {
    it('should render terminal window buttons', () => {
      render(<UsageExample />);

      const terminal = screen.getByTestId('terminal-display');
      const closeButton = terminal.querySelector('.close');
      const minimizeButton = terminal.querySelector('.minimize');
      const maximizeButton = terminal.querySelector('.maximize');

      expect(closeButton).toBeInTheDocument();
      expect(minimizeButton).toBeInTheDocument();
      expect(maximizeButton).toBeInTheDocument();
    });

    it('should display terminal title', () => {
      render(<UsageExample />);

      expect(screen.getByText('Terminal - MirDB')).toBeInTheDocument();
    });

    it('should display telnet command for connection', () => {
      render(<UsageExample />);

      expect(screen.getByText('telnet 127.0.0.1 11211')).toBeInTheDocument();
    });

    it('should display connection success message', () => {
      render(<UsageExample />);

      expect(screen.getByText('Connected to localhost.')).toBeInTheDocument();
    });
  });
});

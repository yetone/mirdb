import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { UsageExample } from '../../../../src/components/sections/UsageExample';

// Mock prismjs
vi.mock('prismjs', () => ({
  default: {
    highlightElement: vi.fn(),
  },
}));

vi.mock('prismjs/components/prism-bash', () => ({}));

describe('UsageExample', () => {
  beforeEach(() => {
    Object.assign(navigator, {
      clipboard: {
        writeText: vi.fn().mockResolvedValue(undefined),
      },
    });
  });

  it('renders the Usage Example heading', () => {
    render(<UsageExample />);

    expect(screen.getByRole('heading', { name: /usage example/i })).toBeInTheDocument();
  });

  it('shows "set mykey" command example', () => {
    render(<UsageExample />);

    expect(screen.getByText(/set mykey/)).toBeInTheDocument();
  });

  it('shows "get mykey" command in code block', () => {
    render(<UsageExample />);

    expect(screen.getByText(/get mykey/)).toBeInTheDocument();
  });

  it('shows "telnet localhost 12333" connection example', () => {
    render(<UsageExample />);

    expect(screen.getByText(/telnet localhost 12333/)).toBeInTheDocument();
  });

  it('shows "delete mykey" command example', () => {
    render(<UsageExample />);

    expect(screen.getByText(/delete mykey/)).toBeInTheDocument();
  });

  it('renders section with correct id for navigation', () => {
    const { container } = render(<UsageExample />);

    const section = container.querySelector('#usage');
    expect(section).toBeInTheDocument();
  });

  it('includes copy button for code block', () => {
    render(<UsageExample />);

    const copyButton = screen.getByRole('button', { name: /copy/i });
    expect(copyButton).toBeInTheDocument();
  });

  it('displays description text about Memcached protocol', () => {
    render(<UsageExample />);

    expect(screen.getByText(/memcached text protocol/i)).toBeInTheDocument();
  });

  it('mentions the default port 12333', () => {
    render(<UsageExample />);

    expect(screen.getByText('12333')).toBeInTheDocument();
  });
});

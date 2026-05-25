/**
 * Unit tests for ARIA attributes on dynamic content.
 * Owner: Scenario 18 - Performance and Accessibility
 *
 * Validates ARIA labels, live regions, form labels, and landmarks.
 */

import { describe, it, expect } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';
import ConnectionStatus from '../../../src/components/connection-status/ConnectionStatus';
import MetricCard from '../../../src/components/metrics/MetricCard';
import Layout from '../../../src/components/layout/Layout';
import HeroSection from '../../../src/components/hero/HeroSection';
import KVExplorer from '../../../src/components/kv-explorer/KVExplorer';
import SystemOverview from '../../../src/components/system-overview/SystemOverview';
import LSMTreeVisualization from '../../../src/components/lsm-tree/LSMTreeVisualization';
import InteractiveDemo from '../../../src/components/interactive-demo/InteractiveDemo';

describe('ARIA Attributes - Connection Status', () => {
  it('has aria-live="polite" for dynamic status updates', () => {
    render(<ConnectionStatus state="connected" />);

    const status = screen.getByTestId('connection-status');
    expect(status).toHaveAttribute('role', 'status');
    expect(status).toHaveAttribute('aria-live', 'polite');
  });

  it('has appropriate aria-label for each state', () => {
    const { rerender } = render(<ConnectionStatus state="connected" />);

    let status = screen.getByTestId('connection-status');
    expect(status).toHaveAttribute('aria-label', 'MirDB server is connected');

    rerender(<ConnectionStatus state="connecting" />);
    status = screen.getByTestId('connection-status');
    expect(status).toHaveAttribute('aria-label', 'MirDB server is connecting');

    rerender(<ConnectionStatus state="disconnected" />);
    status = screen.getByTestId('connection-status');
    expect(status).toHaveAttribute('aria-label', 'MirDB server is disconnected');
  });
});

describe('ARIA Attributes - Metric Cards', () => {
  it('renders with proper test IDs for accessibility', () => {
    render(<MetricCard label="Test Metric" value="42" testId="test-metric" />);

    const card = screen.getByTestId('test-metric');
    expect(card).toBeInTheDocument();

    const label = screen.getByTestId('test-metric-label');
    expect(label).toHaveTextContent('Test Metric');

    const value = screen.getByTestId('test-metric-value');
    expect(value).toHaveTextContent('42');
  });
});

describe('ARIA Attributes - Layout Landmarks', () => {
  it('renders with main landmark', () => {
    render(
      <ThemeProvider>
        <Layout>
          <div data-testid="test-content">Content</div>
        </Layout>
      </ThemeProvider>
    );

    const main = screen.getByTestId('layout-root');
    expect(main).toBeInTheDocument();
  });

  it('renders footer with contentinfo role', () => {
    render(
      <ThemeProvider>
        <Layout>
          <div>Content</div>
        </Layout>
      </ThemeProvider>
    );

    const footer = screen.getByTestId('layout-footer');
    expect(footer).toBeInTheDocument();
  });
});

describe('ARIA Attributes - Hero Section', () => {
  it('has proper aria-label on hero section', () => {
    render(<HeroSection />);

    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toHaveAttribute('aria-label', 'Hero');
  });

  it('has aria-label on external resources nav', () => {
    render(<HeroSection />);

    const linksNav = screen.getByTestId('hero-links');
    expect(linksNav).toHaveAttribute('aria-label', 'External resources');
  });

  it('CTA button has accessible text', () => {
    render(<HeroSection />);

    const ctaButton = screen.getByTestId('hero-cta-button');
    expect(ctaButton).toHaveTextContent('Get Started Now');
  });
});

describe('ARIA Attributes - KV Explorer Form', () => {
  it('has associated label for key input', () => {
    render(<KVExplorer />);

    const input = screen.getByTestId('kv-key-input');
    const id = input.getAttribute('id');
    expect(id).toBeTruthy();

    // The label should have htmlFor matching the input id
    const label = document.querySelector(`label[for="${id}"]`);
    expect(label).not.toBeNull();
    expect(label).toHaveTextContent('Key');
  });

  it('has aria-invalid and aria-describedby for validation errors', async () => {
    render(<KVExplorer />);

    const input = screen.getByTestId('kv-key-input');
    expect(input).toHaveAttribute('aria-invalid', 'false');

    // Submit without entering a key to trigger validation error
    const getButton = screen.getByTestId('kv-get-button');
    getButton.click();

    // After clicking Get without a key, aria-invalid should be true
    await waitFor(() => {
      expect(input).toHaveAttribute('aria-invalid', 'true');
    });
    expect(input).toHaveAttribute('aria-describedby', 'kv-key-error');

    const errorMessage = screen.getByTestId('kv-key-error');
    expect(errorMessage).toHaveAttribute('role', 'alert');
  });

  it('has aria-label on the explorer section', () => {
    render(<KVExplorer />);

    const section = screen.getByTestId('kv-explorer');
    expect(section).toHaveAttribute('aria-label', 'Key-Value Explorer');
  });
});

describe('ARIA Attributes - LSM Tree Visualization', () => {
  it('has aria-label on the section', () => {
    const state = {
      memtable: { key_count: 10, size_bytes: 1024 },
      immutable_memtable: null,
      levels: [{ level: 0, file_count: 2, total_size_bytes: 2048 }],
    };

    render(<LSMTreeVisualization state={state} />);

    const section = screen.getByTestId('lsm-tree-section');
    expect(section).toHaveAttribute('aria-label', 'LSM Tree Visualization');
  });

  it('SVG has role="img" and aria-label', () => {
    const state = {
      memtable: { key_count: 10, size_bytes: 1024 },
      immutable_memtable: null,
      levels: [{ level: 0, file_count: 2, total_size_bytes: 2048 }],
    };

    render(<LSMTreeVisualization state={state} />);

    const svg = screen.getByTestId('lsm-tree-svg');
    expect(svg).toHaveAttribute('role', 'img');
    expect(svg).toHaveAttribute('aria-label');
    expect(svg.getAttribute('aria-label')).toContain('LSM tree');
  });
});

describe('ARIA Attributes - System Overview', () => {
  it('has aria-label on the section', () => {
    const config = {
      listen_address: '0.0.0.0:12333',
      max_lsm_levels: 7,
      work_directory: '/tmp/mirdbs',
      sstable_size_mb: 100,
      memtable_size_mb: 4,
    };

    render(<SystemOverview config={config} />);

    const section = screen.getByTestId('system-overview-section');
    expect(section).toHaveAttribute('aria-label', 'System Overview');
  });

  it('uses description list (dl, dt, dd) for config items', () => {
    const config = {
      listen_address: '0.0.0.0:12333',
      max_lsm_levels: 7,
      work_directory: '/tmp/mirdbs',
      sstable_size_mb: 100,
      memtable_size_mb: 4,
    };

    render(<SystemOverview config={config} />);

    const list = screen.getByTestId('system-overview-list');
    expect(list.tagName.toLowerCase()).toBe('dl');
  });
});

describe('ARIA Attributes - Interactive Demo', () => {
  it('has aria-label on the section', () => {
    render(<InteractiveDemo />);

    const section = screen.getByTestId('interactive-demo');
    expect(section).toHaveAttribute('aria-label', 'Interactive Memcached Demo');
  });

  it('command input has aria-label', () => {
    render(<InteractiveDemo />);

    const commandInput = screen.getByTestId('command-input');
    expect(commandInput).toHaveAttribute('aria-label', 'Memcached command input');
  });

  it('output terminal has aria-live="polite"', () => {
    render(<InteractiveDemo />);

    const terminal = screen.getByTestId('output-terminal');
    expect(terminal).toHaveAttribute('role', 'log');
    expect(terminal).toHaveAttribute('aria-live', 'polite');
  });
});

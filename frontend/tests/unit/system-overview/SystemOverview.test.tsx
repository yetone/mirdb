/**
 * Unit and integration tests for SystemOverview component.
 * Covers REQ-4 (System Overview panel with config display and tooltips).
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import SystemOverview, { CONFIG_ITEMS } from '../../../src/components/system-overview/SystemOverview';
import { getConfig } from '../../../src/api/client';
import type { ServerConfig } from '../../../src/types';

const MOCK_CONFIG: ServerConfig = {
  listen_address: '0.0.0.0:12333',
  max_lsm_levels: 7,
  work_directory: '/tmp/mirdbs',
  sstable_size_mb: 100,
  memtable_size_mb: 4,
};

describe('SystemOverview', () => {
  it('renders the system overview section container', () => {
    render(<SystemOverview config={MOCK_CONFIG} />);
    expect(screen.getByTestId('system-overview-section')).toBeInTheDocument();
  });

  it('renders the section heading', () => {
    render(<SystemOverview config={MOCK_CONFIG} />);
    expect(screen.getByTestId('system-overview-heading')).toHaveTextContent('System Overview');
  });

  it('renders all 5 config rows', () => {
    render(<SystemOverview config={MOCK_CONFIG} />);
    const list = screen.getByTestId('system-overview-list');
    expect(list.children).toHaveLength(5);
  });

  it('renders the listen address with correct label and value', () => {
    render(<SystemOverview config={MOCK_CONFIG} />);
    expect(screen.getByTestId('system-overview-label-listen_address')).toHaveTextContent('Listen Address');
    expect(screen.getByTestId('system-overview-value-listen_address')).toHaveTextContent('0.0.0.0:12333');
  });

  it('renders the max LSM levels with correct label and value', () => {
    render(<SystemOverview config={MOCK_CONFIG} />);
    expect(screen.getByTestId('system-overview-label-max_lsm_levels')).toHaveTextContent('Max LSM Levels');
    expect(screen.getByTestId('system-overview-value-max_lsm_levels')).toHaveTextContent('7');
  });

  it('renders the work directory with correct label and value', () => {
    render(<SystemOverview config={MOCK_CONFIG} />);
    expect(screen.getByTestId('system-overview-label-work_directory')).toHaveTextContent('Work Directory');
    expect(screen.getByTestId('system-overview-value-work_directory')).toHaveTextContent('/tmp/mirdbs');
  });

  it('renders the SSTable max size with correct label and formatted value', () => {
    render(<SystemOverview config={MOCK_CONFIG} />);
    expect(screen.getByTestId('system-overview-label-sstable_size_mb')).toHaveTextContent('SSTable Max Size');
    expect(screen.getByTestId('system-overview-value-sstable_size_mb')).toHaveTextContent('100 MB');
  });

  it('renders the memtable max size with correct label and formatted value', () => {
    render(<SystemOverview config={MOCK_CONFIG} />);
    expect(screen.getByTestId('system-overview-label-memtable_size_mb')).toHaveTextContent('Memtable Max Size');
    expect(screen.getByTestId('system-overview-value-memtable_size_mb')).toHaveTextContent('4 MB');
  });

  it('displays exact string values without modification for address and work directory', () => {
    const customConfig: ServerConfig = {
      listen_address: '127.0.0.1:9999',
      max_lsm_levels: 3,
      work_directory: '/var/lib/mirdb/data',
      sstable_size_mb: 50,
      memtable_size_mb: 2,
    };
    render(<SystemOverview config={customConfig} />);
    expect(screen.getByTestId('system-overview-value-listen_address')).toHaveTextContent('127.0.0.1:9999');
    expect(screen.getByTestId('system-overview-value-work_directory')).toHaveTextContent('/var/lib/mirdb/data');
  });

  it('has proper ARIA attributes for accessibility', () => {
    render(<SystemOverview config={MOCK_CONFIG} />);
    expect(screen.getByTestId('system-overview-section')).toHaveAttribute('aria-label', 'System Overview');
    expect(screen.getByTestId('system-overview-list').tagName.toLowerCase()).toBe('dl');
  });

  it('exports CONFIG_ITEMS with correct labels and tooltip descriptions', () => {
    expect(CONFIG_ITEMS).toHaveLength(5);
    expect(CONFIG_ITEMS[0].label).toBe('Listen Address');
    expect(CONFIG_ITEMS[1].label).toBe('Max LSM Levels');
    expect(CONFIG_ITEMS[2].label).toBe('Work Directory');
    expect(CONFIG_ITEMS[3].label).toBe('SSTable Max Size');
    expect(CONFIG_ITEMS[4].label).toBe('Memtable Max Size');
  });

  it('exports CONFIG_ITEMS with tooltip explaining max LSM levels', () => {
    const maxLevelItem = CONFIG_ITEMS.find((item) => item.key === 'max_lsm_levels');
    expect(maxLevelItem).toBeDefined();
    expect(maxLevelItem!.tooltip).toContain('Maximum number of LSM tree levels');
  });

  it('exports CONFIG_ITEMS with tooltip explaining listen address', () => {
    const addrItem = CONFIG_ITEMS.find((item) => item.key === 'listen_address');
    expect(addrItem).toBeDefined();
    expect(addrItem!.tooltip).toContain('address');
  });

  it('exports CONFIG_ITEMS with tooltip explaining work directory', () => {
    const dirItem = CONFIG_ITEMS.find((item) => item.key === 'work_directory');
    expect(dirItem).toBeDefined();
    expect(dirItem!.tooltip.toLowerCase()).toContain('directory');
  });

  it('exports CONFIG_ITEMS with tooltip explaining SSTable size', () => {
    const sstItem = CONFIG_ITEMS.find((item) => item.key === 'sstable_size_mb');
    expect(sstItem).toBeDefined();
    expect(sstItem!.tooltip.toLowerCase()).toContain('sstable');
  });

  it('exports CONFIG_ITEMS with tooltip explaining memtable size', () => {
    const memItem = CONFIG_ITEMS.find((item) => item.key === 'memtable_size_mb');
    expect(memItem).toBeDefined();
    expect(memItem!.tooltip.toLowerCase()).toContain('memtable');
  });
});

describe('SystemOverview tooltips', () => {
  it('shows tooltip on hover over listen address value', async () => {
    const user = userEvent.setup();
    render(<SystemOverview config={MOCK_CONFIG} />);
    const row = screen.getByTestId('system-overview-row-listen_address');
    await user.hover(row);
    await waitFor(() => {
      expect(screen.getByRole('tooltip')).toBeInTheDocument();
    });
    expect(screen.getByRole('tooltip')).toHaveTextContent(/address/);
  });

  it('shows tooltip explaining max LSM levels on hover', async () => {
    const user = userEvent.setup();
    render(<SystemOverview config={MOCK_CONFIG} />);
    const row = screen.getByTestId('system-overview-row-max_lsm_levels');
    await user.hover(row);
    await waitFor(() => {
      expect(screen.getByRole('tooltip')).toBeInTheDocument();
    });
    expect(screen.getByRole('tooltip')).toHaveTextContent(/Maximum number of LSM tree levels/);
  });

  it('shows tooltip on hover over work directory value', async () => {
    const user = userEvent.setup();
    render(<SystemOverview config={MOCK_CONFIG} />);
    const row = screen.getByTestId('system-overview-row-work_directory');
    await user.hover(row);
    await waitFor(() => {
      expect(screen.getByRole('tooltip')).toBeInTheDocument();
    });
    expect(screen.getByRole('tooltip')).toHaveTextContent(/directory/i);
  });

  it('shows tooltip on hover over SSTable size value', async () => {
    const user = userEvent.setup();
    render(<SystemOverview config={MOCK_CONFIG} />);
    const row = screen.getByTestId('system-overview-row-sstable_size_mb');
    await user.hover(row);
    await waitFor(() => {
      expect(screen.getByRole('tooltip')).toBeInTheDocument();
    });
    expect(screen.getByRole('tooltip')).toHaveTextContent(/SSTable/);
  });

  it('shows tooltip on hover over memtable size value', async () => {
    const user = userEvent.setup();
    render(<SystemOverview config={MOCK_CONFIG} />);
    const row = screen.getByTestId('system-overview-row-memtable_size_mb');
    await user.hover(row);
    await waitFor(() => {
      expect(screen.getByRole('tooltip')).toBeInTheDocument();
    });
    expect(screen.getByRole('tooltip')).toHaveTextContent(/memtable/);
  });

  it('hides tooltip when mouse leaves the row', async () => {
    const user = userEvent.setup();
    render(<SystemOverview config={MOCK_CONFIG} />);
    const row = screen.getByTestId('system-overview-row-listen_address');
    await user.hover(row);
    await waitFor(() => {
      expect(screen.getByRole('tooltip')).toBeInTheDocument();
    });
    await user.unhover(row);
    await waitFor(() => {
      expect(screen.queryByRole('tooltip')).not.toBeInTheDocument();
    });
  });
});

describe('getConfig API integration', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('returns ServerConfig with all required fields on successful response', async () => {
    const mockResponse: ServerConfig = {
      listen_address: '0.0.0.0:12333',
      max_lsm_levels: 7,
      work_directory: '/tmp/mirdbs',
      sstable_size_mb: 100,
      memtable_size_mb: 4,
    };

    (fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    } as Response);

    const result = await getConfig();

    expect(result).toHaveProperty('listen_address');
    expect(result).toHaveProperty('max_lsm_levels');
    expect(result).toHaveProperty('work_directory');
    expect(result).toHaveProperty('sstable_size_mb');
    expect(result).toHaveProperty('memtable_size_mb');
    expect(result.listen_address).toBe('0.0.0.0:12333');
    expect(result.max_lsm_levels).toBe(7);
    expect(result.work_directory).toBe('/tmp/mirdbs');
    expect(result.sstable_size_mb).toBe(100);
    expect(result.memtable_size_mb).toBe(4);
  });

  it('calls the correct API endpoint', async () => {
    const mockResponse: ServerConfig = {
      listen_address: '0.0.0.0:12333',
      max_lsm_levels: 7,
      work_directory: '/tmp/mirdbs',
      sstable_size_mb: 100,
      memtable_size_mb: 4,
    };

    (fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    } as Response);

    await getConfig();

    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/config'),
      expect.objectContaining({
        headers: expect.objectContaining({
          'Content-Type': 'application/json',
        }),
      })
    );
  });

  it('throws an error when the API returns a non-OK status', async () => {
    (fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: false,
      status: 500,
      statusText: 'Internal Server Error',
    } as Response);

    await expect(getConfig()).rejects.toThrow('API error: 500 Internal Server Error');
  });

  it('handles different config values correctly', async () => {
    const mockResponse: ServerConfig = {
      listen_address: '127.0.0.1:8080',
      max_lsm_levels: 10,
      work_directory: '/opt/mirdb',
      sstable_size_mb: 200,
      memtable_size_mb: 8,
    };

    (fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    } as Response);

    const result = await getConfig();
    expect(result.listen_address).toBe('127.0.0.1:8080');
    expect(result.max_lsm_levels).toBe(10);
    expect(result.work_directory).toBe('/opt/mirdb');
    expect(result.sstable_size_mb).toBe(200);
    expect(result.memtable_size_mb).toBe(8);
  });

  it('validates that max_lsm_levels is a number', async () => {
    const mockResponse = {
      listen_address: '0.0.0.0:12333',
      max_lsm_levels: 7,
      work_directory: '/tmp/mirdbs',
      sstable_size_mb: 100,
      memtable_size_mb: 4,
    };

    (fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    } as Response);

    const result = await getConfig();
    expect(typeof result.max_lsm_levels).toBe('number');
  });

  it('validates that sstable_size_mb is a number', async () => {
    const mockResponse = {
      listen_address: '0.0.0.0:12333',
      max_lsm_levels: 7,
      work_directory: '/tmp/mirdbs',
      sstable_size_mb: 100,
      memtable_size_mb: 4,
    };

    (fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    } as Response);

    const result = await getConfig();
    expect(typeof result.sstable_size_mb).toBe('number');
  });

  it('validates that memtable_size_mb is a number', async () => {
    const mockResponse = {
      listen_address: '0.0.0.0:12333',
      max_lsm_levels: 7,
      work_directory: '/tmp/mirdbs',
      sstable_size_mb: 100,
      memtable_size_mb: 4,
    };

    (fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    } as Response);

    const result = await getConfig();
    expect(typeof result.memtable_size_mb).toBe('number');
  });

  it('validates that listen_address is a string', async () => {
    const mockResponse = {
      listen_address: '0.0.0.0:12333',
      max_lsm_levels: 7,
      work_directory: '/tmp/mirdbs',
      sstable_size_mb: 100,
      memtable_size_mb: 4,
    };

    (fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    } as Response);

    const result = await getConfig();
    expect(typeof result.listen_address).toBe('string');
  });

  it('validates that work_directory is a string', async () => {
    const mockResponse = {
      listen_address: '0.0.0.0:12333',
      max_lsm_levels: 7,
      work_directory: '/tmp/mirdbs',
      sstable_size_mb: 100,
      memtable_size_mb: 4,
    };

    (fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    } as Response);

    const result = await getConfig();
    expect(typeof result.work_directory).toBe('string');
  });
});

/**
 * Unit tests for formatting utilities
 *
 * Owner: Scenario 2 - Real-time System Metrics Dashboard
 */

import { describe, it, expect } from 'vitest';
import {
  formatBytes,
  formatNumber,
  formatPercent,
  formatDuration,
  formatMemory,
} from '../../../src/utils/format';

describe('formatBytes', () => {
  it('should format 0 bytes correctly', () => {
    expect(formatBytes(0)).toBe('0 B');
  });

  it('should format bytes (< 1KB) correctly', () => {
    expect(formatBytes(512)).toBe('512 B');
  });

  it('should format kilobytes correctly', () => {
    expect(formatBytes(1024)).toBe('1.00 KB');
    expect(formatBytes(1536)).toBe('1.50 KB');
  });

  it('should format megabytes correctly', () => {
    expect(formatBytes(1048576)).toBe('1.00 MB');
    expect(formatBytes(10485760)).toBe('10.0 MB');
  });

  it('should format gigabytes correctly', () => {
    expect(formatBytes(1073741824)).toBe('1.00 GB');
    expect(formatBytes(1289748480)).toBe('1.20 GB');
    expect(formatBytes(2147483648)).toBe('2.00 GB');
  });

  it('should use appropriate decimal places', () => {
    // Values >= 10 should have 1 decimal place
    expect(formatBytes(11 * 1024 * 1024 * 1024)).toBe('11.0 GB');
    // Values < 10 should have 2 decimal places
    expect(formatBytes(5.5 * 1024 * 1024 * 1024)).toBe('5.50 GB');
  });
});

describe('formatNumber', () => {
  it('should format small numbers without commas', () => {
    expect(formatNumber(42)).toBe('42');
    expect(formatNumber(999)).toBe('999');
  });

  it('should format thousands with commas', () => {
    expect(formatNumber(1000)).toBe('1,000');
    expect(formatNumber(12345)).toBe('12,345');
  });

  it('should format millions with commas', () => {
    expect(formatNumber(1000000)).toBe('1,000,000');
    expect(formatNumber(1234567890)).toBe('1,234,567,890');
  });

  it('should handle zero', () => {
    expect(formatNumber(0)).toBe('0');
  });
});

describe('formatPercent', () => {
  it('should format 0% correctly', () => {
    expect(formatPercent(0)).toBe('0%');
  });

  it('should format 100% correctly', () => {
    expect(formatPercent(1)).toBe('100%');
  });

  it('should format decimal ratios correctly', () => {
    expect(formatPercent(0.94)).toBe('94%');
    expect(formatPercent(0.5)).toBe('50%');
    expect(formatPercent(0.123)).toBe('12%');
  });

  it('should round to nearest integer', () => {
    expect(formatPercent(0.945)).toBe('95%');
    expect(formatPercent(0.944)).toBe('94%');
  });
});

describe('formatDuration', () => {
  it('should format seconds only', () => {
    expect(formatDuration(30)).toBe('30s');
    expect(formatDuration(59)).toBe('59s');
  });

  it('should format minutes only', () => {
    expect(formatDuration(60)).toBe('1m');
    expect(formatDuration(120)).toBe('2m');
  });

  it('should format hours and minutes', () => {
    expect(formatDuration(3600)).toBe('1h');
    expect(formatDuration(3660)).toBe('1h 1m');
    expect(formatDuration(7200)).toBe('2h');
  });

  it('should format days, hours, and minutes', () => {
    expect(formatDuration(86400)).toBe('1d');
    expect(formatDuration(90000)).toBe('1d 1h');
    expect(formatDuration(90060)).toBe('1d 1h 1m');
    expect(formatDuration(172800)).toBe('2d');
  });

  it('should format complex durations', () => {
    // 1 day, 2 hours, 30 minutes = 86400 + 7200 + 1800 = 95400
    expect(formatDuration(95400)).toBe('1d 2h 30m');
  });
});

describe('formatMemory', () => {
  it('should format memory usage correctly', () => {
    expect(formatMemory(1289748480, 2147483648)).toBe('1.20 GB / 2.00 GB');
  });

  it('should handle zero used', () => {
    expect(formatMemory(0, 2147483648)).toBe('0 B / 2.00 GB');
  });
});

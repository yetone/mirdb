/**
 * Formatting utilities
 *
 * Owner: Scenario 2 - Real-time System Metrics Dashboard
 *
 * Exports:
 * - formatBytes(bytes: number): string (e.g., "1.2 GB")
 * - formatNumber(num: number): string (e.g., "12,345")
 * - formatPercent(ratio: number): string (e.g., "94%")
 * - formatDuration(seconds: number): string (e.g., "1d 2h 30m")
 */

/**
 * Formats a byte count into a human-readable string.
 * @param bytes - The number of bytes
 * @returns Formatted string (e.g., "1.2 GB")
 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';

  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const k = 1024;
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  const value = bytes / Math.pow(k, i);

  // For bytes (i === 0), don't show decimals
  if (i === 0) {
    return `${Math.round(value)} ${units[i]}`;
  }

  // Use 1 decimal place for values >= 10, 2 for smaller values
  const decimals = value >= 10 ? 1 : 2;

  return `${value.toFixed(decimals)} ${units[i]}`;
}

/**
 * Formats a number with thousand separators.
 * @param num - The number to format
 * @returns Formatted string (e.g., "12,345")
 */
export function formatNumber(num: number): string {
  return num.toLocaleString('en-US');
}

/**
 * Formats a ratio (0-1) as a percentage.
 * @param ratio - The ratio between 0 and 1
 * @returns Formatted percentage string (e.g., "94%")
 */
export function formatPercent(ratio: number): string {
  const percent = Math.round(ratio * 100);
  return `${percent}%`;
}

/**
 * Formats a duration in seconds into a human-readable string.
 * @param seconds - The duration in seconds
 * @returns Formatted string (e.g., "1d 2h 30m")
 */
export function formatDuration(seconds: number): string {
  if (seconds < 60) {
    return `${seconds}s`;
  }

  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);

  const parts: string[] = [];

  if (days > 0) {
    parts.push(`${days}d`);
  }
  if (hours > 0) {
    parts.push(`${hours}h`);
  }
  if (minutes > 0) {
    parts.push(`${minutes}m`);
  }

  return parts.join(' ') || '0m';
}

/**
 * Formats memory usage as a fraction (e.g., "1.2GB / 2GB").
 * @param used - Used memory in bytes
 * @param total - Total memory in bytes
 * @returns Formatted string
 */
export function formatMemory(used: number, total: number): string {
  return `${formatBytes(used)} / ${formatBytes(total)}`;
}

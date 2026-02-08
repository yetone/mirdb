/**
 * Clipboard Utility Unit Tests
 * Owner: Scenario 3 - Installation Instructions
 *
 * Test cases:
 * 3. Call copyToClipboard utility function with test text
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { copyToClipboard } from './clipboard';

describe('copyToClipboard', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 3: Function returns true and text is available in clipboard
  it('returns true when copy is successful', async () => {
    const testText = 'git clone https://github.com/yetone/mirdb.git';

    const result = await copyToClipboard(testText);

    expect(result).toBe(true);
  });

  it('calls navigator.clipboard.writeText with the correct text', async () => {
    const testText = 'cargo build --release';

    await copyToClipboard(testText);

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(testText);
  });

  it('handles empty strings', async () => {
    const result = await copyToClipboard('');

    expect(result).toBe(true);
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith('');
  });

  it('handles multiline text', async () => {
    const multilineText = `cd mirdb
cargo build --release
./target/release/mirdb`;

    const result = await copyToClipboard(multilineText);

    expect(result).toBe(true);
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(multilineText);
  });

  it('handles special characters in text', async () => {
    const specialText = 'echo "Hello, World!" && ./run.sh --flag="value"';

    const result = await copyToClipboard(specialText);

    expect(result).toBe(true);
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(specialText);
  });
});

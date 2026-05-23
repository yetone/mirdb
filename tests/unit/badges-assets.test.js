/**
 * Unit tests for badge and asset file existence.
 * Owner: Scenario 8 - Status Badges and Visual Assets
 *
 * Tests:
 * - assets/logo.gif exists in the repository
 * - assets/usage.gif exists in the repository
 */

import { describe, it, expect } from 'vitest';
import { existsSync, statSync } from 'fs';
import { resolve, join } from 'path';

describe('Asset File Existence', () => {
  const assetsDir = resolve(process.cwd(), 'assets');

  it('assets/logo.gif exists in the project', () => {
    const logoPath = join(assetsDir, 'logo.gif');
    expect(existsSync(logoPath)).toBe(true);

    const stats = statSync(logoPath);
    expect(stats.size).toBeGreaterThan(0);
  });

  it('assets/usage.gif exists in the project', () => {
    const usagePath = join(assetsDir, 'usage.gif');
    expect(existsSync(usagePath)).toBe(true);

    const stats = statSync(usagePath);
    expect(stats.size).toBeGreaterThan(0);
  });
});

/**
 * Integration tests for badge HTTP status.
 * Owner: Scenario 8 - Status Badges and Visual Assets
 *
 * Tests:
 * - Badge image URL returns HTTP 200 with image content-type
 */

import { describe, it, expect } from 'vitest';

describe('Badge HTTP Status', () => {
  it('CircleCI badge image URL is reachable and returns image content', async () => {
    const badgeUrl = 'https://img.shields.io/circleci/build/gh/yetone/mirdb';
    const response = await fetch(badgeUrl, { method: 'HEAD', redirect: 'follow' });
    expect(response.status).toBe(200);

    const contentType = response.headers.get('content-type') || '';
    expect(contentType).toMatch(/image/);
  });
});

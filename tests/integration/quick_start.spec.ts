import { describe, it, expect } from 'vitest';
import fs from 'fs';

const configSection = fs.readFileSync('src/components/ConfigSection/index.html', 'utf-8');

describe('Quick Start Documentation', () => {
  it('contains exactly 3 installation steps', () => {
    const stepCount = (configSection.match(/<li>/g) || []).length;
    expect(stepCount).toBe(3);
  });

  it('shows correct cargo installation command', () => {
    expect(configSection).toContain('cargo install mirdb');
  });

  it('verifies server start command uses default port', () => {
    expect(configSection).toContain('mirdb start --port 11211');
  });

  it('references latest GitHub release version', () => {
    expect(configSection).toMatch(/Version: v\d+\.\d+\.\d+ \(matches <a href="https:\/\/github\.com\/yestone\/mirdb\/releases\/latest">latest GitHub release<\/a>\)/);
  });

  it('includes OS compatibility warning', () => {
    expect(configSection).toContain('Windows support coming in Q3 2026');
  });

  it('shows verification commands with correct nc syntax', () => {
    expect(configSection).toContain('echo "set test 0 0 5\\nhello\\n" | nc localhost 11211');
  });
});

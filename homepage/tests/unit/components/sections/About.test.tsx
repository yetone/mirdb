import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { About } from '../../../../src/components/sections/About';

describe('About Component', () => {
  // Test Case 1: Component renders with section heading 'About MirDB'
  it('renders with section heading "About MirDB"', () => {
    render(<About />);
    const heading = screen.getByRole('heading', { name: /about mirdb/i });
    expect(heading).toBeInTheDocument();
  });

  // Test Case 2: Text describing key-value storage is present
  it('displays text describing key-value storage', () => {
    render(<About />);
    const keyValueTexts = screen.getAllByText(/key-value/i);
    expect(keyValueTexts.length).toBeGreaterThan(0);
    expect(keyValueTexts[0]).toBeInTheDocument();
  });

  // Test Case 3: Text mentioning Memcached protocol compatibility is present
  it('displays text mentioning Memcached protocol compatibility', () => {
    render(<About />);
    const memcachedTexts = screen.getAllByText(/memcached/i);
    expect(memcachedTexts.length).toBeGreaterThan(0);
    expect(memcachedTexts[0]).toBeInTheDocument();
  });

  // Test Case 4: Text describing persistence capability is present
  it('displays text describing persistence capability', () => {
    render(<About />);
    const persistenceTexts = screen.getAllByText(/persist/i);
    expect(persistenceTexts.length).toBeGreaterThan(0);
    expect(persistenceTexts[0]).toBeInTheDocument();
  });

  // Test Case 5: Visual diagram or illustration is present (image or SVG)
  it('displays an architecture diagram (image or SVG)', () => {
    render(<About />);
    // Check for either an image with architecture alt text or an SVG element
    const diagram = screen.getByRole('img', { name: /architecture|diagram/i });
    expect(diagram).toBeInTheDocument();
  });

  // Test that the diagram mentions key components
  it('architecture diagram shows Memtable component', () => {
    render(<About />);
    const memtableTexts = screen.getAllByText(/memtable/i);
    expect(memtableTexts.length).toBeGreaterThan(0);
    expect(memtableTexts[0]).toBeInTheDocument();
  });

  it('architecture diagram shows SSTable component', () => {
    render(<About />);
    const sstableTexts = screen.getAllByText(/sstable/i);
    expect(sstableTexts.length).toBeGreaterThan(0);
    expect(sstableTexts[0]).toBeInTheDocument();
  });

  it('architecture diagram shows Compaction flow', () => {
    render(<About />);
    const compactionTexts = screen.getAllByText(/compaction/i);
    expect(compactionTexts.length).toBeGreaterThan(0);
    expect(compactionTexts[0]).toBeInTheDocument();
  });

  // Accessibility test: Section has proper accessibility attributes
  it('has proper accessibility attributes', () => {
    render(<About />);
    const section = screen.getByRole('region', { name: /about mirdb/i });
    expect(section).toBeInTheDocument();
  });

  // Test Case 6: Content is 2-3 paragraphs, concise and scannable (manual test, but we can verify structure)
  it('has concise content structure with multiple paragraphs', () => {
    const { container } = render(<About />);
    const paragraphs = container.querySelectorAll('p');
    expect(paragraphs.length).toBeGreaterThanOrEqual(2);
    expect(paragraphs.length).toBeLessThanOrEqual(4);
  });

  // Test that LSM tree is mentioned (as per scenario requirements)
  it('mentions LSM tree architecture', () => {
    render(<About />);
    const lsmText = screen.getByText(/lsm tree/i);
    expect(lsmText).toBeInTheDocument();
  });
});

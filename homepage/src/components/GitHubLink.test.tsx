import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { GitHubLink } from './GitHubLink';

describe('GitHubLink', () => {
  const githubUrl = 'https://github.com/mirdb/mirdb';

  it('renders a link to the GitHub repository', () => {
    render(<GitHubLink />);
    const link = screen.getByRole('link', { name: /github/i });
    expect(link).toBeInTheDocument();
    expect(link).toHaveAttribute('href', githubUrl);
  });

  it('has rel="noopener noreferrer" for security', () => {
    // Test Case 4: Verify GitHub link has rel='noopener noreferrer' for security
    render(<GitHubLink />);
    const link = screen.getByRole('link', { name: /github/i });
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('has target="_blank" to open in new tab', () => {
    render(<GitHubLink />);
    const link = screen.getByRole('link', { name: /github/i });
    expect(link).toHaveAttribute('target', '_blank');
  });

  it('displays the GitHub icon', () => {
    render(<GitHubLink />);
    const icon = screen.getByTestId('github-icon');
    expect(icon).toBeInTheDocument();
  });
});

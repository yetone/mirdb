import React from 'react';
import { Github, Star, ExternalLink } from 'lucide-react';
import { GITHUB_REPO_URL } from '../../utils/constants';
import { useGitHubStars } from '../../hooks/useGitHubStars';
import { cn } from '../../utils/cn';

export default function GitHubLink() {
  const { stars, loading, error } = useGitHubStars();

  return (
    <a
      href={GITHUB_REPO_URL}
      target="_blank"
      rel="noopener noreferrer"
      className={cn(
        'inline-flex items-center gap-2 px-5 py-2.5 rounded-lg',
        'bg-gray-900 dark:bg-gray-800 text-white dark:text-gray-100',
        'hover:bg-gray-800 dark:hover:bg-gray-700',
        'border border-gray-700 dark:border-gray-600',
        'transition-colors duration-200',
        'focus:outline-none focus:ring-2 focus:ring-brand-500 focus:ring-offset-2 dark:focus:ring-offset-gray-950',
        'text-sm font-medium'
      )}
      aria-label={`View MirDB on GitHub${stars !== null ? ` — ${stars} stars` : ''}`}
    >
      <Github size={18} aria-hidden="true" />
      <span>View on GitHub</span>
      {loading && (
        <span className="flex items-center gap-1 text-gray-400" aria-label="Loading star count">
          <Star size={14} aria-hidden="true" className="animate-pulse" />
        </span>
      )}
      {!loading && !error && stars !== null && (
        <span className="flex items-center gap-1 text-yellow-400">
          <Star size={14} aria-hidden="true" fill="currentColor" />
          <span>{stars.toLocaleString()}</span>
        </span>
      )}
      <ExternalLink size={14} aria-hidden="true" className="text-gray-500" />
    </a>
  );
}

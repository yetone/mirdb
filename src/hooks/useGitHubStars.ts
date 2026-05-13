import { useState, useEffect } from 'react';
import { GITHUB_API_URL } from '../utils/constants';

interface GitHubStarsState {
  stars: number | null;
  loading: boolean;
  error: boolean;
}

export function useGitHubStars(): GitHubStarsState {
  const [state, setState] = useState<GitHubStarsState>({
    stars: null,
    loading: true,
    error: false,
  });

  useEffect(() => {
    let cancelled = false;

    async function fetchStars() {
      try {
        const res = await fetch(GITHUB_API_URL);
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`);
        }
        const data = await res.json();
        if (!cancelled) {
          setState({ stars: data.stargazers_count ?? null, loading: false, error: false });
        }
      } catch {
        if (!cancelled) {
          setState({ stars: null, loading: false, error: true });
        }
      }
    }

    fetchStars();

    return () => {
      cancelled = true;
    };
  }, []);

  return state;
}

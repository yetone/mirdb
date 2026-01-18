/**
 * GitHub API utilities for fetching repository statistics at build time.
 * These stats are fetched during the Astro build process and embedded as static data.
 */

export interface GitHubStats {
  stars: number;
  contributors: number;
  latestRelease: string | null;
  fetchedAt: string;
}

const GITHUB_OWNER = 'yetone';
const GITHUB_REPO = 'mirdb';
const GITHUB_API_BASE = 'https://api.github.com';

/**
 * Fetches repository statistics from GitHub API.
 * This function is intended to be called at build time only.
 */
export async function fetchGitHubStats(): Promise<GitHubStats> {
  const headers: HeadersInit = {
    'Accept': 'application/vnd.github.v3+json',
    'User-Agent': 'MirDB-Homepage',
  };

  // Add authorization header if token is available (for higher rate limits)
  const token = process.env.GITHUB_TOKEN;
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }

  try {
    // Fetch repository info (for stars) and contributors count in parallel
    const [repoResponse, contributorsResponse, releasesResponse] = await Promise.all([
      fetch(`${GITHUB_API_BASE}/repos/${GITHUB_OWNER}/${GITHUB_REPO}`, { headers }),
      fetch(`${GITHUB_API_BASE}/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contributors?per_page=1&anon=true`, { headers }),
      fetch(`${GITHUB_API_BASE}/repos/${GITHUB_OWNER}/${GITHUB_REPO}/releases/latest`, { headers }),
    ]);

    // Parse stars from repo info
    let stars = 0;
    if (repoResponse.ok) {
      const repoData = await repoResponse.json();
      stars = repoData.stargazers_count || 0;
    }

    // Parse contributor count from Link header (GitHub returns total in header)
    let contributors = 0;
    if (contributorsResponse.ok) {
      const linkHeader = contributorsResponse.headers.get('Link');
      if (linkHeader) {
        // Parse the last page number from Link header
        const lastPageMatch = linkHeader.match(/page=(\d+)>; rel="last"/);
        if (lastPageMatch) {
          contributors = parseInt(lastPageMatch[1], 10);
        }
      }
      // If no Link header, count the response items
      if (contributors === 0) {
        const contributorsData = await contributorsResponse.json();
        if (Array.isArray(contributorsData)) {
          contributors = contributorsData.length;
        }
      }
    }

    // Parse latest release
    let latestRelease: string | null = null;
    if (releasesResponse.ok) {
      const releaseData = await releasesResponse.json();
      latestRelease = releaseData.tag_name || releaseData.name || null;
    }

    return {
      stars,
      contributors,
      latestRelease,
      fetchedAt: new Date().toISOString(),
    };
  } catch (error) {
    console.error('Failed to fetch GitHub stats:', error);
    // Return default values if fetch fails
    return {
      stars: 0,
      contributors: 0,
      latestRelease: null,
      fetchedAt: new Date().toISOString(),
    };
  }
}

/**
 * Format a number for display (e.g., 1234 -> "1.2k")
 */
export function formatNumber(num: number): string {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1).replace(/\.0$/, '') + 'M';
  }
  if (num >= 1000) {
    return (num / 1000).toFixed(1).replace(/\.0$/, '') + 'k';
  }
  return num.toString();
}

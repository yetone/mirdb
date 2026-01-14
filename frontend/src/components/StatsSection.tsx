import { useEffect, useState } from 'react'
import { motion } from 'framer-motion'

interface StatsData {
  totalUrls: number
  totalClicks: number
  activeUsers: number
}

interface StatsSectionProps {
  /** Optional: Provide custom fetch function for testing */
  fetchStats?: () => Promise<StatsData>
}

const defaultStats: StatsData = {
  totalUrls: 10000,
  totalClicks: 500000,
  activeUsers: 5000,
}

/**
 * Fetches homepage statistics from the API.
 * This function can be mocked in tests.
 */
async function fetchStatsFromAPI(): Promise<StatsData> {
  const response = await fetch('/api/stats/homepage')
  if (!response.ok) {
    throw new Error('Failed to fetch stats')
  }
  return response.json()
}

const StatsSection = ({ fetchStats = fetchStatsFromAPI }: StatsSectionProps) => {
  const [stats, setStats] = useState<StatsData | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [hasError, setHasError] = useState(false)

  useEffect(() => {
    let isMounted = true

    const loadStats = async () => {
      setIsLoading(true)
      setHasError(false)

      try {
        const data = await fetchStats()
        if (isMounted) {
          setStats(data)
          setIsLoading(false)
        }
      } catch {
        if (isMounted) {
          // Use fallback data on error for graceful degradation
          setStats(defaultStats)
          setHasError(true)
          setIsLoading(false)
        }
      }
    }

    loadStats()

    return () => {
      isMounted = false
    }
  }, [fetchStats])

  const statItems = [
    {
      id: 'urls',
      label: 'URLs Shortened',
      value: stats?.totalUrls ?? 0,
      icon: (
        <svg
          xmlns="http://www.w3.org/2000/svg"
          className="h-6 w-6"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
          />
        </svg>
      ),
    },
    {
      id: 'clicks',
      label: 'Total Clicks',
      value: stats?.totalClicks ?? 0,
      icon: (
        <svg
          xmlns="http://www.w3.org/2000/svg"
          className="h-6 w-6"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M15 15l-2 5L9 9l11 4-5 2zm0 0l5 5M7.188 2.239l.777 2.897M5.136 7.965l-2.898-.777M13.95 4.05l-2.122 2.122m-5.657 5.656l-2.12 2.122"
          />
        </svg>
      ),
    },
    {
      id: 'users',
      label: 'Active Users',
      value: stats?.activeUsers ?? 0,
      icon: (
        <svg
          xmlns="http://www.w3.org/2000/svg"
          className="h-6 w-6"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z"
          />
        </svg>
      ),
    },
  ]

  const formatNumber = (num: number): string => {
    if (num >= 1000000) {
      return `${(num / 1000000).toFixed(1)}M+`
    }
    if (num >= 1000) {
      return `${(num / 1000).toFixed(0)}K+`
    }
    return num.toLocaleString()
  }

  return (
    <section
      className="py-12 px-4 sm:px-6 lg:px-8 bg-base-300"
      data-testid="stats-section"
      aria-labelledby="stats-heading"
    >
      <div className="max-w-7xl mx-auto">
        <h2 id="stats-heading" className="sr-only">
          Platform Statistics
        </h2>

        {isLoading ? (
          <div
            className="grid grid-cols-1 md:grid-cols-3 gap-6"
            data-testid="stats-loading"
          >
            {[1, 2, 3].map((i) => (
              <div
                key={i}
                className="card bg-base-100 shadow-md animate-pulse"
                data-testid="stats-skeleton"
              >
                <div className="card-body flex flex-row items-center gap-4">
                  <div className="w-12 h-12 rounded-full bg-base-300" />
                  <div className="flex-1">
                    <div className="h-8 bg-base-300 rounded w-20 mb-2" />
                    <div className="h-4 bg-base-300 rounded w-24" />
                  </div>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <>
            {hasError && (
              <div
                className="text-center text-sm text-base-content/50 mb-4"
                data-testid="stats-fallback-notice"
              >
                Showing estimated statistics
              </div>
            )}
            <motion.div
              className="grid grid-cols-1 md:grid-cols-3 gap-6"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ duration: 0.5 }}
            >
              {statItems.map((stat, index) => (
                <motion.div
                  key={stat.id}
                  className="card bg-base-100 shadow-md hover:shadow-lg transition-shadow"
                  data-testid="stats-card"
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.4, delay: index * 0.1 }}
                >
                  <div className="card-body flex flex-row items-center gap-4">
                    <div className="w-12 h-12 rounded-full bg-primary/10 flex items-center justify-center text-primary">
                      {stat.icon}
                    </div>
                    <div>
                      <div className="text-2xl font-bold text-base-content">
                        {formatNumber(stat.value)}
                      </div>
                      <div className="text-sm text-base-content/70">
                        {stat.label}
                      </div>
                    </div>
                  </div>
                </motion.div>
              ))}
            </motion.div>
          </>
        )}
      </div>
    </section>
  )
}

export default StatsSection

/**
 * Statistics/Social Proof Section component.
 * Owner: Scenario 4 - Social Proof Statistics Section
 *
 * Requirements:
 * - 3 statistic cards: Links Shortened, Clicks Tracked, Active Users
 * - Fetch data from API if available, fallback to static values
 * - Loading state while fetching
 * - Error handling with graceful fallback
 */

import React, { useEffect, useState } from 'react'
import { Link2, MousePointer2, Users } from 'lucide-react'
import { fetchPublicStats, PublicStats, DEFAULT_STATS } from '../../api'
import { Statistic } from '../../types/homepage'

interface StatCardProps {
  icon: React.ReactNode
  value: string | number
  label: string
  testId: string
}

const StatCard: React.FC<StatCardProps> = ({ icon, value, label, testId }) => {
  return (
    <div
      className="card bg-base-200 shadow-xl hover:shadow-2xl transition-shadow duration-300"
      data-testid={testId}
    >
      <div className="card-body items-center text-center">
        <div className="text-primary mb-2">{icon}</div>
        <h3 className="text-4xl font-bold text-base-content" data-testid={`${testId}-value`}>
          {typeof value === 'number' ? value.toLocaleString() : value}
        </h3>
        <p className="text-base-content/70" data-testid={`${testId}-label`}>
          {label}
        </p>
      </div>
    </div>
  )
}

export interface StatsSectionProps {
  fetchStats?: () => Promise<PublicStats>
}

export const StatsSection: React.FC<StatsSectionProps> = ({ fetchStats = fetchPublicStats }) => {
  const [stats, setStats] = useState<PublicStats>(DEFAULT_STATS)
  const [isLoading, setIsLoading] = useState(true)
  const [hasError, setHasError] = useState(false)

  useEffect(() => {
    let isMounted = true

    const loadStats = async () => {
      setIsLoading(true)
      try {
        const data = await fetchStats()
        if (isMounted) {
          setStats(data)
          setHasError(false)
        }
      } catch (error) {
        if (isMounted) {
          setHasError(true)
          setStats(DEFAULT_STATS)
        }
      } finally {
        if (isMounted) {
          setIsLoading(false)
        }
      }
    }

    loadStats()

    return () => {
      isMounted = false
    }
  }, [fetchStats])

  const statistics: (Statistic & { icon: React.ReactNode; testId: string })[] = [
    {
      icon: <Link2 size={48} strokeWidth={1.5} />,
      value: stats.linksShortened,
      label: 'Links Shortened',
      testId: 'stat-links-shortened',
    },
    {
      icon: <MousePointer2 size={48} strokeWidth={1.5} />,
      value: stats.clicksTracked,
      label: 'Clicks Tracked',
      testId: 'stat-clicks-tracked',
    },
    {
      icon: <Users size={48} strokeWidth={1.5} />,
      value: stats.activeUsers,
      label: 'Active Users',
      testId: 'stat-active-users',
    },
  ]

  return (
    <section
      className="py-16 px-4 bg-base-100"
      data-testid="stats-section"
      aria-label="Platform Statistics"
    >
      <div className="container mx-auto">
        <h2 className="text-3xl font-bold text-center mb-12 text-base-content">
          Trusted by Thousands
        </h2>

        {isLoading ? (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8" data-testid="stats-loading">
            {[1, 2, 3].map((i) => (
              <div key={i} className="card bg-base-200 shadow-xl animate-pulse">
                <div className="card-body items-center text-center">
                  <div className="w-12 h-12 bg-base-300 rounded-full mb-2"></div>
                  <div className="h-10 w-24 bg-base-300 rounded"></div>
                  <div className="h-4 w-20 bg-base-300 rounded mt-2"></div>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8" data-testid="stats-content">
            {statistics.map((stat) => (
              <StatCard
                key={stat.testId}
                icon={stat.icon}
                value={stat.value}
                label={stat.label}
                testId={stat.testId}
              />
            ))}
          </div>
        )}

        {hasError && (
          <p className="text-center text-base-content/50 text-sm mt-4" data-testid="stats-error-note">
            Showing estimated statistics
          </p>
        )}
      </div>
    </section>
  )
}

export default StatsSection

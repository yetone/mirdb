/**
 * API client for the URL Shortener service.
 * Owner: Scenario 4 - Social Proof Statistics Section (stats endpoint)
 *
 * This file provides API methods for fetching data from the backend.
 */

import axios, { AxiosError } from 'axios'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api/v1'

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 5000,
  headers: {
    'Content-Type': 'application/json',
  },
})

export interface PublicStats {
  linksShortened: number
  clicksTracked: number
  activeUsers: number
}

export const DEFAULT_STATS: PublicStats = {
  linksShortened: 1250000,
  clicksTracked: 8500000,
  activeUsers: 25000,
}

/**
 * Fetches public statistics for the homepage.
 * Returns fallback values if the API fails or times out.
 */
export async function fetchPublicStats(): Promise<PublicStats> {
  try {
    const response = await apiClient.get<PublicStats>('/stats/public')
    return response.data
  } catch (error) {
    // Return default stats on any error
    console.warn('Failed to fetch stats, using defaults:', error instanceof AxiosError ? error.message : error)
    return DEFAULT_STATS
  }
}

export default apiClient

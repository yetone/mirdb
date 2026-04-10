/**
 * Metrics API client
 *
 * Owner: Scenario 2 - Real-time System Metrics Dashboard
 *
 * Provides functions to fetch system metrics from the MirDB server.
 */

import { apiClient } from './index';
import type { MetricsResponse } from '../types/api';

/**
 * Fetches the current system metrics from the server.
 * @returns Promise resolving to MetricsResponse
 */
export async function fetchMetrics(): Promise<MetricsResponse> {
  return apiClient.get<MetricsResponse>('/metrics');
}

/**
 * Application constants.
 */

import type { Theme } from '../types';

export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api';

export const SUPPORTED_THEMES: readonly Theme[] = [
  'light',
  'dark',
  'cyberpunk',
  'synthwave',
] as const;

export const DEFAULT_THEME: Theme = 'dark';

export const COPY_FEEDBACK_DURATION_MS = 2000;

export const URL_MAX_LENGTH = 2048;

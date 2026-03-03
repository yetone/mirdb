import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor to add auth token
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('auth-token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor for error handling
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('auth-token');
      localStorage.removeItem('auth-user');
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);

export interface ShortenUrlRequest {
  long_url: string;
}

export interface ShortenUrlResponse {
  short_code: string;
  short_url: string;
  long_url: string;
}

export const shortenUrl = async (longUrl: string): Promise<ShortenUrlResponse> => {
  const response = await api.post<ShortenUrlResponse>('/api/urls/shorten', {
    long_url: longUrl,
  });
  return response.data;
};

export default api;

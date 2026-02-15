import axios from 'axios';

const api = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor to add auth token if available
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
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
      localStorage.removeItem('token');
      // Could redirect to login here if needed
    }
    return Promise.reject(error);
  }
);

export interface ShortenUrlRequest {
  original_url: string;
}

export interface ShortenUrlResponse {
  id: number;
  original_url: string;
  short_code: string;
  created_at: string;
  user_id: number | null;
  click_count: number;
}

export const urlsApi = {
  shorten: async (data: ShortenUrlRequest): Promise<ShortenUrlResponse> => {
    const response = await api.post<ShortenUrlResponse>('/urls/', data);
    return response.data;
  },
};

export default api;

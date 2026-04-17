import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
  },
})

api.interceptors.request.use((config) => {
  const token = localStorage.getItem('token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('token')
      window.location.href = '/login'
    }
    return Promise.reject(error)
  }
)

export interface ShortenUrlRequest {
  url: string
  customCode?: string
}

export interface ShortenUrlResponse {
  shortCode: string
  shortUrl: string
  originalUrl: string
  createdAt: string
}

export const shortenUrl = async (data: ShortenUrlRequest): Promise<ShortenUrlResponse> => {
  const response = await api.post<ShortenUrlResponse>('/shorten', data)
  return response.data
}

export default api

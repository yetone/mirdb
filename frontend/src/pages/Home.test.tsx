import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import Home from './Home'
import { AppRoutes } from '../App'

// Test Case 1: Unit test - Login navigation element is visible and accessible
describe('Test Case 1: Login navigation element visibility', () => {
  it('should render Login link/button on homepage', () => {
    render(
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    )
    
    const loginLink = screen.getByTestId('login-link')
    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toBeVisible()
    expect(loginLink).toHaveAttribute('href', '/login')
  })

  it('should have accessible Login button in CTA section', () => {
    render(
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    )
    
    const loginBtn = screen.getByTestId('login-btn')
    expect(loginBtn).toBeInTheDocument()
    expect(loginBtn).toBeVisible()
    expect(loginBtn).toHaveTextContent('Login')
  })
})

// Test Case 2: Unit test - Register/Sign Up navigation element is visible and accessible
describe('Test Case 2: Register navigation element visibility', () => {
  it('should render Sign Up link on homepage', () => {
    render(
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    )
    
    const registerLink = screen.getByTestId('register-link')
    expect(registerLink).toBeInTheDocument()
    expect(registerLink).toBeVisible()
    expect(registerLink).toHaveAttribute('href', '/register')
  })

  it('should have accessible Get Started button in CTA section', () => {
    render(
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    )
    
    const getStartedBtn = screen.getByTestId('get-started-btn')
    expect(getStartedBtn).toBeInTheDocument()
    expect(getStartedBtn).toBeVisible()
    expect(getStartedBtn).toHaveTextContent('Get Started')
  })
})

// Test Case 3: Integration test - Click Login link and check router navigation
describe('Test Case 3: Login navigation integration', () => {
  it('should navigate to /login when Login link is clicked', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )
    
    const loginLink = screen.getByTestId('login-link')
    fireEvent.click(loginLink)
    
    expect(screen.getByTestId('login-page')).toBeInTheDocument()
  })

  it('should navigate to /login when Login button is clicked', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )
    
    const loginBtn = screen.getByTestId('login-btn')
    fireEvent.click(loginBtn)
    
    expect(screen.getByTestId('login-page')).toBeInTheDocument()
  })
})

// Test Case 4: Integration test - Click Register/Sign Up button and check router navigation
describe('Test Case 4: Register navigation integration', () => {
  it('should navigate to /register when Sign Up link is clicked', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )
    
    const registerLink = screen.getByTestId('register-link')
    fireEvent.click(registerLink)
    
    expect(screen.getByTestId('register-page')).toBeInTheDocument()
  })

  it('should navigate to /register when Get Started button is clicked', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )
    
    const getStartedBtn = screen.getByTestId('get-started-btn')
    fireEvent.click(getStartedBtn)
    
    expect(screen.getByTestId('register-page')).toBeInTheDocument()
  })
})

// Test Case 5: E2E test - Navigation response time
describe('Test Case 5: Navigation response time', () => {
  it('should complete Login navigation in less than 1 second', () => {
    const startTime = performance.now()
    
    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )
    
    const loginLink = screen.getByTestId('login-link')
    fireEvent.click(loginLink)
    
    expect(screen.getByTestId('login-page')).toBeInTheDocument()
    
    const endTime = performance.now()
    const navigationTime = endTime - startTime
    
    expect(navigationTime).toBeLessThan(1000)
  })

  it('should complete Register navigation in less than 1 second', () => {
    const startTime = performance.now()
    
    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )
    
    const registerLink = screen.getByTestId('register-link')
    fireEvent.click(registerLink)
    
    expect(screen.getByTestId('register-page')).toBeInTheDocument()
    
    const endTime = performance.now()
    const navigationTime = endTime - startTime
    
    expect(navigationTime).toBeLessThan(1000)
  })
})

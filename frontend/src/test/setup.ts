import '@testing-library/jest-dom'
import * as vitestAxeMatchers from 'vitest-axe/matchers'
import { expect } from 'vitest'

expect.extend(vitestAxeMatchers)

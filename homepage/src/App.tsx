import { Layout } from './components/layout/Layout'
import { Hero } from './components/sections/Hero'
import { Features } from './components/sections/Features'
import { UsageExample } from './components/sections/UsageExample'
import { GettingStarted } from './components/sections/GettingStarted'

function App() {
  return (
    <Layout>
      <Hero />
      <Features />
      <UsageExample />
      <GettingStarted />
    </Layout>
  )
}

export default App

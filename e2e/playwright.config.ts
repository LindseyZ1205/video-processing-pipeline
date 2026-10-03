import { defineConfig, devices } from '@playwright/test';

// Runs against a stack that is already up (`docker compose up --build`); it doesn't start anything itself.
export default defineConfig({
  testDir: './tests',
  timeout: 90_000,
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: process.env.BASE_URL ?? 'http://localhost:8080',
    trace: 'retain-on-failure',
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
});

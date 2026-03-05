import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  timeout: 60_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  retries: 0,
  reporter: "html",
  use: {
    baseURL: "http://localhost:5173",
    trace: "on-first-retry",
  },
  projects: [
    {
      name: "chromium",
      use: { browserName: "chromium" },
    },
  ],
  webServer: [
    {
      command:
        "cd ../backend && python -m uvicorn main:app --host 127.0.0.1 --port 8000",
      port: 8000,
      reuseExistingServer: true,
      timeout: 30_000,
    },
    {
      command: "npm run dev -- --port 5173",
      port: 5173,
      reuseExistingServer: true,
      timeout: 15_000,
    },
  ],
});

import { expect, test } from '@playwright/test';

test.use({ deviceScaleFactor: 2, viewport: { width: 1440, height: 900 } });

// The README's dashboard image (docs/dashboard.png). It needs the monitoring profile
// (`docker compose --profile monitoring up`) and some traffic first, so it only runs when GRAFANA_URL is set, as CI does.
test('the Grafana dashboard shows the pipeline', async ({ page }) => {
  const grafana = process.env.GRAFANA_URL;
  test.skip(!grafana, 'set GRAFANA_URL to a Grafana started with the monitoring profile');

  await page.goto(`${grafana}/d/pipeline/video-processing-pipeline?orgId=1&from=now-10m&to=now&theme=light&kiosk`);
  await expect(page.getByRole('heading', { name: 'Events per second, by outcome' })).toBeVisible();
  await page.waitForLoadState('networkidle');
  // Every panel has data. CI also runs each panel's query against Prometheus.
  await expect(page.getByText('No data')).toHaveCount(0);
  await page.screenshot({ path: 'screenshots/dashboard.png', fullPage: true });
});

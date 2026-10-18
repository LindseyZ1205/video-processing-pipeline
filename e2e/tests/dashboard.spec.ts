import { expect, test } from '@playwright/test';

// Taller than the whole dashboard, so the screenshot needs no full-page capture. That capture resizes the page, and
// Grafana clears its charts to redraw them at the new size, so they came out blank.
test.use({ deviceScaleFactor: 2, viewport: { width: 1440, height: 1300 } });

// The README's dashboard image (docs/dashboard.png). It needs the monitoring profile
// (`docker compose --profile monitoring up`) and some traffic first, so it only runs when GRAFANA_URL is set, as CI does.
test('the Grafana dashboard shows the pipeline', async ({ page }) => {
  const grafana = process.env.GRAFANA_URL;
  test.skip(!grafana, 'set GRAFANA_URL to a Grafana started with the monitoring profile');

  await page.goto(
    `${grafana}/d/pipeline/video-processing-pipeline?orgId=1&from=now-5m&to=now&theme=light&kiosk&_dash.hideTimePicker`,
  );
  await expect(page.getByRole('heading', { name: 'Events per second, by outcome' })).toBeVisible();
  await page.waitForLoadState('networkidle');
  // Every panel has data. CI also runs each panel's query against Prometheus.
  await expect(page.getByText('No data')).toHaveCount(0);
  // Each of the six time series panels draws on a canvas.
  await expect.poll(() => page.locator('canvas').count()).toBeGreaterThanOrEqual(6);
  await page.waitForTimeout(1_000);

  // Down to the last row of panels, leaving out Grafana's footer at the bottom of the window.
  const lastLegend = await page.getByText('failed SQS polls').boundingBox();
  if (!lastLegend) {
    throw new Error('the last panel has no legend');
  }
  await page.screenshot({
    path: 'screenshots/dashboard.png',
    clip: { x: 0, y: 0, width: 1440, height: lastLegend.y + lastLegend.height + 24 },
  });
});

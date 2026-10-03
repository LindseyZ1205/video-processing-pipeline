import { expect, test } from '@playwright/test';

// The path a visitor takes first: open the demo page, pick a file, watch it go through the pipeline. Unlike the API
// smoke test, this exercises the page's JavaScript and the browser's cross-origin PUT straight to S3 (LocalStack), so
// it fails if CORS or the presigned URL's address is wrong.
test('uploads a file from the browser and shows its transcript', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Video processing pipeline' })).toBeVisible();

  await page.getByLabel('Audio or video file').setInputFiles({
    name: 'browser-test.mp4',
    mimeType: 'video/mp4',
    buffer: Buffer.from('not really a video, but S3 does not mind'),
  });
  await page.getByRole('button', { name: 'Upload and transcribe' }).click();

  const transcript = page.locator('#transcript');
  await expect(transcript).toBeVisible({ timeout: 60_000 });
  await expect(transcript).toContainText('browser-test.mp4');
  await expect(page.locator('#log')).toContainText('Transcribed after 1 attempt(s).');
});

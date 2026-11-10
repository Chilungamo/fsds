// Browser half of tests/test_api.py::test_browser_receives_live_alert.
// Opens the dashboard, reports "ready" once the live stream is connected, then waits for
// a card for a newly detected case to appear without reloading the page.
const { chromium } = require("playwright");

(async () => {
  const url = process.argv[2];
  const expected = process.argv[3];
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  const errors = [];
  page.on("pageerror", (e) => errors.push(e.message));
  page.on("console", (m) => m.type() === "error" && errors.push(m.text()));
  const received = [];
  await page.exposeFunction("__radarEvent", (t) => received.push(t));
  await page.addInitScript(() => {
    const Native = window.EventSource;
    class Spy extends Native {
      constructor(u) {
        super(u);
        ["case.created", "case.updated", "case.confirmed", "case.enriched"].forEach((t) =>
          this.addEventListener(t, (e) => window.__radarEvent(`${t}:${JSON.parse(e.data).case_id}`)),
        );
      }
    }
    window.EventSource = Spy;
  });
  await page.goto(url, { waitUntil: "networkidle" });
  await page.locator(".live.on").waitFor({ timeout: 15000 });
  process.stdout.write("ready\n");
  let appeared = false;
  try {
    // .fresh is only set on cards announced by the live stream
    await page.locator(".card.fresh", { hasText: expected }).waitFor({ timeout: 30000 });
    appeared = true;
  } catch {
    appeared = false;
  }
  const firstCard = appeared ? await page.locator(".card.fresh").first().innerText() : "";
  const cards = await page.locator(".card .card-name").allInnerTexts();
  await browser.close();
  console.log(JSON.stringify({ appeared, errors, firstCard, received, cards }));
})();

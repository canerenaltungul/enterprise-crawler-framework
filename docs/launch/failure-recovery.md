# Failure/recovery demo: sharing kit

These are draft posts for manual publication. The example lives in the GitHub
repository; it is not a new package release or a hosted service.

## Turkish post

Crawler veri çekebiliyor. Peki işlem yarıda kalırsa, boş veri dönerse veya rate
limite takılırsa ne oluyor?

MIT lisanslı Python projem Enterprise Crawler Framework için yerel olarak
denenebilen bir failure/recovery demosu hazırladım:

- HTTP 503 sonrası retry state SQLite'ta kalıyor; yeni worker process devam ediyor.
- Beklenmeyen boş veri başarılı çıktı olarak yazılmıyor.
- HTTP 429'da örnekteki worker batch'i duruyor, sıradaki iş bekliyor.
- Sorun giderildikten sonra seçilen başarısız iş yeniden çalıştırılıyor;
  tamamlanan işler baştan çekilmiyor, hata kaydı korunuyor.

Collection kodunu siz yazıyorsunuz. ECF çevresindeki lifecycle, queue, retry ve
recovery altyapısını sağlıyor. Demodaki hata politikaları uygulama örneği;
otomatik rate-limit çözümü veya exactly-once garantisi değil.

Repo'yu klonlayıp `python -m pip install .` çalıştırdıktan sonra:

`python examples/event_pipeline/failure_recovery_demo.py`

Harici site, hesap veya ayrı sunucu kurulumu gerekmiyor. Kendi collection
fonksiyonunuzu bağlamak için README'deki rehberi kullanabilirsiniz. Denerken
takıldığınız adımı GitHub Issues'a yazmanız geliştirmeye yön verecek.

https://github.com/canerenaltungul/enterprise-crawler-framework

## English post

Your crawler collects data. What happens when a worker stops, a response is
empty, or a source returns a rate limit?

I built a local failure/recovery demo for Enterprise Crawler Framework, my
MIT-licensed Python project:

- HTTP 503 retry state persists in SQLite and resumes in a new worker process.
- Unexpected empty data is rejected before output is written.
- HTTP 429 stops the example's batch, leaving the next job pending.
- After resolving the cause, replay one failed job without recollecting completed
  jobs. The original failure record stays available.

You write the collection logic; ECF supplies lifecycle, queue, retry and recovery
infrastructure around it. These are example application policies, not automatic
rate-limit recovery or an exactly-once guarantee.

Clone the repo, run `python -m pip install .`, then:

`python examples/event_pipeline/failure_recovery_demo.py`

No external website, account or separate server setup. The README links to a
guide for connecting your own collection function. If you get stuck trying it,
please open a GitHub issue with the step and error you encountered.

https://github.com/canerenaltungul/enterprise-crawler-framework

## Terminal recording outline (approximately 45–60 seconds)

Use an installed checkout and run:

```bash
python examples/event_pipeline/failure_recovery_demo.py --pace 8
```

Record the real terminal output; timing depends on the machine. Five pauses add
40 seconds. Keep the command and five verified steps visible, then finish on
the verification message and retained state path.

Suggested narration: “Collection is only part of the job. Here a temporary error
survives a worker restart. Empty output is rejected. A rate limit stops the batch.
After a simulated operator review, only the selected failure is replayed. The
successful job was collected once, and failure history remains available.”

Caption: “Local HTTP fixture • SQLite state • separate worker processes •
example application policies.” This is a recording outline, not a recorded video.

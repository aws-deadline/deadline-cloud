## 0.39.0 (2024-03-06)

### BREAKING CHANGES
* Add hashAlg file extension to files uploaded to CAS (#167) ([`398da18`](https://github.com/casillas2/deadline-cloud/commit/398da18169962967ecf2a257d352ef49a940d5fc))
* **job_attachments**: rename OperatingSystemFamily to StorageProfileOperatingSystemFamily (#192) ([`7743ad8`](https://github.com/casillas2/deadline-cloud/commit/7743ad85586194ac72672c1cd8178fe6705bc402))

### Features
* **cli**: pre-prompt display of download summary (#183) ([`21b7e8b`](https://github.com/casillas2/deadline-cloud/commit/21b7e8ba71ca4ffbce4651cdd713c0a6ba5452e6))

### Bug Fixes
* **job_attachments**: use TransferManager for upload and download (#191) ([`41b5964`](https://github.com/casillas2/deadline-cloud/commit/41b59642a6da2e1dc73a69089b15df80a4ed855f))

## 0.38.0 (2024-02-16)

### BREAKING CHANGES
* **JobAttachments**: Add &#39;last seen on S3&#39; cache (#172) ([`99ebaea`](https://github.com/casillas2/deadline-cloud/commit/99ebaea3c2564d6b047c9f7a15caf095f8e80cf7))
* Validate paths for Job Bundles (#171) ([`278e4f6`](https://github.com/casillas2/deadline-cloud/commit/278e4f679b7e5a063206e44a0ecb41bd41b9f17c))

### Features
* Support Deadline Cloud Monitor migration away from Studios (#179) ([`800b44d`](https://github.com/casillas2/deadline-cloud/commit/800b44d45a3bd80fa27e35268f55b280e6610351))
* **cli**: add log message for no output in download-output command (#99) ([`4269e11`](https://github.com/casillas2/deadline-cloud/commit/4269e115718650f41c31cc90d711f5596bbde8e5))

### Bug Fixes
* **JobAttachments**: Ignore empty lists for job attachments (#181) ([`7c63a75`](https://github.com/casillas2/deadline-cloud/commit/7c63a7553a96b3211d721a3a3a212020b7949d0e))
* Removing references to Fus3 (#178) ([`e46cab7`](https://github.com/casillas2/deadline-cloud/commit/e46cab78a476a97a68cb8b790c6c7a0d8ce8a753))
* Removing VFS termination from sync_outputs (#175) ([`ef782bf`](https://github.com/casillas2/deadline-cloud/commit/ef782bffad18786d58363ed4bc339d4b4a237479))
* Allow empty job parameters from the CLI ([`602322b`](https://github.com/casillas2/deadline-cloud/commit/602322b5cf72cf2456a29bfe4f9c92ee9f12bbae))


## 0.41.0 (2024-03-19)

### BREAKING CHANGES
* **job_attachments**: remove local storage of manifest files (#207) ([`8c5ea38`](https://github.com/casillas2/deadline-cloud/commit/8c5ea38946fa89d7248fd406ab1ba5fa6298775d))
* Remove the special-case deadline endpoint logic ([`e936938`](https://github.com/casillas2/deadline-cloud/commit/e936938cf21fb3c989701a4569388df818d71422))
* **job_attachment**: remove `os_group` field from Windows filesystem permission settings (#215) ([`739cb20`](https://github.com/casillas2/deadline-cloud/commit/739cb208978a5573772779932bb309cee57f0687))

### Features
* **job_attachments**: enhance handling S3 timeout errors and BotoCoreError (#206) ([`24fe21c`](https://github.com/casillas2/deadline-cloud/commit/24fe21c4bd0d579d6b5a56c0922ea00d8e4425d4))
* Add telemetry opt-out env var (#212) ([`4f270ba`](https://github.com/casillas2/deadline-cloud/commit/4f270bad4e1179dbea43c309d4c16b276551ef36))
* Add UI for custom host worker capability requirements (#186) ([`c5bbcd3`](https://github.com/casillas2/deadline-cloud/commit/c5bbcd3252d8c1e40e26c14db0392897d7333417))
* Adds common data to telemetry events (#205) ([`7c2522c`](https://github.com/casillas2/deadline-cloud/commit/7c2522ca480f5b6220479f30a790ab8a468851f4))

### Bug Fixes
* **job_attachments**: Use files&#39; last modification time to identify output files to be synced (#211) ([`1688c5b`](https://github.com/casillas2/deadline-cloud/commit/1688c5bdf6a3f8b36408635acc2db7475cb401e9))
* clear storage profiles in gui submitter (#204) ([`7223195`](https://github.com/casillas2/deadline-cloud/commit/7223195bf763dafd0613d9626e4903fb5729766d))

## 0.40.0 (2024-03-11)

### BREAKING CHANGES
* **job_attachment**: use username instead of group for Windows file permissions setting (#196) ([`4c092bb`](https://github.com/casillas2/deadline-cloud/commit/4c092bbe926dbc599f655806bb9f6f5c0aa7ea50))
* rename creds -&gt; auth, credentials -&gt; authentication (#199) ([`66126a1`](https://github.com/casillas2/deadline-cloud/commit/66126a1864dfed81e7c8b3187c1fe51090d23731))

### Features
* keep standalone gui open after submission (#198) ([`3b8d907`](https://github.com/casillas2/deadline-cloud/commit/3b8d907b63085f02a85a87c9df0df18d9f7aca3f))

### Bug Fixes
* **job_attachments**: improvements to nonvalid error messages (#200) ([`148587a`](https://github.com/casillas2/deadline-cloud/commit/148587aa9be55248d5d56e55e4eb44ada912cbc8))
* **ui**: resource listing ignores case (#194) ([`223026a`](https://github.com/casillas2/deadline-cloud/commit/223026aeae096d9fcb55aaa39470c93df9b51b71))

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


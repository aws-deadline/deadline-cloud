## 0.49.3 (2024-11-29)


### Features
* allow attachments commands to read openjd path mapping, allow upload with given manifest name and attach metadata (#515) ([`1a0f711`](https://github.com/aws-deadline/deadline-cloud/commit/1a0f711a036001814f5b3b8b5f6a7d5671a03b67))
* add CLI config clear command (#513) ([`43cd362`](https://github.com/aws-deadline/deadline-cloud/commit/43cd3623f3bb6cd2c0295205fc61f5677975dd51))

### Bug Fixes
* use description from GUI submitter (#517) ([`f585ca5`](https://github.com/aws-deadline/deadline-cloud/commit/f585ca577afd1ee5e0e140590a43fbbc20262bf7))

## 0.49.2 (2024-11-21)

### Bug Fixes
* Updated PySide-essentials range. (#511) ([`4f6134e`](https://github.com/aws-deadline/deadline-cloud/commit/4f6134ed50ea7f127316dea754974c2426f34929))


## 0.49.1 (2024-11-19)

### Bug Fixes
* revert: "chore(deps): update pyside6-essentials requirement (#470)" ([`61e5fa1`](https://github.com/aws-deadline/deadline-cloud/commit/61e5fa1bc1101944cc3224179af641961afdc090))
* revert: "chore(deps): update pyinstaller requirement from ==5.13.* to ==6.11.* (#487)" ([`01b3464`](https://github.com/aws-deadline/deadline-cloud/commit/01b346454e527725130f6e0db3da4264b48e1960))


## 0.49.0 (2024-11-18)


### BREAKING CHANGES
* Refactor manifest aggregation, add helper to persist manifests and check disk capacity. Removes `aggregate_asset_root_manifests` public interface. (#483) ([`f57f637`](https://github.com/aws-deadline/deadline-cloud/commit/f57f637d78c2b5643a39b1c3b6190cbae745c079))

### Features
* **JA**: Add manifest upload and download to complete the JA standalone API+CLI featureset. ([`b5b203b`](https://github.com/aws-deadline/deadline-cloud/commit/b5b203bf5b370cb5e51b3e60f6b052e87e11d6d7))
* refactor manifest aggregation, add helper to persist manifests and check disk capacity (#483) ([`f57f637`](https://github.com/aws-deadline/deadline-cloud/commit/f57f637d78c2b5643a39b1c3b6190cbae745c079))
* **JA**: Add force-rehash option to snapshot command. Move Integ Tsts for Diff and Snapshot to Integ ([`276edae`](https://github.com/aws-deadline/deadline-cloud/commit/276edaea9cc11f8a0800a9f336f9e0d2057ff53b))
* **JA**: Implementation for Job Attachments diff command (#465) ([`97e8bc4`](https://github.com/aws-deadline/deadline-cloud/commit/97e8bc45a21b3e88bc37ecefb42b92055ecd7ecf))

### Bug Fixes
* state upperbound (3.12) for python version support (#501) ([`f35765f`](https://github.com/aws-deadline/deadline-cloud/commit/f35765ff294e4af8d9e12c6b19897bb079d3355b))
* De-duplicate error messages on expired credentials ([`cfc7759`](https://github.com/aws-deadline/deadline-cloud/commit/cfc77592edc472442950d0539ee1093cee534a59))
* Refresh queue parameter defaults when loading a new job bundle ([`5abcef1`](https://github.com/aws-deadline/deadline-cloud/commit/5abcef1b2b88bcf189786ed7fa6ca826ee536ed6))
* The hash_cache fails with filenames including surrogates (#492) ([`39c285d`](https://github.com/aws-deadline/deadline-cloud/commit/39c285d2e8bc3638a9f88f14ccd8c234b49cb2c7))
* Job parameter values dropped when merging with queue parameter ([`73e1b24`](https://github.com/aws-deadline/deadline-cloud/commit/73e1b2494883885dda0752a7b514235edabdb2a4))
* pass in cache directory for attachment upload (#476) ([`1f776f6`](https://github.com/aws-deadline/deadline-cloud/commit/1f776f6fbe6643094ee565f75a4bd1eb06d3c89c))

## 0.48.9 (2024-10-10)


### Features
* Add a simple backoff and retry utility helper (#452) ([`88a4ef6`](https://github.com/aws-deadline/deadline-cloud/commit/88a4ef6908c7bfba1b5226ea5fc3464d6e0a5438))
* adds --storage-profile-id option to the bundle submit command (#442) ([`8c105ed`](https://github.com/aws-deadline/deadline-cloud/commit/8c105ed2129e64ca71617e6eaf0984a87f1dfd3b))
* support --submitter-name option for bundle GUI submitter command (#416) ([`569af3b`](https://github.com/aws-deadline/deadline-cloud/commit/569af3b8be23cc09386631f472fd8bfffa4c6315))
* update DEVELOPMENT.md on how to run integ tests. (#419) ([`3fe65e6`](https://github.com/aws-deadline/deadline-cloud/commit/3fe65e66675b94d53a64ee2b905e065f22b4f102))

### Bug Fixes
* ctypes (libffi) is no longer required on linux (#455) ([`6446b66`](https://github.com/aws-deadline/deadline-cloud/commit/6446b66d2038bc62f8b7bc393e2629ed2a488f2d))
* running parallel bundle submits no longer clobbers config file (#444) ([`609e027`](https://github.com/aws-deadline/deadline-cloud/commit/609e0277675e2bb3d3fd57e94345bcd0a4be754d))
* improve help text (#436) ([`343bf7b`](https://github.com/aws-deadline/deadline-cloud/commit/343bf7b18e1d32cb50445f0a9f28d8ad4825dbae))
* credential caching improvements (#431) ([`2a0c487`](https://github.com/aws-deadline/deadline-cloud/commit/2a0c4878d4e85042103ee15070fa5d384891f8a7))
* use devnull for stderr pipe to dcm process handle (#421) ([`c333452`](https://github.com/aws-deadline/deadline-cloud/commit/c3334521322c59c8875ff3f313441ace0558b97d))

## 0.48.8 (2024-07-24)


### Features
* support skipping prompts for installing GUI dependencies (#395) ([`74293ac`](https://github.com/aws-deadline/deadline-cloud/commit/74293acadd15d5e9437f94d8a8e8b8d908a84d2c))
* record Success-Fail telemetry event on job attachments upload. (#393) ([`9bc590b`](https://github.com/aws-deadline/deadline-cloud/commit/9bc590b1fc17bf3c6e33d6b46e4231d5e252526b))

### Bug Fixes
* Fixed the job submission example (#415) ([`17eadb7`](https://github.com/aws-deadline/deadline-cloud/commit/17eadb75844794e9d8a1cab8f40db8bb6231ad52))
* Add better log when JA hit Windows path length limit (#403) ([`0344537`](https://github.com/aws-deadline/deadline-cloud/commit/0344537a2d6ccdea3a17596f28e263edaff39879))

## 0.48.7 (2024-07-03)


### Features
* support JSON output from bundle gui-submit (#380) ([`eb9acb0`](https://github.com/aws-deadline/deadline-cloud/commit/eb9acb0147494ae910ec92cef91ace589f7e8b74))

## 0.48.6 (2024-06-26)


### Bug Fixes
* revert: "feat: support JSON output in bundle GUI submitter (#357)" ([`8d6dc62`](https://github.com/aws-deadline/deadline-cloud/pull/374/commits/8d6dc62ae35679461831b2e64d3e21cb43b9f116))

## 0.48.5 (2024-06-24)


### Features
* support JSON output in bundle GUI submitter (#357) ([`aad9a49`](https://github.com/aws-deadline/deadline-cloud/commit/aad9a49c9085e67031abc3ce342ca8068a6508d1))

### Bug Fixes
* bundle gui-submit fails loading bundles with saved queue parameter values (#360) ([`a2c1f2d`](https://github.com/aws-deadline/deadline-cloud/commit/a2c1f2ddb697f3e6ac8f17d443ad5618674216a6))
* use sids when granting permissions with icacls (#359) ([`133b059`](https://github.com/aws-deadline/deadline-cloud/commit/133b05938288afbdc11b3dbfe11c8c98349f81b3))
* bundles are stored in job_history in their original format (#344) ([`b5de504`](https://github.com/aws-deadline/deadline-cloud/commit/b5de504918cc6420d80d5206f9332cb8880471a5))

## 0.48.4 (2024-06-03)



### Bug Fixes
* **cli**: correct download-output command displaying resolved UNC path (#337) ([`6bcd9a2`](https://github.com/aws-deadline/deadline-cloud/commit/6bcd9a2285999d4b2fe0ba72d28f04fa0c6e6db4))

## 0.48.3 (2024-05-27)



### Bug Fixes
* reverts using AWS CRT for faster transfers (#333) ([`2798190`](https://github.com/aws-deadline/deadline-cloud/commit/2798190c594610131855ea7d1bbb1c2adf7d8fb0))

## 0.48.2 (2024-05-22)


### Features
* use AWS CRT for faster transfers (#319) ([`52da0ea`](https://github.com/aws-deadline/deadline-cloud/commit/52da0ea8816df2f39ec24fd35646b27f978a0891))

### Bug Fixes
* bundle submit parameter processing splits name/value at right-most = (#331) ([`09bead0`](https://github.com/aws-deadline/deadline-cloud/commit/09bead08be81f016e84408aead589fd31d2a6f01))
* Prevent submission dialog sometimes closing on exceptions (#329) ([`3e369f9`](https://github.com/aws-deadline/deadline-cloud/commit/3e369f9a9f288f159f84eda1afc31a01f3305779))
* Install boto3 CRT extra feature always (#328) ([`dffc71e`](https://github.com/aws-deadline/deadline-cloud/commit/dffc71e0c113437cfe3da5c934360db3573ad77c))

## 0.48.1 (2024-05-06)



### Bug Fixes
* remove deprecated .aws/sso/cache watcher (#322) ([`72d6c26`](https://github.com/aws-deadline/deadline-cloud/commit/72d6c26b768c09c1caa4301bebc4db61bdbb7861))
* fix Deadline Cloud Monitor to be lower cased monitor ([`a3f924e`](https://github.com/aws-deadline/deadline-cloud/commit/a3f924e1514c0a51e68b498e8a48900a7b1e1c9b))
* Fix typos in error messages ([`1ab3ccc`](https://github.com/aws-deadline/deadline-cloud/commit/1ab3ccc32c6fd53bc558d0fea54dd241735a6e88))

## 0.48.0 (2024-04-25)

### BREAKING CHANGES
* Improve handling of misconfigured input job attachments (that are not within any locations for the submission machines’s configured storage profile), handle empty/non-existent paths and add them to asset references, add `require_paths_exist` option (#309) ([`f8d5826`](https://github.com/aws-deadline/deadline-cloud/commit/f8d5826316cbaae1a41d11c2decad38a4ab5ca5d))
* **job_attachments**: use correct profile for GetStorageProfileForQueue API (#296) ([`a8de5f6`](https://github.com/aws-deadline/deadline-cloud/commit/a8de5f679a7b7da53ce83ab1ba25cacded06773f))



## 0.47.3 (2024-04-16)



### Bug Fixes
* Use correct paths on windows for local manifest file (#301) ([`c691be1`](https://github.com/aws-deadline/deadline-cloud/commit/c691be1c043380c37cefb56411654b0bf63db0df))
* **job_attachments**: fix integration test exception (#298) ([`3c700b0`](https://github.com/aws-deadline/deadline-cloud/commit/3c700b00f1c20097fd178bbffac9b635c7bec3bc))
* **job_attachments**: fix output syncing when using identically named local File System Locations across different OS (#295) ([`7fcf845`](https://github.com/aws-deadline/deadline-cloud/commit/7fcf845d84f0e6d5776bca5b0c810b55fd14f325))
* Write job attachment manifests locally when submitting ([`70958f5`](https://github.com/aws-deadline/deadline-cloud/commit/70958f5e583e9c5ba0cfda3fe7a53c8dca13b7a6))
* Throw error in sync-inputs if total input size is too large (#290) ([`4d40b8c`](https://github.com/aws-deadline/deadline-cloud/commit/4d40b8c75ac6a38a40a4028793739331206e9d2a))

## 0.47.2 (2024-04-07)



### Bug Fixes
* **job_attachments**: handle case-insensitive path extraction on Windows (#287) ([`7c3cc3d`](https://github.com/aws-deadline/deadline-cloud/commit/7c3cc3dfa4a861c22fc3e212a5a55132e5386820))
* **job_attachments**: pass original exception to AssetSyncError (#285) ([`c1707b3`](https://github.com/aws-deadline/deadline-cloud/commit/c1707b311e5e3cdfd28bca349920e9d62c7dabef))
* set QT_API to pyside6 or pyside2 for deadline-cli (#284) ([`e6ca757`](https://github.com/aws-deadline/deadline-cloud/commit/e6ca757fe027a76f73b93583e5a8ab2ac6af6c9e))

## 0.47.1 (2024-04-02)



### Bug Fixes
* catch on correct exception type if downloading with no outputs (#278) ([`dafa5a8`](https://github.com/aws-deadline/deadline-cloud/commit/dafa5a85fa37bd2fe71c5cfa66cffa19ad76994f))

## 0.47.0 (2024-04-01)

### BREAKING CHANGES
* public release (#265) ([`e8680c6`](https://github.com/aws-deadline/deadline-cloud/commit/e8680c63a35a4c1eb3736f3ec537c16ec53c9b74))
* python 3.8 or higher is required


### Bug Fixes
* only hookup stdin on windows for dcm login (#271) ([`cb91b2c`](https://github.com/aws-deadline/deadline-cloud/commit/cb91b2c5831301ee4f3f6022004fbc98bc992ab0))
* Move telemetry urllib3 context to initialize function (#263) ([`aebd13b`](https://github.com/aws-deadline/deadline-cloud/commit/aebd13b11ad444e9edd2dcb35cc829464a722c6e))
* only load queue environments when connected to a queue (#264) ([`ea1e617`](https://github.com/aws-deadline/deadline-cloud/commit/ea1e61776fa8483fa35873ffc276914c15514c3a))
* bringing vfs_cache env var into vfs launch environment (#262) ([`1a6b8c8`](https://github.com/aws-deadline/deadline-cloud/commit/1a6b8c8a9070d4389e65e4d5b2694f32401050ef))

## 0.46.0 (2024-03-28)

### BREAKING CHANGES
* move VFS Logs to under sessionfolder/.vfs_logs (#259) ([`28e16bb`](https://github.com/casillas2/deadline-cloud/commit/28e16bbce5a70ec651eeab89d8ae0f31a58541fd))



## 0.45.3 (2024-03-28)



### Bug Fixes
* add missing import to pyinstaller spec (#256) ([`a81694f`](https://github.com/casillas2/deadline-cloud/commit/a81694f5fd1b87e408df14534b74ee9680e6f8ed))
* include python3.dll in windows pyinstaller builds (#255) ([`a63f3ae`](https://github.com/casillas2/deadline-cloud/commit/a63f3ae85368ea73aac040a9c7bb21870026372c))
* Update config command help text (#254) ([`98b14c7`](https://github.com/casillas2/deadline-cloud/commit/98b14c710c4373f1c46c2a652bb5a21a7c7c77be))
* Use botocore ssl context for telemetry requests (#253) ([`6a6b114`](https://github.com/casillas2/deadline-cloud/commit/6a6b114f67faf992e544d8cc8957a8ec85f94327))
* Fix storage profiles list broken osFamily matching (#252) ([`c8151db`](https://github.com/casillas2/deadline-cloud/commit/c8151db55eec6c0e7878e9a816fa163edac7f7df))

## 0.45.2 (2024-03-26)



### Bug Fixes
* **job-attachments**: remove dependency on pywin32 for submission code (#250) ([`30b44df`](https://github.com/casillas2/deadline-cloud/commit/30b44dfec56f89bce486b1a3e5dc461ed42a0232))

## 0.45.1 (2024-03-26)



### Bug Fixes
* Removing overridden AWS_CONFIG_FILE path and base environment variables from deadline_vfs POpen launch env and using -E option to persist environment through sudo (#247) ([`4a7be81`](https://github.com/casillas2/deadline-cloud/commit/4a7be8131e7af99cfe2e0b8e6459591079f27154))

## 0.45.0 (2024-03-25)

### BREAKING CHANGES
* revert &#34;feat!: prep for rootPathFormat becoming ALL UPPERS (#222)&#34; (#243) ([`9de687e`](https://github.com/casillas2/deadline-cloud/commit/9de687e5256634165c2e73e42da03acd3974c539))

### Features
* **job_attachment**: reject files on non-Windows systems that do not support O_NOFOLLOW (#242) ([`9e23b81`](https://github.com/casillas2/deadline-cloud/commit/9e23b81535e769946610c82b19d12e5922abcaf0))


## 0.44.2 (2024-03-25)


### Features
* prevent uploading files outside session directory via symlinks (#225) ([`3c3a4fa`](https://github.com/casillas2/deadline-cloud/commit/3c3a4facd4118082afdf028076c3966eea7463b8))

### Bug Fixes
* VFS Disk Cache Group Permissions, Merged Manifests Folder, is_mount checks (#235) ([`30dac3d`](https://github.com/casillas2/deadline-cloud/commit/30dac3d8c09c67fd2ca69e30841e634e4fb2b3b2))

## 0.44.1 (2024-03-24)



### Bug Fixes
* Use boto SSL for telemetry requests, add opt out settings in UI (#230) ([`b678086`](https://github.com/casillas2/deadline-cloud/commit/b678086a6b90da4904ed5b1f84e3a410369641d1))
* swap exec to exec_ (#234) ([`b3853c2`](https://github.com/casillas2/deadline-cloud/commit/b3853c22808de730f99c7ef322b713cc05d7b878))

## 0.44.0 (2024-03-23)


### Features
* make os_user optional in cleanup_session (#232) ([`241d12b`](https://github.com/casillas2/deadline-cloud/commit/241d12bc484299614c1d1ebec6c4366e125d0c78))


## 0.43.0 (2024-03-23)

### BREAKING CHANGES
* Switch to running deadline_vfs as os_user (#223) ([`cf9c2d2`](https://github.com/casillas2/deadline-cloud/commit/cf9c2d29c4e5e90055f0bfcca13e6928613c1c35))
* use qtpy and add support for pyside6 (#202) ([`deb2cca`](https://github.com/casillas2/deadline-cloud/commit/deb2ccabe00f7c97d65216ee91a18b9535b1d5f6))

### Features
* enable cache for VFS (#209) ([`91dfa83`](https://github.com/casillas2/deadline-cloud/commit/91dfa83594cde8121ec7dd1621dc4cace86bfb2e))

### Bug Fixes
* Mock STS calls for some JA upload tests (#229) ([`119aabd`](https://github.com/casillas2/deadline-cloud/commit/119aabdc115d35371e80a733bc4ed49acb19753a))
* record attachments mtimes after mounting vfs ([`f0dcfa3`](https://github.com/casillas2/deadline-cloud/commit/f0dcfa314da8962e7bc089961265de0c63277ad6))

## 0.42.0 (2024-03-21)

### BREAKING CHANGES
* prep for rootPathFormat becoming ALL UPPERS (#222) ([`d49c885`](https://github.com/casillas2/deadline-cloud/commit/d49c885efe3b97b79d1eca3dfaaac472bf85aaf2))


### Bug Fixes
* Make StorageProfileOperatingSystemFamily enum case-insensitive ([`0da921c`](https://github.com/casillas2/deadline-cloud/commit/0da921c5f46b3c63aff3a8fdcd892701447900b6))

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


# Changelog
All notable changes to **MictlanX** project will be documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).






## [0.1.0a0] - 2025-08-27
### 🔥 Breaking changes
- **Package layout flattened.** All code previously under `mictlanx.v4.*` has been removed.  
  Import paths now start at `mictlanx.*`.
**Most common migrations**
  - `from mictlanx.v4.client import AsyncPeer` ➜ `from mictlanx.services import AsyncPeer`
  - `from mictlanx.v4.errors import MictlanXError` ➜ `from mictlanx.errors import MictlanXError`
  - `from mictlanx.v4.types import VerifyType` ➜ `from mictlanx.types import VerifyType`
  - `from mictlanx.v4.interfaces import responses` ➜ `from mictlanx.interfaces import responses`
  - `from mictlanx.v4.utils import Utils` ➜ `from mictlanx.utils import Utils`

  **Old ➜ New module map (non-exhaustive)**
  - `mictlanx/v4/client.py` ➜ `mictlanx/services` (e.g., `AsyncPeer`)
  - `mictlanx/v4/errors.py` ➜ `mictlanx/errors`
  - `mictlanx/v4/interfaces/*` ➜ `mictlanx/interfaces/*`
  - `mictlanx/v4/types.py` ➜ `mictlanx/types`
  - `mictlanx/v4/utils.py` ➜ `mictlanx/utils`
  - `mictlanx/v4/logger*` ➜ `mictlanx/logger`
  - `mictlanx/v4/retry*` ➜ `mictlanx/retry`
  - `mictlanx/v4/ipc*` ➜ `mictlanx/ipc`
  - `mictlanx/v4/asyncx*` ➜ `mictlanx/asyncx`
  - (new) `mictlanx/apac`, `mictlanx/summoner` top-level packages

- **Deprecated code removed.** Any imports from `mictlanx.v4` will fail. Update your imports as above.


### Added
- Community Standards files

- **URI utilities**: `MictlanXURI` to parse/build `mictlanx://` connection URIs and instantiate routers.
  ```python
  from mictlanx.utils.uri import MictlanXURI
  from mictlanx.services import AsyncRouter
  from typing import List 

  routers:List[AsyncRouters] = MictlanXURI.build(
      "mictlanx://r0@localhost:60666,r1@localhost:60667?protocol=http&api_version=4&http2=0"
  )

- Richer network error taxonomy in mictlanx.errors:

- NetworkError (1000), ConnectFailedError (1001), DNSResolutionError (1002),
RequestTimeoutError (1004), UpstreamProtocolError (1005).

- MictlanXError.from_exception() now unwraps httpx root causes and includes endpoint details.

### [Removed]
- Useless code in different location of the project.

### [Deprecated] 
- ```mictlanx/v4``` imports are no longer supported

### Older releases (no detailed changelog)

- **0.0.162** — Aug 26, 2025
- **0.0.161a40** (pre-release) — Aug 5, 2025
- **0.0.161a39** (pre-release) — Jul 13, 2025
- **0.0.161a38** (pre-release) — Jul 12, 2025
- **0.0.161a37** (pre-release) — Jul 10, 2025
- **0.0.161a36** (pre-release) — Jun 23, 2025
- **0.0.161a35** (pre-release) — Jun 22, 2025
- **0.0.161a34** (pre-release) — Jun 21, 2025
- **0.0.161a33** (pre-release) — Jun 15, 2025
- **0.0.161a32** (pre-release) — Jun 15, 2025
- **0.0.161a31** (pre-release) — May 16, 2025
- **0.0.161a30** (pre-release) — May 15, 2025
- **0.0.161a29** (pre-release) — Apr 22, 2025
- **0.0.161a28** (pre-release) — Apr 22, 2025
- **0.0.161a27** (pre-release) — Apr 20, 2025
- **0.0.161a26** (pre-release) — Apr 19, 2025
- **0.0.161a25** (pre-release) — Apr 19, 2025
- **0.0.161a24** (pre-release) — Apr 19, 2025
- **0.0.161a23** (pre-release) — Apr 19, 2025
- **0.0.161a22** (pre-release) — Apr 19, 2025
- **0.0.161a21** (pre-release) — Apr 19, 2025
- **0.0.161a20** (pre-release) — Apr 18, 2025
- **0.0.161a19** (pre-release) — Apr 17, 2025
- **0.0.161a18** (pre-release) — Apr 12, 2025
- **0.0.161a17** (pre-release) — Apr 3, 2025
- **0.0.161a16** (pre-release) — Mar 19, 2025
- **0.0.161a15** (pre-release) — Mar 18, 2025
- **0.0.161a14** (pre-release) — Mar 18, 2025
- **0.0.161a13** (pre-release) — Mar 16, 2025
- **0.0.161a12** (pre-release) — Mar 16, 2025
- **0.0.161a11** (pre-release) — Mar 16, 2025
- **0.0.161a10** (pre-release) — Mar 16, 2025
- **0.0.161a9** (pre-release) — Mar 16, 2025
- **0.0.161a8** (pre-release) — Mar 16, 2025
- **0.0.161a7** (pre-release) — Mar 16, 2025
- **0.0.161a6** (pre-release) — Mar 16, 2025
- **0.0.161a5** (pre-release) — Mar 16, 2025
- **0.0.161a4** (pre-release) — Mar 15, 2025
- **0.0.161a3** (pre-release) — Mar 4, 2025
- **0.0.161a2** (pre-release) — Feb 24, 2025
- **0.0.161a1** (pre-release) — Feb 23, 2025
- **0.0.161a0** (pre-release) — Feb 23, 2025
- **0.0.160** — Jul 25, 2024
- **0.0.160a6** (pre-release) — Feb 20, 2025
- **0.0.160a5** (pre-release) — Feb 19, 2025
- **0.0.160a4** (pre-release) — Feb 19, 2025
- **0.0.160a3** (pre-release) — Sep 27, 2024
- **0.0.159** — Jul 15, 2024
- **0.0.158** — Jul 12, 2024
- **0.0.157** — Jul 3, 2024
- **0.0.156** — Jun 20, 2024
- **0.0.155** — Jun 20, 2024
- **0.0.154** — Jun 20, 2024
- **0.0.153** — Jun 20, 2024
- **0.0.152** — Jun 19, 2024
- **0.0.151** — Jun 18, 2024
- **0.0.150** — Jun 16, 2024
- **0.0.149** — Jun 16, 2024
- **0.0.148** — Jun 14, 2024
- **0.0.147** — Jun 13, 2024
- **0.0.146** — Jun 13, 2024
- **0.0.145** — Jun 12, 2024
- **0.0.144** — Jun 10, 2024
- **0.0.143** — Jun 7, 2024
- **0.0.142** — May 31, 2024
- **0.0.141** — May 16, 2024
- **0.0.140** — May 14, 2024
- **0.0.139** — May 6, 2024
- **0.0.138** — May 4, 2024
- **0.0.137** — May 3, 2024
- **0.0.136** — May 3, 2024
- **0.0.135** — May 2, 2024
- **0.0.134** — Apr 26, 2024
- **0.0.133** — Apr 22, 2024
- **0.0.132** — Apr 17, 2024
- **0.0.131** — Apr 8, 2024
- **0.0.130** — Apr 8, 2024
- **0.0.129** — Apr 7, 2024
- **0.0.128** — Mar 24, 2024
- **0.0.127** — Mar 24, 2024
- **0.0.126** — Mar 24, 2024
- **0.0.125** — Mar 24, 2024
- **0.0.124** — Mar 16, 2024
- **0.0.123** — Mar 5, 2024
- **0.0.122** — Mar 5, 2024
- **0.0.121** — Mar 4, 2024
- **0.0.120** — Mar 2, 2024
- **0.0.119** — Mar 2, 2024
- **0.0.118** — Mar 2, 2024
- **0.0.117** — Mar 2, 2024
- **0.0.116** — Feb 29, 2024
- **0.0.115** — Feb 29, 2024
- **0.0.114** — Feb 27, 2024
- **0.0.113** — Feb 27, 2024
- **0.0.112** — Feb 27, 2024
- **0.0.111** — Feb 27, 2024
- **0.0.110** — Feb 18, 2024
- **0.0.109** — Feb 16, 2024
- **0.0.108** — Feb 15, 2024
- **0.0.107** — Feb 11, 2024
- **0.0.106** — Feb 10, 2024
- **0.0.105** — Feb 8, 2024
- **0.0.104** — Feb 8, 2024
- **0.0.103** — Feb 8, 2024
- **0.0.102** — Jan 29, 2024
- **0.0.101** — Jan 29, 2024
- **0.0.100** — Jan 28, 2024
- **0.0.99** — Jan 28, 2024
- **0.0.98** — Jan 28, 2024
- **0.0.97** — Jan 24, 2024
- **0.0.96** — Jan 24, 2024
- **0.0.95** — Jan 24, 2024
- **0.0.94** — Jan 23, 2024
- **0.0.93** — Jan 21, 2024
- **0.0.92** — Jan 21, 2024
- **0.0.91** — Jan 21, 2024
- **0.0.90** — Jan 21, 2024
- **0.0.89** — Jan 21, 2024
- **0.0.88** — Jan 21, 2024
- **0.0.87** — Jan 20, 2024
- **0.0.86** — Jan 17, 2024
- **0.0.85** — Jan 17, 2024
- **0.0.84** — Jan 17, 2024
- **0.0.83** — Jan 17, 2024
- **0.0.82** — Jan 17, 2024
- **0.0.81** — Dec 21, 2023
- **0.0.80** — Nov 21, 2023
- **0.0.79** — Nov 21, 2023
- **0.0.78** — Nov 20, 2023
- **0.0.77** — Nov 18, 2023
- **0.0.76** — Nov 16, 2023
- **0.0.75** — Nov 12, 2023
- **0.0.74** — Nov 11, 2023
- **0.0.73** — Nov 11, 2023
- **0.0.72** — Nov 10, 2023
- **0.0.71** — Nov 9, 2023
- **0.0.70** — Nov 9, 2023
- **0.0.69** — Nov 9, 2023
- **0.0.68** — Oct 14, 2023
- **0.0.67** — Oct 3, 2023
- **0.0.66** — Oct 2, 2023
- **0.0.65** — Oct 2, 2023
- **0.0.64** — Oct 2, 2023
- **0.0.63** — Oct 2, 2023
- **0.0.62** — Oct 1, 2023
- **0.0.61** — Sep 30, 2023
- **0.0.60** — Sep 29, 2023
- **0.0.59** — Sep 28, 2023
- **0.0.58** — Sep 28, 2023
- **0.0.57** — Sep 23, 2023
- **0.0.56** — Sep 22, 2023
- **0.0.55** — Sep 22, 2023
- **0.0.54** — Sep 14, 2023
- **0.0.53** — Sep 12, 2023
- **0.0.52** — Sep 12, 2023
- **0.0.51** — Sep 12, 2023
- **0.0.50** — Sep 10, 2023
- **0.0.49** — Sep 6, 2023
- **0.0.48** — Sep 6, 2023
- **0.0.47** — Sep 6, 2023
- **0.0.46** — Sep 5, 2023
- **0.0.45** — Sep 5, 2023
- **0.0.44** — Sep 3, 2023
- **0.0.43** — Aug 19, 2023
- **0.0.42** — Jul 26, 2023
- **0.0.41** — Jun 27, 2023
- **0.0.40** — Jun 26, 2023
- **0.0.39** — Jun 20, 2023
- **0.0.38** — Jun 10, 2023
- **0.0.37** — May 18, 2023
- **0.0.36** — May 18, 2023
- **0.0.35** — May 15, 2023
- **0.0.34** — May 14, 2023
- **0.0.33** — May 6, 2023
- **0.0.32** — Apr 23, 2023
- **0.0.31** — Apr 23, 2023
- **0.0.30** — Apr 22, 2023
- **0.0.29** — Apr 22, 2023
- **0.0.28** — Apr 21, 2023
- **0.0.27** — Apr 21, 2023
- **0.0.26** — Mar 19, 2023
- **0.0.25** — Mar 19, 2023
- **0.0.24** — Mar 19, 2023
- **0.0.23** — Mar 17, 2023
- **0.0.22** — Mar 17, 2023
- **0.0.22a0** (pre-release) — Mar 17, 2023
- **0.0.21** — Mar 17, 2023
- **0.0.20** — Mar 16, 2023
- **0.0.19** — Mar 14, 2023
- **0.0.18** — Mar 12, 2023
- **0.0.17** — Feb 25, 2023
- **0.0.16** — Feb 23, 2023
- **0.0.15** — Feb 17, 2023
- **0.0.14** — Feb 16, 2023
- **0.0.13** — Feb 16, 2023
- **0.0.12** — Feb 16, 2023
- **0.0.11** — Feb 16, 2023
- **0.0.10** — Feb 16, 2023
- **0.0.9** — Feb 16, 2023
- **0.0.8** — Feb 16, 2023
- **0.0.7** — Feb 15, 2023
- **0.0.5** — Feb 15, 2023
- **0.0.4** — Feb 15, 2023
- **0.0.3** — Feb 15, 2023
- **0.0.2** — Feb 15, 2023
- **0.0.1** — Feb 15, 2023
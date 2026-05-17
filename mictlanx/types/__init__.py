"""Type aliases used across the MictlanX client SDK."""

from typing import Union
import ssl

VerifyType = Union[ssl.SSLContext, str, bool]
"""SSL verification option for httpx requests.

Accepted values mirror httpx's ``verify`` parameter:

* ``False`` — disable certificate verification (useful for self-signed
  certs in local development).
* ``True`` — verify using the system CA bundle.
* ``str`` — path to a custom CA bundle file.
* ``ssl.SSLContext`` — a pre-configured SSL context.
"""
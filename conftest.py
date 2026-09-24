"""Session-wide test setup that must run before ``app.feeds`` is imported.

UX post URIs come from ``src/app/ux_posts_resolved.json``, which is generated at
deploy time and deliberately not committed, so a test run (and CI in particular)
has no manifest. ``app.feeds`` resolves its pinned, survey and logged-out post URIs
at import, so without this every feed would come up with no UX posts and a large
part of the suite would be exercising a state that never reaches production.

Seeding the override environment variable here — before any test module imports
``app.feeds`` — makes the whole suite behave like a deployed service. Tests that
care about resolution itself (``ux_posts_test.py``, the ``feeds_with_manifest``
fixture) clear this and supply their own manifest.
"""

import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent / "src"))

from app import ux_posts  # noqa: E402

os.environ.setdefault(
    ux_posts.URI_ENV_VAR,
    json.dumps(
        {
            name: f"at://{ux_posts.PUBLISHER_DID}/app.bsky.feed.post/test-{name.removesuffix('.md')}"
            for name in ux_posts.MANAGED_POSTS
        }
    ),
)
ux_posts._reset_cache_for_tests()

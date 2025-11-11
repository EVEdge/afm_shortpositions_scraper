import requests
from urllib.parse import urlencode

from config import WP_BASE_URL, WP_USERNAME, WP_APP_PASSWORD, CATEGORY_ID, PUBLISH_STATUS

def _wp_get(path: str, params: dict | None = None, edit_context: bool = False):
    url = f"{WP_BASE_URL}/wp-json/wp/v2{path}"
    if params:
        url += "?" + urlencode(params, doseq=True)
    if edit_context:
        url += ("&" if "?" in url else "?") + "context=edit"
    resp = requests.get(url, auth=(WP_USERNAME, WP_APP_PASSWORD), timeout=25)
    resp.raise_for_status()
    return resp.json()

def _post_exists_by_slug(slug: str) -> bool:
    """
    DB-loze dedupe:
      - check published posts met deze slug
      - check drafts (via context=edit)
    """
    try:
        data_pub = _wp_get("/posts", {"slug": slug, "per_page": 1})
        if isinstance(data_pub, list) and len(data_pub) > 0:
            return True
        data_draft = _wp_get("/posts", {"slug": slug, "status": "draft", "per_page": 1}, edit_context=True)
        if isinstance(data_draft, list) and len(data_draft) > 0:
            return True
    except requests.HTTPError as e:
        print(f"[WARN] slug check HTTP error: {e}")
    except Exception as e:
        print(f"[WARN] slug check failed: {e}")
    return False

def publish_to_wordpress(article: dict):
    if not WP_BASE_URL or not WP_APP_PASSWORD or not WP_USERNAME:
        print(f"[DRY-RUN] Would publish: {article['title']}")
        return

    slug = article.get("slug")
    if slug and _post_exists_by_slug(slug):
        print(f"[SKIP] Post with slug already exists: {slug}")
        return

    url = f"{WP_BASE_URL}/wp-json/wp/v2/posts"
    data = {
        "title": article["title"],
        "content": article["content"],
        "status": PUBLISH_STATUS,  # e.g. "draft"
        "slug": slug,
    }
    if CATEGORY_ID:
        data["categories"] = [CATEGORY_ID]

    resp = requests.post(url, json=data, auth=(WP_USERNAME, WP_APP_PASSWORD), timeout=25)
    if resp.status_code >= 400:
        print(f"[ERROR] WP {resp.status_code}: {resp.text}")
        return
    post_id = resp.json().get("id")
    print(f"[OK] Posted to WP: {article['title']} (id={post_id})")

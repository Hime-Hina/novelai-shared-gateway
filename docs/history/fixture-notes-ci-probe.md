# Synthetic image fixture

`synthetic.webp` is a project-generated 2×2 RGB image with red, green, blue and
white pixels, in that row order. It contains no user image or downloaded content.
It is a lossless VP8L WebP, 34 bytes, SHA256:
`15a40e7d7f1a5ecb5a783b581f173368fd809699b0565be9714284e8b67f4ead`.

Generated with the existing Codex bundled Python, Pillow 12.3.0 and libwebp 1.6.0.
These are fixture-generation tools, not project runtime dependencies. From the
repository root, the following script recreates and independently decodes it;
matching encoder versions are needed for identical compressed bytes:

```python
from pathlib import Path
from hashlib import sha256
from PIL import Image

pixels = [(255, 0, 0), (0, 255, 0), (0, 0, 255), (255, 255, 255)]
image = Image.new("RGB", (2, 2))
image.putdata(pixels)
target = Path("tests/fixtures/synthetic.webp")
image.save(target, format="WEBP", lossless=True, quality=100, method=6, exact=True)
with Image.open(target) as decoded:
    decoded.load()
    assert decoded.size == (2, 2)
    assert [decoded.convert("RGB").getpixel((x, y))
            for y in range(2) for x in range(2)] == pixels
print(sha256(target.read_bytes()).hexdigest())
```

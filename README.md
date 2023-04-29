# Tool for Declarative Web Crawler for XPath and RegEx Lovers

## Installation

```
pip install git+https://github.com/amachang/media-scrapy
```

## Usage

Write site\_config.py like below.

```python
class SiteConfig:
    start_url = "http://example.com/"
    save_dir = "/data/example_com_data"
    login = {
        "url": "http://example.com/login",
        "formdata": {
            "user": "amachang",
            "password": "password",
        }
    }
    structure = [
        {
            "url": r"http://example\.com/",
            "content": "//*[has-class('main-content')]",
        },
        {
            "url": r"http://example\.com/gallery/(\w+)(\?page=\d+)?",
            "file_path": r"\g<1>",
            "content": "//*[has-class('image-list')]",
            "paging": True,
        },
        {
            "url": r"http://example\.com/gallery/images/(\w+)",
            "as_url": r"http://cdn.example.com/images/\g<1>.jpg",
            "file_path": r"\g<1>.jpg",
        }
    ]

```

Start the crawler like below.

```
python -m media_scrapy -c site_config.py
```


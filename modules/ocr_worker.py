import json
import sys
from pathlib import Path

import cv2
from rapidocr_onnxruntime import RapidOCR


def main() -> int:
    if len(sys.argv) != 2:
        print(json.dumps({"error": "expected image path"}))
        return 2

    image_path = Path(sys.argv[1])
    if not image_path.exists():
        print(json.dumps({"error": f"missing image: {image_path}"}))
        return 2

    image = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
    if image is None:
        print(json.dumps({"error": f"unable to load image: {image_path}"}))
        return 2

    engine = RapidOCR()
    result, _ = engine(image)
    text = "\n".join(item[1].strip() for item in (result or []) if item[1].strip()).strip()
    print(json.dumps({"text": text}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

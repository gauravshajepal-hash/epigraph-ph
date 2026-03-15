import os
import base64
import fitz
from litellm import completion
from dotenv import load_dotenv

load_dotenv()

def test_3_1_lite():
    pdf_path = "data/raw_pdfs/hiv_sti/IB_rHIVda.pdf"
    doc = fitz.open(pdf_path)
    page = doc.load_page(0)
    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
    img_data = pix.tobytes("png")
    b64_img = base64.b64encode(img_data).decode('utf-8')
    
    print("Testing gemini/gemini-3.1-flash-lite-preview...")
    
    response = completion(
        model="gemini/gemini-3.1-flash-lite-preview",
        messages=[
            {"role": "system", "content": "OCR this page into Markdown."},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Extract content:"},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_img}"}}
                ]
            }
        ]
    )
    print("Success!")
    print(response.choices[0].message.content[:500])

if __name__ == "__main__":
    test_3_1_lite()

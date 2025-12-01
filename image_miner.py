import sys
import json
import os
import fitz  # PyMuPDF
import requests
import pandas as pd
from fuzzywuzzy import fuzz

# CONFIG
CLOUDFLARE_ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID")
CLOUDFLARE_API_TOKEN = os.environ.get("CF_API_TOKEN")

def upload_to_cloudflare(image_bytes, filename):
    url = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/images/v1"
    headers = {"Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}"}
    files = {"file": (filename, image_bytes)}
    try:
        r = requests.post(url, headers=headers, files=files)
        if r.status_code == 200:
            return r.json()['result']['variants'][0]
    except Exception as e:
        print(f"Upload Error: {e}")
    return None

def find_image_below_text(doc, text_query):
    query_short = str(text_query)[:100]
    best_match_page = -1
    best_rect = None
    highest_ratio = 0
    
    for page_num, page in enumerate(doc):
        text_blocks = page.get_text("blocks")
        for block in text_blocks:
            ratio = fuzz.partial_ratio(query_short, block[4])
            if ratio > 85 and ratio > highest_ratio:
                highest_ratio = ratio
                best_match_page = page_num
                best_rect = fitz.Rect(block[:4])

    if best_match_page == -1: return None

    page = doc[best_match_page]
    images = page.get_images(full=True)
    text_bottom = best_rect.y1
    candidate_xref, min_dist = None, 1000
    
    for img in images:
        xref = img[0]
        rects = page.get_image_rects(xref)
        if not rects: continue
        if rects[0].y0 >= text_bottom:
            dist = rects[0].y0 - text_bottom
            if dist < min_dist:
                min_dist = dist
                candidate_xref = xref

    if candidate_xref:
        base = doc.extract_image(candidate_xref)
        return {"bytes": base["image"], "ext": base["ext"]}
    return None

def main():
    # Args: [1]=ExcelInput, [2]=SourcePDF, [3]=OutputJSON
    try:
        df = pd.read_excel(sys.argv[1])
        questions = df.to_dict(orient='records')
    except:
        print("Error reading Excel input")
        return

    doc = fitz.open(sys.argv[2])
    lookup_map = {}

    for i, q in enumerate(questions):
        has_img = str(q.get('has_image', False)).lower() in ['true', '1']
        if has_img:
            img_data = find_image_below_text(doc, q.get('Question', ''))
            if img_data:
                url = upload_to_cloudflare(img_data['bytes'], f"q_{i}.{img_data['ext']}")
                if url: lookup_map[q.get('Question', '')] = url

    with open(sys.argv[3], 'w', encoding='utf-8') as f:
        json.dump(lookup_map, f, indent=4)

if __name__ == "__main__":
    main()

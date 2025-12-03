import sys
import json
import os
import fitz  # PyMuPDF
import requests
import pandas as pd
from fuzzywuzzy import fuzz

# --- 1. UPLOAD FUNCTION (Custom API) ---
def upload_image_api(image_bytes, filename):
    # YOUR CUSTOM API URL
    url = "https://backend.succeedquiz.com/api/v1/upload"
    
    # READ TOKEN FROM SECRET (Security Best Practice)
    token = os.environ.get("SUCCEED_API_TOKEN")
    
    if not token:
        print("Error: SUCCEED_API_TOKEN not found in environment variables.")
        return None
    
    headers = {
        'Authorization': f'Bearer {token}'
    }

    # API CONFIGURATION
    files = [
        ('file', (filename, image_bytes, 'image/png')) 
    ]

    try:
        response = requests.post(url, headers=headers, files=files)
        
        if response.status_code == 200 or response.status_code == 201:
            # TRY TO EXTRACT URL
            # Adjust this based on the exact JSON structure of your API
            data = response.json()
            
            # Pattern 1: { "url": "..." }
            if 'url' in data: return data['url']
            
            # Pattern 2: { "data": { "url": "..." } }
            if 'data' in data and isinstance(data['data'], dict):
                if 'url' in data['data']: return data['data']['url']
                if 'link' in data['data']: return data['data']['link']
            
            # Pattern 3: { "secure_url": "..." }
            if 'secure_url' in data: return data['secure_url']

            print(f"  -> Uploaded, but couldn't find URL key in: {data}")
            return None
            
        else:
            print(f"  -> API Error ({response.status_code}): {response.text}")
            return None
            
    except Exception as e:
        print(f"  -> Upload Failed: {e}")
        return None

# --- 2. VISUAL SEARCH FUNCTION ---
def find_image_below_text(doc, text_query):
    best_match_page = -1
    best_rect = None
    highest_ratio = 0
    query_short = str(text_query)[:100]
    
    # A. Find Text
    for page_num, page in enumerate(doc):
        text_blocks = page.get_text("blocks")
        for block in text_blocks:
            ratio = fuzz.partial_ratio(query_short, block[4])
            if ratio > 85 and ratio > highest_ratio:
                highest_ratio = ratio
                best_match_page = page_num
                best_rect = fitz.Rect(block[:4])

    if best_match_page == -1: return None

    # B. Find Image Below Text
    page = doc[best_match_page]
    images = page.get_images(full=True)
    text_bottom = best_rect.y1
    candidate_xref, min_dist = None, 1000
    
    for img in images:
        xref = img[0]
        rects = page.get_image_rects(xref)
        if not rects: continue
        
        # Check logic: Image must be below text (y0 >= text_bottom)
        if rects[0].y0 >= text_bottom:
            dist = rects[0].y0 - text_bottom
            if dist < min_dist:
                min_dist = dist
                candidate_xref = xref

    # C. Extract
    if candidate_xref:
        base = doc.extract_image(candidate_xref)
        return {"bytes": base["image"], "ext": base["ext"]}
    return None

# --- 3. MAIN EXECUTION ---
def main():
    input_excel = sys.argv[1]
    pdf_path = sys.argv[2]
    output_json = sys.argv[3]

    print(f"Reading Excel: {input_excel}")
    try:
        df = pd.read_excel(input_excel)
        questions = df.to_dict(orient='records')
    except Exception as e:
        print(f"Excel Read Error: {e}")
        return

    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        print(f"PDF Read Error: {e}")
        return

    lookup_map = {}

    print("Starting Image Mining...")

    for i, q in enumerate(questions):
        # Check has_image flag
        has_img = str(q.get('has_image', False)).lower() in ['true', '1']
        
        if has_img:
            q_text = str(q.get('Question', ''))
            print(f"Searching Q{i}...")
            img_data = find_image_below_text(doc, q_text)
            
            if img_data:
                filename = f"q_{i}.{img_data['ext']}"
                
                # --- THIS WAS THE BROKEN LINE ---
                # Now correctly calls 'upload_image_api' instead of 'upload_to_cloudflare'
                url = upload_image_api(img_data['bytes'], filename)
                
                if url:
                    print(f"  -> Uploaded: {url}")
                    lookup_map[q_text] = url
                else:
                    print("  -> Upload failed (Check API response parsing)")
            else:
                print("  -> No image found visually below text")

    # Save Lookup Map
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(lookup_map, f, indent=4)
    
    print(f"Mining Complete. Saved {len(lookup_map)} images.")

if __name__ == "__main__":
    main()
